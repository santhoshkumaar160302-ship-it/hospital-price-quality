"""Load a CSV release into a raw Postgres table, safely.

Raw tables keep every column as TEXT (typing happens in dbt staging), plus:
  _release      which source release the row came from
  _source_file  file it was read from
  _loaded_at    load time (used by dbt source freshness)

A load is one transaction: delete that release's rows, COPY the file, check
the row count, commit. If anything fails (bad header, ragged row, too few
rows, encoding error) the transaction rolls back and the previous data stays.
Re-running the same release replaces it instead of duplicating it.

New columns in a later release are added automatically (ALTER TABLE ADD
COLUMN), so additive schema changes don't break the load.
"""

import csv
import sys
from pathlib import Path

import psycopg
from psycopg import sql

from ingest.config import Dataset
from ingest.contract import ContractError, check_required_columns, check_row_count, normalize_header

csv.field_size_limit(sys.maxsize)

META_COLUMNS = ("_release", "_source_file", "_loaded_at")
ENCODINGS = ("utf-8-sig", "cp1252")  # CMS files are UTF-8; a few older ones are Windows-1252

AUDIT_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw._load_audit (
    load_id      BIGSERIAL PRIMARY KEY,
    dataset      TEXT NOT NULL,
    release_id   TEXT NOT NULL,
    source       TEXT NOT NULL,
    file_sha256  TEXT,
    status       TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    rows_loaded  BIGINT,
    columns_added TEXT,
    error        TEXT,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ
);
"""


def ensure_audit_table(conn: psycopg.Connection) -> None:
    with conn.transaction():
        conn.execute(AUDIT_DDL)


def already_loaded(conn: psycopg.Connection, dataset: str, release_id: str, sha: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM raw._load_audit WHERE dataset = %s AND release_id = %s "
        "AND file_sha256 = %s AND status = 'success' LIMIT 1",
        (dataset, release_id, sha),
    ).fetchone()
    return row is not None


def audit_start(conn: psycopg.Connection, dataset: str, release_id: str, source: str, sha: str) -> int:
    with conn.transaction():
        return conn.execute(
            "INSERT INTO raw._load_audit (dataset, release_id, source, file_sha256, status) "
            "VALUES (%s, %s, %s, %s, 'running') RETURNING load_id",
            (dataset, release_id, source, sha),
        ).fetchone()[0]


def audit_finish(conn: psycopg.Connection, load_id: int, status: str, rows: int | None = None,
                 columns_added: list[str] | None = None, error: str | None = None) -> None:
    with conn.transaction():
        conn.execute(
            "UPDATE raw._load_audit SET status = %s, rows_loaded = %s, columns_added = %s, "
            "error = %s, finished_at = now() WHERE load_id = %s",
            (status, rows, ",".join(columns_added) if columns_added else None,
             error[:2000] if error else None, load_id),
        )


def ensure_table(conn: psycopg.Connection, ds: Dataset, columns: list[str]) -> list[str]:
    """Create the raw table if needed and add any new columns. Returns added columns."""
    conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(ds.schema)))
    conn.execute(
        sql.SQL(
            "CREATE TABLE IF NOT EXISTS {} ({}, _release TEXT NOT NULL, _source_file TEXT, "
            "_loaded_at TIMESTAMPTZ NOT NULL DEFAULT now())"
        ).format(
            sql.Identifier(ds.schema, ds.table_name),
            sql.SQL(", ").join(sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in columns),
        )
    )
    existing = {
        r[0] for r in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
            (ds.schema, ds.table_name),
        )
    }
    added = [c for c in columns if c not in existing]
    for c in added:
        conn.execute(sql.SQL("ALTER TABLE {} ADD COLUMN {} TEXT").format(
            sql.Identifier(ds.schema, ds.table_name), sql.Identifier(c)))
    return added


def _copy_file(conn: psycopg.Connection, ds: Dataset, release_id: str, path: Path,
               encoding: str, min_rows: int) -> tuple[int, list[str]]:
    with path.open(newline="", encoding=encoding) as f:
        reader = csv.reader(f)
        try:
            header = normalize_header(next(reader), ds.column_aliases)
        except StopIteration:
            raise ContractError(f"{path} is empty") from None
        check_required_columns(header, ds.required_columns)
        clash = [c for c in header if c in META_COLUMNS]
        if clash:
            raise ContractError(f"source columns clash with metadata columns: {clash}")

        rows = 0
        with conn.transaction():
            added = ensure_table(conn, ds, header)
            target = sql.Identifier(ds.schema, ds.table_name)
            conn.execute(sql.SQL("DELETE FROM {} WHERE _release = %s").format(target), (release_id,))
            copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(
                target, sql.SQL(", ").join(sql.Identifier(c) for c in [*header, "_release", "_source_file"])
            )
            with conn.cursor().copy(copy_sql) as copy:
                for row in reader:
                    if not any(cell.strip() for cell in row):
                        continue  # trailing blank lines
                    if len(row) != len(header):
                        raise ContractError(
                            f"line {reader.line_num}: expected {len(header)} fields, got {len(row)}")
                    copy.write_row([cell if cell.strip() != "" else None for cell in row]
                                   + [release_id, path.name])
                    rows += 1
            check_row_count(rows, min_rows)  # raising here rolls the whole load back
        return rows, added


def load_release(conn: psycopg.Connection, ds: Dataset, release_id: str, path: Path,
                 min_rows: int | None = None) -> tuple[int, list[str]]:
    min_rows = ds.min_rows if min_rows is None else min_rows
    last_error: Exception | None = None
    for encoding in ENCODINGS:
        try:
            return _copy_file(conn, ds, release_id, path, encoding, min_rows)
        except UnicodeDecodeError as e:  # transaction already rolled back; retry
            last_error = e
    raise ContractError(f"could not decode {path} with {ENCODINGS}: {last_error}")
