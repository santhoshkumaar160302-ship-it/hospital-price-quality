"""Loader tests against a real Postgres. Skipped unless WAREHOUSE_DSN or
WAREHOUSE_HOST is set (CI provides a Postgres service container)."""

import os
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from ingest.config import Dataset, warehouse_conninfo  # noqa: E402
from ingest.contract import ContractError  # noqa: E402
from ingest.load import load_release  # noqa: E402

pytestmark = pytest.mark.skipif(
    not (os.environ.get("WAREHOUSE_DSN") or os.environ.get("WAREHOUSE_HOST")),
    reason="needs a Postgres warehouse",
)


@pytest.fixture
def conn():
    with psycopg.connect(warehouse_conninfo(), autocommit=True) as c:
        yield c


@pytest.fixture
def ds(conn):
    schema = f"pytest_{uuid.uuid4().hex[:8]}"
    yield Dataset(name="t", kind="pinned_files", table=f"{schema}.measures",
                  required_columns=["facility_id", "score"], min_rows=2,
                  column_aliases={"provider_id": "facility_id"})
    conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def write_csv(tmp_path, name, text):
    p = tmp_path / name
    p.write_text(text)
    return p


def count(conn, ds, release=None):
    sql = f"SELECT count(*) FROM {ds.table}" + (" WHERE _release = %s" if release else "")
    return conn.execute(sql, (release,) if release else ()).fetchone()[0]


def test_load_and_reload_same_release_is_idempotent(conn, ds, tmp_path):
    f = write_csv(tmp_path, "a.csv", "Facility ID,Score\n010001,1.5\n010002,Not Available\n")
    assert load_release(conn, ds, "2024", f) == (2, [])
    assert load_release(conn, ds, "2024", f)[0] == 2
    assert count(conn, ds) == 2  # replaced, not duplicated


def test_empty_cells_become_null(conn, ds, tmp_path):
    f = write_csv(tmp_path, "a.csv", "Facility ID,Score\n010001,\n010002, \n")
    load_release(conn, ds, "2024", f)
    assert conn.execute(f"SELECT count(*) FROM {ds.table} WHERE score IS NULL").fetchone()[0] == 2


def test_failed_load_keeps_previous_data(conn, ds, tmp_path):
    good = write_csv(tmp_path, "good.csv", "Facility ID,Score\n1,1\n2,2\n3,3\n")
    load_release(conn, ds, "2024", good)

    short = write_csv(tmp_path, "short.csv", "Facility ID,Score\n1,1\n")  # below min_rows
    with pytest.raises(ContractError, match="partial file"):
        load_release(conn, ds, "2024", short)

    ragged = write_csv(tmp_path, "ragged.csv", "Facility ID,Score\n1,1\n2,2,EXTRA\n")
    with pytest.raises(ContractError, match="line 3"):
        load_release(conn, ds, "2024", ragged)

    assert count(conn, ds, "2024") == 3  # rolled back both times


def test_missing_required_column_fails_before_touching_table(conn, ds, tmp_path):
    f = write_csv(tmp_path, "a.csv", "Facility ID,Rating\n1,4\n2,5\n")
    with pytest.raises(ContractError, match="missing required columns"):
        load_release(conn, ds, "2024", f)


def test_alias_and_new_columns(conn, ds, tmp_path):
    v1 = write_csv(tmp_path, "v1.csv", "Provider ID,Score\n1,1\n2,2\n")
    load_release(conn, ds, "2023", v1)
    v2 = write_csv(tmp_path, "v2.csv", "Facility ID,Score,New Field\n1,1,x\n2,2,y\n")
    rows, added = load_release(conn, ds, "2024", v2)
    assert added == ["new_field"]
    assert count(conn, ds) == 4  # both releases kept side by side


def test_windows_1252_file_is_decoded(conn, ds, tmp_path):
    p = tmp_path / "cp1252.csv"
    p.write_bytes("Facility ID,Score\n1,1\nCaf\xe9 Hospital,2\n".encode("cp1252"))
    assert load_release(conn, ds, "2024", p)[0] == 2
