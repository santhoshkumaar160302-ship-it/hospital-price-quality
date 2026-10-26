"""Orchestrates ingestion for one dataset: resolve -> download -> validate + load -> audit.

Called by the Airflow DAG (one task per dataset) and by the CLI.
"""

import logging
from pathlib import Path

import psycopg
import requests

from ingest.config import Dataset, landing_dir, load_datasets, warehouse_conninfo
from ingest.load import already_loaded, audit_finish, audit_start, ensure_audit_table, load_release
from ingest.sources import Release, SourceError, download, http_session, resolve_releases, sha256_of

log = logging.getLogger("ingest")


def manual_dir(dataset: str) -> Path:
    """Drop folder for files downloaded by hand: landing/manual/<dataset>/<release>.csv"""
    return landing_dir() / "manual" / dataset


def manual_release(ds: Dataset, wanted: Release) -> Release | None:
    """A hand-downloaded file to use when the automated download is blocked.

    pinned_files: must be named <data year>.csv (e.g. 2024.csv)
    provider_data: the newest <YYYY-MM-DD>.csv in the folder
    """
    folder = manual_dir(ds.name)
    if ds.kind == "pinned_files":
        p = folder / f"{wanted.release_id}.csv"
        return Release(ds.name, wanted.release_id, str(p), is_local=True) if p.exists() else None
    files = sorted(folder.glob("*.csv"))
    return Release(ds.name, files[-1].stem, str(files[-1]), is_local=True) if files else None


def fetch(ds: Dataset, rel: Release, session) -> tuple[Release, Path, str]:
    """Download a release; if the source blocks us, fall back to the manual drop folder."""
    try:
        path, sha = download(rel, landing_dir(), session=session)
        return rel, path, sha
    except (requests.RequestException, SourceError) as e:
        if rel.is_local:
            raise
        manual = manual_release(ds, rel)
        if manual is None:
            hint = f"{rel.release_id}.csv" if ds.kind == "pinned_files" else "<YYYY-MM-DD>.csv (the release date)"
            raise SourceError(
                f"{ds.name}: download failed ({e}). Download the CSV in a browser from {rel.location} "
                f"and save it as {manual_dir(ds.name)}/{hint}"
            ) from e
        log.warning("%s: download blocked (%s); loading manual file %s", ds.name, e, manual.location)
        path = Path(manual.location)
        return manual, path, sha256_of(path)


def run_dataset(name: str, source_dir: str | Path | None = None, force: bool = False,
                min_rows: int | None = None, conninfo: str | None = None) -> list[dict]:
    """Ingest every available release of a dataset. Returns one result per release.

    status: "loaded"  - new or changed file, loaded
            "skipped" - this exact file (same release + sha256) was already loaded
    Raises on failure (after recording it in raw._load_audit) so the caller can retry.
    """
    ds = load_datasets()[name]
    session = None if source_dir else http_session()
    releases = resolve_releases(ds, session=session, source_dir=source_dir)
    if not releases:
        raise RuntimeError(f"{name}: no releases found")

    results = []
    with psycopg.connect(conninfo or warehouse_conninfo(), autocommit=True) as conn:
        ensure_audit_table(conn)
        for rel in releases:
            rel, path, sha = fetch(ds, rel, session)
            if not force and already_loaded(conn, name, rel.release_id, sha):
                log.info("%s %s: already loaded (sha %s), skipping", name, rel.release_id, sha[:12])
                results.append({"dataset": name, "release": rel.release_id, "status": "skipped", "rows": 0})
                continue

            load_id = audit_start(conn, name, rel.release_id, rel.location, sha)
            try:
                rows, added = load_release(conn, ds, rel.release_id, path, min_rows=min_rows)
            except Exception as e:
                audit_finish(conn, load_id, "failed", error=f"{type(e).__name__}: {e}")
                log.error("%s %s: load failed: %s", name, rel.release_id, e)
                raise
            audit_finish(conn, load_id, "success", rows=rows, columns_added=added)
            if added:
                log.warning("%s %s: new source columns added to raw table: %s", name, rel.release_id, added)
            log.info("%s %s: loaded %s rows", name, rel.release_id, rows)
            results.append({"dataset": name, "release": rel.release_id, "status": "loaded", "rows": rows})
    return results


def run_all(source_dir: str | Path | None = None, force: bool = False, min_rows: int | None = None) -> list[dict]:
    results = []
    for name in load_datasets():
        results.extend(run_dataset(name, source_dir=source_dir, force=force, min_rows=min_rows))
    return results
