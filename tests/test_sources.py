from pathlib import Path

import pytest

from ingest.config import load_datasets
from ingest.sources import Release, SourceError, download, release_from_metastore, resolve_releases

FIXTURES = Path(__file__).parent / "fixtures"


def test_config_is_valid():
    ds = load_datasets()
    assert set(ds) == {"hospital_general", "unplanned_visits", "complications_deaths", "inpatient_drg"}
    for d in ds.values():
        assert d.required_columns and d.min_rows > 0 and "." in d.table
    assert ds["hospital_general"].kind == "provider_data" and ds["hospital_general"].dataset_id
    assert ds["inpatient_drg"].releases  # pinned years
    assert all(u.startswith("https://data.cms.gov/") for u in ds["inpatient_drg"].releases.values())


def test_release_from_metastore_picks_csv_and_date():
    meta = {
        "modified": "2026-09-10",
        "distribution": [
            {"mediaType": "application/json", "downloadURL": "https://x/api.json"},
            {"mediaType": "text/csv", "downloadURL": "https://x/Unplanned.csv"},
        ],
    }
    rel = release_from_metastore("unplanned_visits", meta)
    assert rel == Release("unplanned_visits", "2026-09-10", "https://x/Unplanned.csv")


def test_release_from_metastore_without_csv_fails():
    with pytest.raises(SourceError):
        release_from_metastore("x", {"modified": "2026-01-01", "distribution": [{"mediaType": "text/html"}]})


def test_resolve_local_fixture_releases():
    ds = load_datasets()["hospital_general"]
    rels = resolve_releases(ds, source_dir=FIXTURES / "phase1")
    assert [r.release_id for r in rels] == ["2026-07-22"] and rels[0].is_local


def test_resolve_pinned_releases_sorted_by_year():
    rels = resolve_releases(load_datasets()["inpatient_drg"])
    years = [r.release_id for r in rels]
    assert years == sorted(years) and "2024" in years


class FakeResponse:
    def __init__(self, chunks):
        self.chunks = chunks

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        yield from self.chunks


class FakeSession:
    def __init__(self, chunks):
        self.chunks, self.calls = chunks, 0

    def get(self, url, **kw):
        self.calls += 1
        return FakeResponse(self.chunks)


def test_download_writes_atomically_and_reuses(tmp_path):
    rel = Release("ds", "2024", "https://data.cms.gov/files/data.csv")
    session = FakeSession([b"a,b\n", b"1,2\n"])
    path, sha = download(rel, tmp_path, session=session)
    assert path == tmp_path / "ds" / "2024" / "data.csv"
    assert path.read_bytes() == b"a,b\n1,2\n"
    assert not list(tmp_path.rglob("*.part"))
    path2, sha2 = download(rel, tmp_path, session=session)
    assert (path2, sha2) == (path, sha) and session.calls == 1  # second call reused the file


def test_download_rejects_empty_file(tmp_path):
    rel = Release("ds", "2024", "https://data.cms.gov/files/empty.csv")
    with pytest.raises(SourceError, match="empty"):
        download(rel, tmp_path, session=FakeSession([]))
    assert not list(tmp_path.rglob("*.csv"))


class BlockedSession:
    """Simulates data.cms.gov bot protection returning 403."""

    def get(self, url, **kw):
        import io

        import requests

        resp = requests.Response()
        resp.status_code, resp.url, resp.raw = 403, url, io.BytesIO(b"Forbidden")
        return resp


def test_catalog_403_falls_back_to_pinned_release():
    ds = load_datasets()["hospital_general"]
    rels = resolve_releases(ds, session=BlockedSession())
    assert len(rels) == 1
    assert rels[0].release_id == max(ds.fallback_releases)
    assert rels[0].location.endswith(".csv")


def test_catalog_403_without_fallback_raises():
    import requests

    ds = load_datasets()["hospital_general"]
    ds.fallback_releases = {}
    with pytest.raises(requests.HTTPError):
        resolve_releases(ds, session=BlockedSession())


def test_browser_headers_sent():
    from ingest.sources import http_session

    s = http_session()
    assert s.headers["User-Agent"].startswith("Mozilla/5.0")
    assert "text/csv" in s.headers["Accept"]


def test_blocked_download_uses_manual_drop_folder(tmp_path, monkeypatch):
    from ingest import pipeline

    monkeypatch.setenv("LANDING_DIR", str(tmp_path))
    ds = load_datasets()["hospital_general"]
    rel = Release("hospital_general", "2026-07-22", "https://data.cms.gov/provider-data/x.csv")

    with pytest.raises(SourceError, match="save it as"):  # nothing in the drop folder yet
        pipeline.fetch(ds, rel, BlockedSession())

    drop = tmp_path / "manual" / "hospital_general"
    drop.mkdir(parents=True)
    (drop / "2026-10-01.csv").write_text("Facility ID\n1\n")
    used, path, sha = pipeline.fetch(ds, rel, BlockedSession())
    assert used.release_id == "2026-10-01" and path == drop / "2026-10-01.csv" and len(sha) == 64

