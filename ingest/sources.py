"""Resolve which releases (files) exist for a dataset, and download them."""

import hashlib
import logging
import os
from dataclasses import dataclass
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingest.config import Dataset

METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/{dataset_id}"
# data.cms.gov sits behind bot protection that rejects many non-browser
# clients with 403, so send browser-style headers. Override with CMS_USER_AGENT.
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0 Safari/537.36"
)

log = logging.getLogger("ingest")


class SourceError(Exception):
    pass


@dataclass(frozen=True)
class Release:
    dataset: str
    release_id: str  # provider_data: catalog "modified" date; pinned_files: data year
    location: str  # URL, or a local path in fixture mode
    is_local: bool = False


def http_session() -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=2,  # 2s, 4s, 8s, ...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    s = requests.Session()
    s.headers.update({
        "User-Agent": os.environ.get("CMS_USER_AGENT", DEFAULT_USER_AGENT),
        "Accept": "application/json, text/csv, */*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def resolve_releases(ds: Dataset, session: requests.Session | None = None,
                     source_dir: str | Path | None = None) -> list[Release]:
    if source_dir is not None:
        files = sorted((Path(source_dir) / ds.name).glob("*.csv"))
        return [Release(ds.name, p.stem, str(p), is_local=True) for p in files]

    if ds.kind == "pinned_files":
        return [Release(ds.name, year, url) for year, url in sorted(ds.releases.items())]

    session = session or http_session()
    try:
        resp = session.get(METASTORE_URL.format(dataset_id=ds.dataset_id), timeout=30)
        resp.raise_for_status()
        return [release_from_metastore(ds.name, resp.json())]
    except (requests.RequestException, ValueError, SourceError) as e:
        if not ds.fallback_releases:
            raise
        release_id, url = max(ds.fallback_releases.items())
        log.warning("%s: catalog lookup failed (%s); using pinned fallback release %s",
                    ds.name, e, release_id)
        return [Release(ds.name, release_id, url)]


def release_from_metastore(dataset: str, meta: dict) -> Release:
    """Pick the CSV distribution and the release date from catalog metadata."""
    csvs = [
        d for d in meta.get("distribution", [])
        if "csv" in (d.get("mediaType") or "").lower()
        or (d.get("downloadURL") or "").lower().endswith(".csv")
    ]
    if not csvs or not meta.get("modified"):
        raise SourceError(f"{dataset}: no CSV distribution or modified date in catalog metadata")
    return Release(dataset, str(meta["modified"])[:10], csvs[0]["downloadURL"])


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def download(release: Release, landing: Path, session: requests.Session | None = None) -> tuple[Path, str]:
    """Download to landing/<dataset>/<release>/<file>. Writes to a .part file and
    renames on success, so a crash never leaves a truncated file that looks complete.
    Re-uses a previously completed download."""
    if release.is_local:
        p = Path(release.location)
        return p, sha256_of(p)

    filename = release.location.rstrip("/").rsplit("/", 1)[-1] or "data.csv"
    dest_dir = landing / release.dataset / release.release_id
    dest = dest_dir / filename
    if dest.exists() and dest.stat().st_size > 0:
        return dest, sha256_of(dest)

    dest_dir.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(dest.suffix + ".part")
    session = session or http_session()
    with session.get(release.location, stream=True, timeout=(30, 300)) as resp:
        resp.raise_for_status()
        with part.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    if part.stat().st_size == 0:
        part.unlink()
        raise SourceError(f"{release.location} returned an empty file")
    part.rename(dest)
    return dest, sha256_of(dest)
