"""Loads config/sources.yml and connection settings."""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass
class Dataset:
    name: str
    kind: str  # "provider_data" | "pinned_files"
    table: str  # schema-qualified, e.g. raw.hospital_general_info
    required_columns: list[str]
    min_rows: int
    dataset_id: str | None = None
    releases: dict[str, str] = field(default_factory=dict)
    column_aliases: dict[str, str] = field(default_factory=dict)
    # provider_data only: known release -> CSV, used if the catalog API is unreachable
    fallback_releases: dict[str, str] = field(default_factory=dict)

    @property
    def schema(self) -> str:
        return self.table.split(".", 1)[0]

    @property
    def table_name(self) -> str:
        return self.table.split(".", 1)[1]


def load_datasets(path: str | Path | None = None) -> dict[str, Dataset]:
    path = Path(path or os.environ.get("SOURCES_CONFIG", PROJECT_ROOT / "config" / "sources.yml"))
    raw = yaml.safe_load(path.read_text())["datasets"]
    datasets = {}
    for name, cfg in raw.items():
        if cfg["kind"] not in ("provider_data", "pinned_files"):
            raise ValueError(f"{name}: unknown kind {cfg['kind']!r}")
        if "." not in cfg["table"]:
            raise ValueError(f"{name}: table must be schema-qualified")
        datasets[name] = Dataset(
            name=name,
            kind=cfg["kind"],
            table=cfg["table"],
            required_columns=list(cfg["required_columns"]),
            min_rows=int(cfg.get("min_rows", 1)),
            dataset_id=cfg.get("dataset_id"),
            releases={str(k): v for k, v in (cfg.get("releases") or {}).items()},
            column_aliases=dict(cfg.get("column_aliases") or {}),
            fallback_releases={str(k): v for k, v in (cfg.get("fallback_releases") or {}).items()},
        )
    return datasets


def warehouse_conninfo() -> str:
    if os.environ.get("WAREHOUSE_DSN"):
        return os.environ["WAREHOUSE_DSN"]
    parts = {
        "host": os.environ.get("WAREHOUSE_HOST", "localhost"),
        "port": os.environ.get("WAREHOUSE_PORT", "5432"),
        "dbname": os.environ.get("WAREHOUSE_DB", "warehouse"),
        "user": os.environ.get("WAREHOUSE_USER", "warehouse"),
        "password": os.environ.get("WAREHOUSE_PASSWORD", "warehouse"),
    }
    return " ".join(f"{k}={v}" for k, v in parts.items() if v != "")


def landing_dir() -> Path:
    return Path(os.environ.get("LANDING_DIR", PROJECT_ROOT / "landing"))
