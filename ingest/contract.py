"""Schema contract: header normalization and checks that run before commit."""

import re


class ContractError(Exception):
    """The source file doesn't match what downstream models depend on."""


def normalize_column(name: str) -> str:
    """'City/Town' -> 'city_town', 'Hospital overall rating' -> 'hospital_overall_rating'."""
    s = name.replace("﻿", "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s or "unnamed"


def normalize_header(columns: list[str], aliases: dict[str, str] | None = None) -> list[str]:
    aliases = aliases or {}
    out = [aliases.get(normalize_column(c), normalize_column(c)) for c in columns]
    dupes = sorted({c for c in out if out.count(c) > 1})
    if dupes:
        raise ContractError(f"duplicate columns after normalization: {dupes}")
    return out


def check_required_columns(header: list[str], required: list[str]) -> None:
    missing = [c for c in required if c not in header]
    if missing:
        raise ContractError(
            f"missing required columns {missing}; file has {header}. "
            "The source schema changed: update config/sources.yml (aliases) and the staging model."
        )


def check_row_count(rows: int, min_rows: int) -> None:
    if rows < min_rows:
        raise ContractError(
            f"only {rows} rows loaded, expected at least {min_rows}; "
            "refusing to replace existing data with a partial file"
        )
