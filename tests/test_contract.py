import pytest

from ingest.contract import (
    ContractError,
    check_required_columns,
    check_row_count,
    normalize_column,
    normalize_header,
)


@pytest.mark.parametrize("raw,expected", [
    ("Facility ID", "facility_id"),
    ("City/Town", "city_town"),
    ("County/Parish", "county_parish"),
    ("Hospital overall rating", "hospital_overall_rating"),
    ("﻿Facility ID", "facility_id"),             # UTF-8 BOM on the first header
    ("  Rndrng_Prvdr_CCN ", "rndrng_prvdr_ccn"),
    ("Count of MORT Measures Better", "count_of_mort_measures_better"),
    ("###", "unnamed"),
])
def test_normalize_column(raw, expected):
    assert normalize_column(raw) == expected


def test_aliases_map_old_column_names():
    header = normalize_header(["Rndrng_CCN", "DRG_Cd"], {"rndrng_ccn": "rndrng_prvdr_ccn"})
    assert header == ["rndrng_prvdr_ccn", "drg_cd"]


def test_duplicate_columns_after_normalization_rejected():
    with pytest.raises(ContractError, match="duplicate"):
        normalize_header(["City/Town", "City Town"])


def test_missing_required_columns_reported():
    with pytest.raises(ContractError, match=r"\['score'\]"):
        check_required_columns(["facility_id", "measure_id"], ["facility_id", "measure_id", "score"])
    check_required_columns(["a", "b", "extra"], ["a", "b"])  # extra columns are fine


def test_row_count_threshold():
    check_row_count(100, 100)
    with pytest.raises(ContractError, match="partial file"):
        check_row_count(99, 100)
