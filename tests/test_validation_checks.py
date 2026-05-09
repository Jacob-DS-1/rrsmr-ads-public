import pandas as pd

from rrsmr_ads.validation import (
    check_hourly_continuity,
    check_no_duplicate_keys,
    check_snake_case_columns,
    check_timestamp_utc,
    missing_values_summary,
    require_columns,
)


def test_require_columns_passes_when_columns_present():
    df = pd.DataFrame({"timestamp_utc": [], "nd_mw": []})

    result = require_columns(df, ["timestamp_utc", "nd_mw"])

    assert result.passed is True


def test_require_columns_fails_when_columns_missing():
    df = pd.DataFrame({"timestamp_utc": []})

    result = require_columns(df, ["timestamp_utc", "nd_mw"])

    assert result.passed is False
    assert result.details["missing_columns"] == ["nd_mw"]


def test_snake_case_check_flags_bad_columns():
    df = pd.DataFrame({"timestamp_utc": [], "Bad Column": []})

    result = check_snake_case_columns(df)

    assert result.passed is False
    assert "Bad Column" in result.details["bad_columns"]


def test_no_duplicate_keys_passes_for_unique_keys():
    df = pd.DataFrame(
        {
            "timestamp_utc": pd.date_range(
                "2030-01-01 00:00", periods=3, freq="h", tz="UTC"
            ),
            "demand_mw": [1.0, 2.0, 3.0],
        }
    )

    result = check_no_duplicate_keys(df, ["timestamp_utc"])

    assert result.passed is True


def test_no_duplicate_keys_fails_for_duplicate_keys():
    df = pd.DataFrame(
        {
            "timestamp_utc": [
                pd.Timestamp("2030-01-01 00:00", tz="UTC"),
                pd.Timestamp("2030-01-01 00:00", tz="UTC"),
            ],
            "demand_mw": [1.0, 2.0],
        }
    )

    result = check_no_duplicate_keys(df, ["timestamp_utc"])

    assert result.passed is False
    assert result.details["duplicate_count"] == 1


def test_timestamp_utc_passes_for_utc_datetime():
    df = pd.DataFrame(
        {
            "timestamp_utc": pd.date_range(
                "2030-01-01 00:00", periods=2, freq="h", tz="UTC"
            )
        }
    )

    result = check_timestamp_utc(df)

    assert result.passed is True


def test_timestamp_utc_fails_for_naive_datetime():
    df = pd.DataFrame(
        {
            "timestamp_utc": pd.date_range(
                "2030-01-01 00:00", periods=2, freq="h"
            )
        }
    )

    result = check_timestamp_utc(df)

    assert result.passed is False


def test_hourly_continuity_passes_for_complete_series():
    df = pd.DataFrame(
        {
            "timestamp_utc": pd.date_range(
                "2030-01-01 00:00", periods=3, freq="h", tz="UTC"
            )
        }
    )

    result = check_hourly_continuity(df)

    assert result.passed is True


def test_hourly_continuity_fails_for_missing_hour():
    df = pd.DataFrame(
        {
            "timestamp_utc": [
                pd.Timestamp("2030-01-01 00:00", tz="UTC"),
                pd.Timestamp("2030-01-01 02:00", tz="UTC"),
            ]
        }
    )

    result = check_hourly_continuity(df)

    assert result.passed is False
    assert result.details["missing_hours_count"] == 1


def test_missing_values_summary_reports_missing_values():
    df = pd.DataFrame({"timestamp_utc": [pd.NaT], "nd_mw": [None]})

    result = missing_values_summary(df)

    assert result.passed is False
    assert result.details["missing_counts"]["nd_mw"] == 1
