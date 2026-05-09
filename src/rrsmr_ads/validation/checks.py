"""Reusable validation checks for RR SMR ADS datasets."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Any

import pandas as pd


@dataclass
class CheckResult:
    """Single validation check result."""

    name: str
    passed: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)


def require_columns(df: pd.DataFrame, required_columns: Iterable[str]) -> CheckResult:
    """Check that required columns are present."""
    required = list(required_columns)
    missing = [column for column in required if column not in df.columns]

    return CheckResult(
        name="require_columns",
        passed=len(missing) == 0,
        message="All required columns present." if not missing else "Missing required columns.",
        details={"required_columns": required, "missing_columns": missing},
    )


def check_snake_case_columns(df: pd.DataFrame) -> CheckResult:
    """Check that column names use lower snake_case."""
    bad_columns = [
        column
        for column in df.columns
        if not column.islower()
        or " " in column
        or "-" in column
        or column.startswith("_")
        or column.endswith("_")
    ]

    return CheckResult(
        name="check_snake_case_columns",
        passed=len(bad_columns) == 0,
        message="All columns appear to use snake_case." if not bad_columns else "Some columns are not snake_case.",
        details={"bad_columns": bad_columns},
    )


def check_no_duplicate_keys(df: pd.DataFrame, key_columns: Iterable[str]) -> CheckResult:
    """Check that key columns do not contain duplicate rows."""
    keys = list(key_columns)

    missing_key_result = require_columns(df, keys)
    if not missing_key_result.passed:
        return CheckResult(
            name="check_no_duplicate_keys",
            passed=False,
            message="Cannot check duplicates because key columns are missing.",
            details=missing_key_result.details,
        )

    duplicate_count = int(df.duplicated(subset=keys).sum())

    return CheckResult(
        name="check_no_duplicate_keys",
        passed=duplicate_count == 0,
        message="No duplicate keys found." if duplicate_count == 0 else "Duplicate keys found.",
        details={"key_columns": keys, "duplicate_count": duplicate_count},
    )


def check_timestamp_utc(df: pd.DataFrame, timestamp_column: str = "timestamp_utc") -> CheckResult:
    """Check that timestamp column exists, is datetime-like, and is UTC-aware."""
    if timestamp_column not in df.columns:
        return CheckResult(
            name="check_timestamp_utc",
            passed=False,
            message=f"Missing timestamp column: {timestamp_column}",
            details={"timestamp_column": timestamp_column},
        )

    series = df[timestamp_column]

    if not pd.api.types.is_datetime64_any_dtype(series):
        return CheckResult(
            name="check_timestamp_utc",
            passed=False,
            message=f"{timestamp_column} is not datetime dtype.",
            details={"dtype": str(series.dtype)},
        )

    timezone = getattr(series.dt, "tz", None)
    passed = str(timezone) == "UTC"

    return CheckResult(
        name="check_timestamp_utc",
        passed=passed,
        message=f"{timestamp_column} is UTC-aware." if passed else f"{timestamp_column} is not UTC-aware.",
        details={"timestamp_column": timestamp_column, "timezone": str(timezone)},
    )


def check_hourly_continuity(
    df: pd.DataFrame,
    timestamp_column: str = "timestamp_utc",
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
) -> CheckResult:
    """Check that an hourly UTC timestamp series has no missing hours.

    The check uses the min/max timestamp in the data unless start/end are supplied.
    """
    timestamp_check = check_timestamp_utc(df, timestamp_column)
    if not timestamp_check.passed:
        return CheckResult(
            name="check_hourly_continuity",
            passed=False,
            message="Cannot check hourly continuity because timestamp check failed.",
            details=timestamp_check.details,
        )

    actual = pd.DatetimeIndex(df[timestamp_column].drop_duplicates().sort_values())

    if len(actual) == 0:
        return CheckResult(
            name="check_hourly_continuity",
            passed=False,
            message="No timestamps available.",
            details={},
        )

    start_ts = pd.Timestamp(start, tz="UTC") if start is not None else actual.min()
    end_ts = pd.Timestamp(end, tz="UTC") if end is not None else actual.max()

    expected = pd.date_range(start=start_ts, end=end_ts, freq="h", tz="UTC")
    missing = expected.difference(actual)
    extra = actual.difference(expected)

    passed = len(missing) == 0 and len(extra) == 0

    return CheckResult(
        name="check_hourly_continuity",
        passed=passed,
        message="Hourly timestamp sequence is complete." if passed else "Hourly timestamp sequence has gaps or extras.",
        details={
            "start": str(start_ts),
            "end": str(end_ts),
            "expected_hours": len(expected),
            "actual_unique_hours": len(actual),
            "missing_hours_count": len(missing),
            "extra_hours_count": len(extra),
            "first_missing_hours": [str(value) for value in missing[:10]],
            "first_extra_hours": [str(value) for value in extra[:10]],
        },
    )


def missing_values_summary(df: pd.DataFrame) -> CheckResult:
    """Summarise missing values by column."""
    missing_counts = df.isna().sum().astype(int)
    missing_percent = (df.isna().mean() * 100).round(3)

    columns_with_missing = [
        column for column, count in missing_counts.items() if int(count) > 0
    ]

    return CheckResult(
        name="missing_values_summary",
        passed=len(columns_with_missing) == 0,
        message="No missing values found." if not columns_with_missing else "Missing values found.",
        details={
            "row_count": int(len(df)),
            "columns_with_missing": columns_with_missing,
            "missing_counts": missing_counts.to_dict(),
            "missing_percent": missing_percent.to_dict(),
        },
    )


def dataset_overview(
    df: pd.DataFrame,
    timestamp_column: str | None = "timestamp_utc",
) -> dict[str, Any]:
    """Return basic dataset metadata for README/data dictionary use."""
    overview: dict[str, Any] = {
        "row_count": int(len(df)),
        "column_count": int(len(df.columns)),
        "columns": list(df.columns),
    }

    if timestamp_column and timestamp_column in df.columns:
        timestamps = df[timestamp_column]
        overview["timestamp_min"] = str(timestamps.min())
        overview["timestamp_max"] = str(timestamps.max())

    return overview


def run_standard_time_series_checks(
    df: pd.DataFrame,
    required_columns: Iterable[str],
    key_columns: Iterable[str],
    timestamp_column: str = "timestamp_utc",
) -> list[CheckResult]:
    """Run standard checks for hourly time-series datasets."""
    return [
        require_columns(df, required_columns),
        check_snake_case_columns(df),
        check_timestamp_utc(df, timestamp_column=timestamp_column),
        check_no_duplicate_keys(df, key_columns=key_columns),
        check_hourly_continuity(df, timestamp_column=timestamp_column),
        missing_values_summary(df),
    ]
