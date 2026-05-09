"""Validate a CSV or Parquet dataset using standard project checks."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from rrsmr_ads.validation import run_standard_time_series_checks


def load_dataset(path: Path) -> pd.DataFrame:
    """Load a CSV or Parquet dataset."""
    if path.suffix == ".parquet":
        return pd.read_parquet(path)

    if path.suffix == ".csv":
        return pd.read_csv(path, parse_dates=["timestamp_utc"] if "timestamp_utc" else None)

    raise ValueError(f"Unsupported file type: {path.suffix}")


def result_to_dict(result) -> dict:
    """Convert CheckResult to a JSON-serialisable dict."""
    return {
        "name": result.name,
        "passed": result.passed,
        "message": result.message,
        "details": result.details,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True, help="Path to CSV or Parquet file.")
    parser.add_argument(
        "--required-columns",
        nargs="+",
        required=True,
        help="Required columns for the dataset.",
    )
    parser.add_argument(
        "--key-columns",
        nargs="+",
        required=True,
        help="Columns that should uniquely identify rows.",
    )
    args = parser.parse_args()

    path = Path(args.path)
    df = load_dataset(path)

    if "timestamp_utc" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    results = run_standard_time_series_checks(
        df=df,
        required_columns=args.required_columns,
        key_columns=args.key_columns,
    )

    print(json.dumps([result_to_dict(result) for result in results], indent=2))

    return 0 if all(result.passed for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
