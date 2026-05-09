"""
Task 3 -- Baseline shape libraries + 3-year ERA5 weather scaffold.

This task does not touch the ML model. It assembles the two ingredients that
Task 4 will combine via Priyanshi's Baseline + Weather Adjustment formula:

1. **Typical historic CF library** (``typical_cf_library.parquet``).
   Median historic capacity factor for wind and solar by
   ``month x hour x is_weekend``. Computed from
   ``supply_model_training_ready.parquet``. This is the *denominator* of the
   adjustment factor in Task 4.

2. **Hourly weather scaffold** (``weather_scaffold_hourly.parquet``).
   For each future hour in 2030-2045 and each of the three representative
   weather years (2010 = low wind, 2014 = average, 2015 = high), the matching
   ERA5 wind speed and SSRD are mapped onto the future calendar. Future
   Feb-29 hours fall back to Feb-28 weather (all three weather years are
   non-leap). Adds ``day_type``, ``is_weekend``, ``month``, ``hour`` so the
   baseline shape and typical CF joins in Task 4 are direct.

The baseline MW shape itself (``p50_mw`` per
``tech x month x day_type x hour``) is the ``genmix_profile_library.parquet``
already exported by Task 1 -- this task does not re-export it but documents
that it is the *baseline profile* in the Baseline + Adjustment formula.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PREPROCESSED = PROJECT_ROOT / "data" / "processed" / "preprocessing"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "objective1_generation"

CALENDAR_PATH = DATA_PREPROCESSED / "calendar_hourly_2010_2045.parquet"
ERA5_PATH = DATA_PREPROCESSED / "era5_resource_hourly_gb_2010_2024.parquet"
TRAINING_PATH = OUTPUT_DIR / "supply_model_training_ready.parquet"

OUTPUT_TYPICAL_CF = OUTPUT_DIR / "typical_cf_library.parquet"
OUTPUT_SCAFFOLD = OUTPUT_DIR / "weather_scaffold_hourly.parquet"

REPRESENTATIVE_WEATHER_YEARS: tuple[int, ...] = (2010, 2014, 2015)
WEATHER_YEAR_LABELS: dict[int, str] = {
    2010: "low_wind",
    2014: "average_wind",
    2015: "high_wind",
}
FUTURE_YEAR_START = 2030
FUTURE_YEAR_END = 2045


def _ensure_utc(s: pd.Series) -> pd.Series:
    if not pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s, utc=True)
    if s.dt.tz is None:
        return s.dt.tz_localize("UTC")
    return s.dt.tz_convert("UTC")


def build_typical_cf_library(training: pd.DataFrame) -> pd.DataFrame:
    """Median historic CF by (tech, month, hour, is_weekend) for wind and solar."""
    df = training.copy()
    df["timestamp_utc"] = _ensure_utc(df["timestamp_utc"])
    df["month"] = df["timestamp_utc"].dt.month.astype(np.int32)
    df["hour"] = df["timestamp_utc"].dt.hour.astype(np.int32)
    df["is_weekend"] = df["timestamp_utc"].dt.dayofweek >= 5

    rows: list[pd.DataFrame] = []
    for tech, cf_col in (("wind_total_mw", "wind_cf"), ("solar_mw", "solar_cf")):
        sub = (
            df.groupby(["month", "hour", "is_weekend"], as_index=False)[cf_col]
            .agg(typical_cf="median", n_samples="size")
        )
        sub["tech"] = tech
        rows.append(sub[["tech", "month", "hour", "is_weekend", "typical_cf", "n_samples"]])
    return pd.concat(rows, ignore_index=True)


def _weather_lookup_for_year(era5: pd.DataFrame, weather_year: int) -> pd.DataFrame:
    w = era5.loc[era5["timestamp_utc"].dt.year == weather_year].copy()
    if len(w) != 8760:
        raise ValueError(
            f"Expected 8760 hourly rows for weather year {weather_year}, got {len(w)}."
        )
    w["month"] = w["timestamp_utc"].dt.month.astype(np.int32)
    w["day"] = w["timestamp_utc"].dt.day.astype(np.int32)
    w["hour"] = w["timestamp_utc"].dt.hour.astype(np.int32)
    return w[["month", "day", "hour", "wind_speed_100m_ms", "ssrd_j_m2"]].drop_duplicates(
        subset=["month", "day", "hour"]
    )


def _scaffold_one_weather_year(
    cal_fut: pd.DataFrame, era5: pd.DataFrame, weather_year: int
) -> pd.DataFrame:
    """Map representative-year weather onto future timestamps. Feb-29 -> Feb-28."""
    ref = _weather_lookup_for_year(era5, weather_year)

    out = cal_fut.copy()
    out["lookup_day"] = np.where(
        (out["month"] == 2) & (out["day"] == 29),
        np.int32(28),
        out["day"].astype(np.int32),
    )
    merged = out.merge(
        ref,
        left_on=["month", "lookup_day", "hour"],
        right_on=["month", "day", "hour"],
        how="left",
        validate="many_to_one",
    )
    if merged["wind_speed_100m_ms"].isna().any() or merged["ssrd_j_m2"].isna().any():
        bad = merged.loc[merged["wind_speed_100m_ms"].isna() | merged["ssrd_j_m2"].isna()]
        raise ValueError(f"Weather scaffold has missing values; first bad row:\n{bad.head(1)}")

    merged = merged.drop(columns=["lookup_day", "day_y"], errors="ignore")
    if "day_x" in merged.columns:
        merged = merged.rename(columns={"day_x": "day"})

    merged["weather_year"] = np.int32(weather_year)
    merged["weather_year_role"] = WEATHER_YEAR_LABELS[weather_year]
    merged["is_weekend"] = merged["is_weekend"].astype(bool)
    # Lowercase ``weekday`` / ``weekend`` to match the preprocessed
    # ``genmix_profile_library`` ``day_type`` values exactly (case-sensitive merge).
    merged["day_type"] = np.where(merged["is_weekend"], "weekend", "weekday")
    return merged[
        [
            "timestamp_utc",
            "year",
            "weather_year",
            "weather_year_role",
            "month",
            "day",
            "hour",
            "is_weekend",
            "day_type",
            "wind_speed_100m_ms",
            "ssrd_j_m2",
        ]
    ]


def build_weather_scaffold(cal_fut: pd.DataFrame, era5: pd.DataFrame) -> pd.DataFrame:
    parts = [
        _scaffold_one_weather_year(cal_fut, era5, wy)
        for wy in REPRESENTATIVE_WEATHER_YEARS
    ]
    out = pd.concat(parts, ignore_index=True)
    return out.sort_values(["weather_year", "timestamp_utc"]).reset_index(drop=True)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    training = pd.read_parquet(TRAINING_PATH)
    typical_cf = build_typical_cf_library(training)
    typical_cf.to_parquet(OUTPUT_TYPICAL_CF, index=False)

    calendar = pd.read_parquet(CALENDAR_PATH)
    calendar["timestamp_utc"] = _ensure_utc(calendar["timestamp_utc"])
    future_mask = (
        (calendar["year"] >= FUTURE_YEAR_START) & (calendar["year"] <= FUTURE_YEAR_END)
    )
    cal_fut = calendar.loc[future_mask].sort_values("timestamp_utc").reset_index(drop=True)

    era5 = pd.read_parquet(
        ERA5_PATH, columns=["timestamp_utc", "wind_speed_100m_ms", "ssrd_j_m2"]
    )
    era5["timestamp_utc"] = _ensure_utc(era5["timestamp_utc"])

    scaffold = build_weather_scaffold(cal_fut, era5)
    scaffold.to_parquet(OUTPUT_SCAFFOLD, index=False)

    print("Task 3 -- baseline shapes + 3-year weather scaffold")
    print(f"  Typical CF library  : {OUTPUT_TYPICAL_CF.name}    rows={len(typical_cf):,}")
    print(f"  Weather scaffold    : {OUTPUT_SCAFFOLD.name}      rows={len(scaffold):,}")
    print(f"  Weather years       : {sorted(scaffold['weather_year'].unique())}")
    print(
        "  Hours per weather year (sanity): "
        + ", ".join(
            f"wy={wy}:{int((scaffold['weather_year']==wy).sum())}"
            for wy in sorted(scaffold["weather_year"].unique())
        )
    )


if __name__ == "__main__":
    main()
