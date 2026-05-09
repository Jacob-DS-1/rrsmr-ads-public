"""
Task 4 -- Weather adjustment + unscaled future supply.

Combines the ingredients produced by Tasks 1-3 to emit the **unscaled**
hourly future supply (per representative weather year) using Priyanshi's
Baseline + Weather Adjustment formula:

    unscaled_MW = baseline p50_mw  x  ( ML predicted CF / typical historic CF )

Renewables (wind, solar)
------------------------
For each future hour and each of the three representative weather years
(2010 = low wind, 2014 = average, 2015 = high):

1. The 3-year ERA5 weather scaffold from Task 3 supplies the wind speed and
   SSRD on the future calendar (Feb-29 -> Feb-28 fallback).
2. ``era5_renewable_models.joblib`` from Task 2 predicts the weather-driven
   capacity factor for each hour.
3. The typical historic CF library from Task 3 supplies the median observed
   CF by ``month x hour x is_weekend`` (the *denominator* of the adjustment
   factor).
4. Adjustment factor = predicted CF / typical CF, clipped to [0, 5]. Hours
   where the typical CF is below 1e-3 (e.g. solar at night) are forced to a
   factor of 1.0 -- the baseline MW is also ~0 there so the product is ~0.
5. Unscaled MW = baseline ``p50_mw`` (from ``genmix_profile_library``) x
   adjustment factor.

Dispatchables and storage
-------------------------
Nuclear, biomass, hydro, "other", coal, gas, and storage net use the historic
``p50_mw`` profile directly (no weather correction). They are still emitted
once per weather year so the parquet schema is uniform downstream.

Imports baseline
----------------
``imports_net_mw`` is built from DUKES 5.13: the 2020-2024 mean Total net
imports (GWh -> TWh) is held flat across 2030-2045 as a baseline cap, with
the historic ``p50_mw`` hourly shape rescaled per year so the annual sum
matches the cap. Sign convention: + = import to GB, - = export.

Output
------
``future_supply_hourly_unscaled.parquet`` (long form):
columns = ``timestamp_utc``, ``year``, ``weather_year``, ``weather_year_role``,
``tech``, ``unscaled_generation_mw``.
"""

from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import pandas as pd

# Importing task2_ml_training registers the ``clipped_regressor`` sys.modules
# alias so the legacy joblib pickle loads cleanly.
import task2_ml_training  # noqa: F401

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PREPROCESSED = PROJECT_ROOT / "data" / "processed" / "preprocessing"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "objective1_generation"

INTERCONNECTOR_PATH = DATA_PREPROCESSED / "interconnector_annual_hist_2010_2024.parquet"

PROFILE_LIBRARY_PATH = OUTPUT_DIR / "genmix_profile_library.parquet"
WEATHER_SCAFFOLD_PATH = OUTPUT_DIR / "weather_scaffold_hourly.parquet"
TYPICAL_CF_PATH = OUTPUT_DIR / "typical_cf_library.parquet"
MODELS_PATH = OUTPUT_DIR / "era5_renewable_models.joblib"

OUTPUT_PARQUET = OUTPUT_DIR / "future_supply_hourly_unscaled.parquet"

REPRESENTATIVE_WEATHER_YEARS: tuple[int, ...] = (2010, 2014, 2015)
WEATHER_YEAR_LABELS: dict[int, str] = {
    2010: "low_wind",
    2014: "average_wind",
    2015: "high_wind",
}

# DUKES 5.13 baseline-cap window for imports.
IMPORTS_BASELINE_YEARS: tuple[int, ...] = (2020, 2021, 2022, 2023, 2024)

# Adjustment-factor guards (Priyanshi's Baseline + Weather Adjustment).
ADJ_FACTOR_MAX = 5.0
TYPICAL_CF_FLOOR = 1e-3  # below this the typical CF is treated as "near zero"

# Profile-only dispatchables/storage that stay on the historic p50 shape.
PROFILE_TECHS: tuple[str, ...] = (
    "biomass_mw",
    "coal_mw",
    "gas_mw",
    "hydro_mw",
    "nuclear_mw",
    "other_mw",
    "storage_net_mw",
)
IMPORTS_TECH = "imports_net_mw"


def _ensure_utc(s: pd.Series) -> pd.Series:
    if not pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s, utc=True)
    if s.dt.tz is None:
        return s.dt.tz_localize("UTC")
    return s.dt.tz_convert("UTC")


def _baseline_for_tech(profile: pd.DataFrame, tech: str) -> pd.DataFrame:
    sub = profile[profile["tech"] == tech].copy()
    if sub.empty:
        raise ValueError(f"No profile-library rows for tech {tech!r}.")
    if "is_weekend" not in sub.columns:
        sub["is_weekend"] = sub["day_type"].astype(str).str.lower().eq("weekend")
    return sub[["month", "day_type", "is_weekend", "hour", "p50_mw"]]


def _adjustment_factor(predicted_cf: np.ndarray, typical_cf: np.ndarray) -> np.ndarray:
    """Adjustment factor = predicted CF / typical CF with safe divide and bounded range."""
    pred = np.asarray(predicted_cf, dtype=np.float64)
    typ = np.asarray(typical_cf, dtype=np.float64)
    factor = np.ones_like(pred)
    mask = typ >= TYPICAL_CF_FLOOR
    factor[mask] = np.clip(pred[mask] / typ[mask], 0.0, ADJ_FACTOR_MAX)
    return factor


def _build_renewable_unscaled(
    scaffold: pd.DataFrame,
    profile: pd.DataFrame,
    typical_cf: pd.DataFrame,
    wind_model,
    solar_model,
) -> pd.DataFrame:
    wind_baseline = _baseline_for_tech(profile, "wind_total_mw").rename(
        columns={"p50_mw": "baseline_mw"}
    )
    solar_baseline = _baseline_for_tech(profile, "solar_mw").rename(
        columns={"p50_mw": "baseline_mw"}
    )
    typical_wind = typical_cf[typical_cf["tech"] == "wind_total_mw"][
        ["month", "hour", "is_weekend", "typical_cf"]
    ].copy()
    typical_solar = typical_cf[typical_cf["tech"] == "solar_mw"][
        ["month", "hour", "is_weekend", "typical_cf"]
    ].copy()

    parts: list[pd.DataFrame] = []
    for wy in REPRESENTATIVE_WEATHER_YEARS:
        wy_scaffold = scaffold[scaffold["weather_year"] == wy].copy()
        if wy_scaffold.empty:
            raise ValueError(f"Weather scaffold has no rows for weather_year={wy}.")

        x_wind = wy_scaffold[["wind_speed_100m_ms"]].to_numpy(dtype=np.float64)
        x_solar = wy_scaffold[["ssrd_j_m2"]].to_numpy(dtype=np.float64)
        wind_pred_cf = np.asarray(wind_model.predict(x_wind), dtype=np.float64)
        solar_pred_cf = np.asarray(solar_model.predict(x_solar), dtype=np.float64)

        # Wind branch.
        w = wy_scaffold[["timestamp_utc", "year", "month", "day_type", "is_weekend", "hour"]].copy()
        w["predicted_cf"] = wind_pred_cf
        w = w.merge(typical_wind, on=["month", "hour", "is_weekend"], how="left")
        w = w.merge(wind_baseline, on=["month", "day_type", "is_weekend", "hour"], how="left")
        if w["typical_cf"].isna().any() or w["baseline_mw"].isna().any():
            raise ValueError("Wind baseline/typical join produced NaNs.")
        w["adjustment_factor"] = _adjustment_factor(
            w["predicted_cf"].to_numpy(), w["typical_cf"].to_numpy()
        )
        w["unscaled_generation_mw"] = w["baseline_mw"].to_numpy() * w["adjustment_factor"]
        w["tech"] = "unscaled_wind_mw"
        w["weather_year"] = wy
        w["weather_year_role"] = WEATHER_YEAR_LABELS[wy]
        parts.append(
            w[
                [
                    "timestamp_utc",
                    "year",
                    "weather_year",
                    "weather_year_role",
                    "tech",
                    "unscaled_generation_mw",
                ]
            ]
        )

        # Solar branch.
        s = wy_scaffold[["timestamp_utc", "year", "month", "day_type", "is_weekend", "hour"]].copy()
        s["predicted_cf"] = solar_pred_cf
        s = s.merge(typical_solar, on=["month", "hour", "is_weekend"], how="left")
        s = s.merge(solar_baseline, on=["month", "day_type", "is_weekend", "hour"], how="left")
        if s["typical_cf"].isna().any() or s["baseline_mw"].isna().any():
            raise ValueError("Solar baseline/typical join produced NaNs.")
        s["adjustment_factor"] = _adjustment_factor(
            s["predicted_cf"].to_numpy(), s["typical_cf"].to_numpy()
        )
        s["unscaled_generation_mw"] = s["baseline_mw"].to_numpy() * s["adjustment_factor"]
        s["tech"] = "unscaled_solar_mw"
        s["weather_year"] = wy
        s["weather_year_role"] = WEATHER_YEAR_LABELS[wy]
        parts.append(
            s[
                [
                    "timestamp_utc",
                    "year",
                    "weather_year",
                    "weather_year_role",
                    "tech",
                    "unscaled_generation_mw",
                ]
            ]
        )

    return pd.concat(parts, ignore_index=True)


def _build_dispatchable_unscaled(
    scaffold: pd.DataFrame, profile: pd.DataFrame
) -> pd.DataFrame:
    """Map historic p50 profile onto future hours for each non-renewable tech."""
    cal = (
        scaffold[scaffold["weather_year"] == REPRESENTATIVE_WEATHER_YEARS[0]][
            ["timestamp_utc", "year", "month", "day_type", "hour"]
        ]
        .copy()
        .reset_index(drop=True)
    )

    techs = list(PROFILE_TECHS)
    lib = profile[profile["tech"].isin(techs)].copy()
    merged = cal.merge(lib, on=["month", "day_type", "hour"], how="left")
    if len(merged) != len(cal) * len(techs):
        raise ValueError(
            f"Profile merge row count mismatch: got {len(merged)}, expected {len(cal) * len(techs)}."
        )

    base = merged[["timestamp_utc", "year", "tech", "p50_mw"]].rename(
        columns={"p50_mw": "unscaled_generation_mw"}
    )

    parts: list[pd.DataFrame] = []
    for wy in REPRESENTATIVE_WEATHER_YEARS:
        b = base.copy()
        b["weather_year"] = wy
        b["weather_year_role"] = WEATHER_YEAR_LABELS[wy]
        parts.append(
            b[
                [
                    "timestamp_utc",
                    "year",
                    "weather_year",
                    "weather_year_role",
                    "tech",
                    "unscaled_generation_mw",
                ]
            ]
        )
    return pd.concat(parts, ignore_index=True)


def _imports_baseline_twh() -> float:
    """DUKES 5.13 baseline cap = mean Total net imports across IMPORTS_BASELINE_YEARS."""
    df = pd.read_parquet(INTERCONNECTOR_PATH)
    total = df[df["connector_or_total"] == "Total"]
    sub = total[total["year"].isin(IMPORTS_BASELINE_YEARS)]
    if sub.empty:
        raise ValueError(
            f"No DUKES 5.13 'Total' rows for years {IMPORTS_BASELINE_YEARS}; "
            "cannot build imports baseline."
        )
    mean_gwh = float(sub["net_imports_gwh"].mean())
    return mean_gwh / 1000.0  # GWh -> TWh


def _build_imports_unscaled(
    scaffold: pd.DataFrame, profile: pd.DataFrame
) -> pd.DataFrame:
    cal = (
        scaffold[scaffold["weather_year"] == REPRESENTATIVE_WEATHER_YEARS[0]][
            ["timestamp_utc", "year", "month", "day_type", "hour"]
        ]
        .copy()
        .reset_index(drop=True)
    )
    lib = profile[profile["tech"] == IMPORTS_TECH][
        ["month", "day_type", "hour", "p50_mw"]
    ].copy()
    merged = cal.merge(lib, on=["month", "day_type", "hour"], how="left")
    if merged["p50_mw"].isna().any():
        raise ValueError("Imports profile-library merge produced NaNs.")

    baseline_twh = _imports_baseline_twh()

    merged = merged[["timestamp_utc", "year", "p50_mw"]].copy()
    annual_sum_mwh = merged.groupby("year")["p50_mw"].transform("sum")
    annual_sum_twh = annual_sum_mwh / 1_000_000.0
    with np.errstate(divide="ignore", invalid="ignore"):
        scaled = merged["p50_mw"] * (baseline_twh / annual_sum_twh)
    scaled = scaled.where(annual_sum_twh > 0.0, 0.0)
    merged["unscaled_generation_mw"] = scaled.astype(np.float64)
    merged["tech"] = IMPORTS_TECH

    parts: list[pd.DataFrame] = []
    for wy in REPRESENTATIVE_WEATHER_YEARS:
        b = merged.copy()
        b["weather_year"] = wy
        b["weather_year_role"] = WEATHER_YEAR_LABELS[wy]
        parts.append(
            b[
                [
                    "timestamp_utc",
                    "year",
                    "weather_year",
                    "weather_year_role",
                    "tech",
                    "unscaled_generation_mw",
                ]
            ]
        )
    return pd.concat(parts, ignore_index=True)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    scaffold = pd.read_parquet(WEATHER_SCAFFOLD_PATH)
    scaffold["timestamp_utc"] = _ensure_utc(scaffold["timestamp_utc"])
    scaffold["is_weekend"] = scaffold["is_weekend"].astype(bool)

    profile = pd.read_parquet(PROFILE_LIBRARY_PATH)
    if "is_weekend" not in profile.columns:
        profile["is_weekend"] = profile["day_type"].astype(str).str.lower().eq("weekend")

    typical_cf = pd.read_parquet(TYPICAL_CF_PATH)

    bundle = joblib.load(MODELS_PATH)
    wind_model = bundle["wind"]
    solar_model = bundle["solar"]

    renewables = _build_renewable_unscaled(
        scaffold, profile, typical_cf, wind_model, solar_model
    )
    dispatchables = _build_dispatchable_unscaled(scaffold, profile)
    imports = _build_imports_unscaled(scaffold, profile)

    long = pd.concat([renewables, dispatchables, imports], ignore_index=True)
    if long["unscaled_generation_mw"].isna().any():
        bad = long.loc[long["unscaled_generation_mw"].isna()].head(3)
        raise ValueError(f"NaN values in unscaled_generation_mw; sample:\n{bad}")

    long = long.sort_values(
        ["weather_year", "timestamp_utc", "tech"], kind="mergesort"
    ).reset_index(drop=True)
    long.to_parquet(OUTPUT_PARQUET, index=False)

    summary = (
        long.groupby(["weather_year", "weather_year_role", "tech"], as_index=False)[
            "unscaled_generation_mw"
        ]
        .agg(annual_mean_mw="mean", total_mwh="sum")
    )
    n_years = int(scaffold["year"].nunique())
    summary["total_twh_per_year_avg"] = summary["total_mwh"] / 1_000_000.0 / n_years
    print("Task 4 -- weather adjustment + unscaled future supply")
    print(summary.to_string(index=False))
    print(f"\nSaved: {OUTPUT_PARQUET}  ({len(long):,} rows)")


if __name__ == "__main__":
    main()
