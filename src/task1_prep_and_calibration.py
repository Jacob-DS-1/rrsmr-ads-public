"""
Task 1 -- Preparation and calibration.

Reads the read-only ``data/processed/preprocessing/`` inputs (DUKES capacity & load
factors, NESO genmix, ERA5 GB-mean resource, FES supply, the genmix profile
library, and the historic interconnector flows) and writes the foundational
artefacts the rest of the Objective 1 pipeline depends on:

1. ``supply_model_training_ready.parquet`` -- hourly genmix + ERA5 + DUKES
   wind/solar capacity, with capacity factors. This is the training table for
   the ML model in Task 2.
2. ``era5_resource_hourly.parquet`` -- GB-mean ERA5 hourly wind speed (m/s)
   and SSRD (J/m^2) for the three representative weather years used by
   Task 3 (2010 = low wind, 2014 = average, 2015 = high). All three are
   non-leap so the Feb-29 -> Feb-28 fallback in Task 3 carries cleanly.
3. ``tech_year_calibration.csv`` -- per (technology, year) DUKES installed
   capacity, observed genmix-derived annual generation (MWh / TWh), DUKES
   published load factor (%), and the implied load factor. Used by Task 5
   as a calibration constraint.
4. ``genmix_profile_library.parquet`` -- copy of the historic technology
   profile library augmented with an explicit ``is_weekend`` boolean.
5. ``genmix_taxonomy_map.csv`` -- the explicit raw NESO source -> internal
   name -> model column map, with each technology's role
   (renewable / dispatchable / storage / imports / reference) and FES anchor
   target. Diagnostic-only columns (gas, coal, imports) are clearly flagged.

No transformation in this task touches the future timeline -- it operates on
historical data only.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PREPROCESSED_DIR = PROJECT_ROOT / "data" / "processed" / "preprocessing"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "objective1_generation"

GENMIX_FILE = PREPROCESSED_DIR / "genmix_hist_hourly.parquet"
ERA5_FILE = PREPROCESSED_DIR / "era5_resource_hourly_gb_2010_2024.parquet"
DUKES_CAP_FILE = PREPROCESSED_DIR / "dukes_capacity_hist_2010_2024.parquet"
DUKES_LF_FILE = PREPROCESSED_DIR / "dukes_loadfactor_hist_2010_2024.parquet"
PROFILE_LIB_SRC = PREPROCESSED_DIR / "genmix_profile_library.parquet"

OUTPUT_TRAINING_PARQUET = OUTPUT_DIR / "supply_model_training_ready.parquet"
OUTPUT_ERA5_DELIV = OUTPUT_DIR / "era5_resource_hourly.parquet"
OUTPUT_CAL_CSV = OUTPUT_DIR / "tech_year_calibration.csv"
OUTPUT_PROFILE_LIB = OUTPUT_DIR / "genmix_profile_library.parquet"
OUTPUT_TAXONOMY_CSV = OUTPUT_DIR / "genmix_taxonomy_map.csv"

# Three representative ERA5 weather years, picked from the full 2010-2024
# hourly record by GB-mean wind speed:
#   2010 mean wind = 7.480 m/s  (lowest in the record)
#   2014 mean wind = 8.213 m/s  (closest to the 15-year median)
#   2015 mean wind = 8.699 m/s  (highest in the record)
# All three are non-leap (8760 h), so the Feb-29 -> Feb-28 fallback in Task 3
# carries cleanly across every future leap year (2032 / 2036 / 2040 / 2044).
REPRESENTATIVE_WEATHER_YEARS: tuple[int, ...] = (2010, 2014, 2015)
WEATHER_YEAR_LABELS: dict[int, str] = {
    2010: "low_wind",
    2014: "average_wind",
    2015: "high_wind",
}

# Calibration table: maps internal tech column to the (already cleaned)
# DUKES `tech` label used in the new preprocessed parquets. Capacity is from
# DUKES 5.7 (which now contains both thermal and renewable rows after the
# preprocessing rewrite); load factor is from DUKES 6.3 for renewables and
# DUKES 5.10.B for thermal/nuclear. The textual `dukes_*_label` fields are
# preserved in the calibration CSV for human readability.
CALIBRATION_TECHS: list[dict] = [
    {
        "internal_name": "wind_total_mw",
        "dukes_capacity_tech": "wind_total",
        "dukes_capacity_label": "DUKES 5.7: Offshore wind; Onshore wind",
        "dukes_lf_tech": "wind_total",
        "dukes_lf_label": "DUKES 6.3: Wind",
    },
    {
        "internal_name": "solar_mw",
        "dukes_capacity_tech": "solar",
        "dukes_capacity_label": "DUKES 5.7: Solar",
        "dukes_lf_tech": "solar",
        "dukes_lf_label": "DUKES 6.3: Solar photovoltaics",
    },
    {
        "internal_name": "nuclear_mw",
        "dukes_capacity_tech": "nuclear",
        "dukes_capacity_label": "DUKES 5.7: Nuclear stations",
        "dukes_lf_tech": "nuclear",
        "dukes_lf_label": "DUKES 5.10.B: Nuclear stations",
    },
    {
        "internal_name": "gas_mw",
        "dukes_capacity_tech": "gas",
        "dukes_capacity_label": "DUKES 5.7: Gas fired",
        "dukes_lf_tech": "gas",
        "dukes_lf_label": "DUKES 5.10.B: Combined cycle gas turbine stations",
    },
    {
        "internal_name": "coal_mw",
        "dukes_capacity_tech": "coal",
        "dukes_capacity_label": "DUKES 5.7: Coal fired",
        "dukes_lf_tech": "coal",
        "dukes_lf_label": "DUKES 5.10.B: of which coal-fired stations [note 7]",
    },
    {
        "internal_name": "hydro_mw",
        "dukes_capacity_tech": "hydro",
        "dukes_capacity_label": "DUKES 5.7: Hydro (natural flow)",
        "dukes_lf_tech": "hydro",
        "dukes_lf_label": "DUKES 6.3: Hydro",
    },
]

# Taxonomy: model technology columns and how they are treated in Obj 1.
# Roles:
#   renewable             -- weather-driven, ML+adjustment shape, FES-anchored.
#   dispatchable          -- historic profile shape, FES-anchored.
#   dispatchable_existing -- historic profile shape, FES-anchored, treated as
#                            the existing fleet baseline (SMRs added in Obj 3).
#   storage               -- net (sign: + discharge, - charge), FES-anchored.
#   imports_baseline      -- DUKES 5.13 baseline cap (no FES anchor here).
#   reference             -- diagnostic-only profile (no anchor; Obj 3 reshapes).
TAXONOMY_ROWS: list[dict] = [
    {
        "neso_source": "WIND + WIND_EMB",
        "internal_name": "wind_total_mw",
        "model_column": "wind_mw",
        "role": "renewable",
        "fes_target_techs": "Onshore Wind|Offshore Wind",
        "is_anchored_to_fes": True,
        "notes": "Wind: ML CF + baseline shape adjustment, anchored to FES TWh.",
    },
    {
        "neso_source": "SOLAR",
        "internal_name": "solar_mw",
        "model_column": "solar_mw",
        "role": "renewable",
        "fes_target_techs": "Solar PV",
        "is_anchored_to_fes": True,
        "notes": "Solar PV: ML CF + baseline shape adjustment, anchored to FES TWh.",
    },
    {
        "neso_source": "NUCLEAR",
        "internal_name": "nuclear_mw",
        "model_column": "nuclear_existing_mw",
        "role": "dispatchable_existing",
        "fes_target_techs": "Nuclear",
        "is_anchored_to_fes": True,
        "notes": "Existing nuclear fleet baseline. SMR contribution added in Obj 3.",
    },
    {
        "neso_source": "BIOMASS",
        "internal_name": "biomass_mw",
        "model_column": "biomass_mw",
        "role": "dispatchable",
        "fes_target_techs": "Biomass|CCS Biomass|Waste",
        "is_anchored_to_fes": True,
        "notes": "Biomass + CCS biomass + waste rolled up to one anchored series.",
    },
    {
        "neso_source": "HYDRO",
        "internal_name": "hydro_mw",
        "model_column": "hydro_mw",
        "role": "dispatchable",
        "fes_target_techs": "Hydro",
        "is_anchored_to_fes": True,
        "notes": "Hydro generation, anchored to FES Hydro TWh.",
    },
    {
        "neso_source": "OTHER",
        "internal_name": "other_mw",
        "model_column": "other_mw",
        "role": "dispatchable",
        "fes_target_techs": "Other Renewable|Other Thermal|Hydrogen",
        "is_anchored_to_fes": True,
        "notes": "Aggregates Other Renewable + Other Thermal + Hydrogen FES targets.",
    },
    {
        "neso_source": "STORAGE",
        "internal_name": "storage_net_mw",
        "model_column": "storage_net_mw",
        "role": "storage",
        "fes_target_techs": "Battery|Long Duration Energy Storage",
        "is_anchored_to_fes": True,
        "notes": (
            "Net storage. Sign: + = generation/discharge to grid, - = pumping/charge. "
            "Anchored to (Battery + LDES) FES TWh."
        ),
    },
    {
        "neso_source": "GAS",
        "internal_name": "gas_mw",
        "model_column": "gas_reference_mw",
        "role": "reference",
        "fes_target_techs": "Gas|CCS Gas",
        "is_anchored_to_fes": False,
        "notes": (
            "Reference / diagnostic only -- gas is the balancing source in Obj 3. "
            "Annual energy is capped at FES (Gas + CCS Gas) TWh as a sanity scale "
            "but not used for system balance in Obj 1."
        ),
    },
    {
        "neso_source": "COAL",
        "internal_name": "coal_mw",
        "model_column": "coal_reference_mw",
        "role": "reference",
        "fes_target_techs": "",
        "is_anchored_to_fes": False,
        "notes": (
            "Reference / diagnostic only. FES contains no Coal entry; the "
            "historic p50 profile is carried forward unscaled (no placeholder "
            "multiplier)."
        ),
    },
    {
        "neso_source": "IMPORTS (DUKES 5.13)",
        "internal_name": "imports_net_mw",
        "model_column": "imports_net_baseline_mw",
        "role": "imports_baseline",
        "fes_target_techs": "",
        "is_anchored_to_fes": False,
        "notes": (
            "Net imports baseline. Sign: + = importing to GB, - = exporting. "
            "Annual energy comes from the DUKES 5.13 2020-2024 mean Total; the "
            "hourly shape comes from the historic p50 profile."
        ),
    },
]


def _ensure_utc(s: pd.Series) -> pd.Series:
    if not pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s, utc=True)
    if s.dt.tz is None:
        return s.dt.tz_localize("UTC")
    return s.dt.tz_convert("UTC")


def _extract_gb_renewable_capacity_mw(dukes: pd.DataFrame) -> pd.DataFrame:
    """Annual wind and solar installed capacity (MW) from DUKES.

    The new preprocessed parquet has clean lowercase ``tech`` labels
    (``wind_total``, ``solar``, ...) so wind / solar capacity is a simple
    per-year max over the relevant rows.
    """
    records: list[dict] = []
    for year, group in dukes.groupby("year", sort=True):
        gtech = group["tech"].astype(str).str.strip().str.lower()

        wind_lines = group.loc[gtech == "wind_total", "capacity_mw"].dropna()
        if wind_lines.empty:
            raise ValueError(f"No wind_total capacity row for year {year}.")
        wind_capacity_mw = float(wind_lines.max())

        solar_lines = group.loc[gtech == "solar", "capacity_mw"].dropna()
        solar_capacity_mw = float(solar_lines.max()) if not solar_lines.empty else float("nan")

        records.append(
            {
                "year": int(year),
                "wind_capacity_mw": wind_capacity_mw,
                "solar_capacity_mw": solar_capacity_mw,
            }
        )

    return pd.DataFrame.from_records(records)


def _extract_capacity_for_tech(dukes_cap: pd.DataFrame, dukes_tech: str) -> pd.DataFrame:
    sub = dukes_cap[dukes_cap["tech"].astype(str).str.strip().str.lower() == dukes_tech.lower()]
    if sub.empty:
        return pd.DataFrame(columns=["year", "capacity_mw"])
    pos = sub[sub["capacity_mw"] > 0]
    if pos.empty:
        return pd.DataFrame(columns=["year", "capacity_mw"])
    return pos.groupby("year", as_index=False)["capacity_mw"].max()


def _extract_loadfactor_for_tech(dukes_lf: pd.DataFrame, dukes_tech: str) -> pd.DataFrame:
    sub = dukes_lf[dukes_lf["tech"].astype(str).str.strip().str.lower() == dukes_tech.lower()]
    if sub.empty:
        return pd.DataFrame(columns=["year", "load_factor_pct"])
    return sub.groupby("year", as_index=False)["load_factor_pct"].mean()


def _build_tech_year_calibration(
    genmix: pd.DataFrame,
    dukes_cap: pd.DataFrame,
    dukes_lf: pd.DataFrame,
) -> pd.DataFrame:
    g = genmix.copy()
    g["timestamp_utc"] = _ensure_utc(g["timestamp_utc"])
    g["year"] = g["timestamp_utc"].dt.year.astype(int)

    rows: list[pd.DataFrame] = []
    for spec in CALIBRATION_TECHS:
        col = spec["internal_name"]
        if col not in g.columns:
            continue
        annual_mwh = g.groupby("year", as_index=False)[col].sum().rename(
            columns={col: "annual_generation_mwh"}
        )
        annual_mwh["annual_generation_twh"] = annual_mwh["annual_generation_mwh"] / 1_000_000.0

        cap = _extract_capacity_for_tech(dukes_cap, spec["dukes_capacity_tech"])
        lf = _extract_loadfactor_for_tech(dukes_lf, spec["dukes_lf_tech"])

        merged = annual_mwh.merge(cap, on="year", how="left").merge(lf, on="year", how="left")
        merged["internal_name"] = col
        merged["dukes_capacity_label"] = spec["dukes_capacity_label"]
        merged["dukes_lf_label"] = spec["dukes_lf_label"]

        hours_per_year = merged["year"].apply(
            lambda y: 8784 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 8760
        )
        with np.errstate(divide="ignore", invalid="ignore"):
            implied = (
                merged["annual_generation_mwh"]
                / (merged["capacity_mw"] * hours_per_year)
                * 100.0
            )
        merged["implied_load_factor_pct"] = implied.replace([np.inf, -np.inf], np.nan)

        rows.append(merged)

    out = pd.concat(rows, ignore_index=True)
    cols = [
        "internal_name",
        "year",
        "dukes_capacity_label",
        "capacity_mw",
        "annual_generation_mwh",
        "annual_generation_twh",
        "dukes_lf_label",
        "load_factor_pct",
        "implied_load_factor_pct",
    ]
    return out[cols].sort_values(["internal_name", "year"]).reset_index(drop=True)


def _enrich_profile_library(profile: pd.DataFrame) -> pd.DataFrame:
    out = profile.copy()
    out["is_weekend"] = (
        out["day_type"].astype(str).str.strip().str.lower() == "weekend"
    )
    cols = ["tech", "month", "day_type", "is_weekend", "hour", "p10_mw", "p50_mw", "p90_mw"]
    return out[[c for c in cols if c in out.columns]]


def build_training_frame() -> pd.DataFrame:
    genmix = pd.read_parquet(GENMIX_FILE)
    era5 = pd.read_parquet(
        ERA5_FILE,
        columns=["timestamp_utc", "wind_speed_100m_ms", "ssrd_j_m2"],
    )
    dukes = pd.read_parquet(
        DUKES_CAP_FILE,
        columns=["year", "tech", "capacity_mw", "source_table"],
    )

    capacity_year = _extract_gb_renewable_capacity_mw(dukes)

    train = genmix[["timestamp_utc", "wind_total_mw", "solar_mw"]].merge(
        era5, on="timestamp_utc", how="inner"
    )
    train["timestamp_utc"] = _ensure_utc(train["timestamp_utc"])
    train["year"] = train["timestamp_utc"].dt.year.astype("int64")

    out = train.merge(capacity_year, on="year", how="left")
    out = out.loc[(out["wind_capacity_mw"] > 0) & (out["solar_capacity_mw"] > 0)].copy()

    out["wind_cf"] = out["wind_total_mw"] / out["wind_capacity_mw"]
    out["solar_cf"] = out["solar_mw"] / out["solar_capacity_mw"]

    pos_gen = (out["wind_total_mw"] >= 0) & (out["solar_mw"] >= 0)
    cf_ok = (
        (out["wind_cf"] >= 0)
        & (out["wind_cf"] <= 1.0)
        & (out["solar_cf"] >= 0)
        & (out["solar_cf"] <= 1.0)
    )

    out = out.loc[pos_gen & cf_ok].dropna(
        subset=[
            "timestamp_utc",
            "wind_total_mw",
            "solar_mw",
            "wind_speed_100m_ms",
            "ssrd_j_m2",
            "wind_capacity_mw",
            "solar_capacity_mw",
            "wind_cf",
            "solar_cf",
        ]
    )

    column_order = [
        "timestamp_utc",
        "year",
        "wind_total_mw",
        "solar_mw",
        "wind_speed_100m_ms",
        "ssrd_j_m2",
        "wind_capacity_mw",
        "solar_capacity_mw",
        "wind_cf",
        "solar_cf",
    ]
    return out[column_order].sort_values("timestamp_utc").reset_index(drop=True)


def write_era5_resource_deliverable() -> int:
    era5 = pd.read_parquet(ERA5_FILE)
    era5["timestamp_utc"] = _ensure_utc(era5["timestamp_utc"])
    era5["year"] = era5["timestamp_utc"].dt.year.astype(int)
    sub = era5[era5["year"].isin(REPRESENTATIVE_WEATHER_YEARS)].copy()
    sub["weather_year_role"] = sub["year"].map(WEATHER_YEAR_LABELS)
    sub = sub.sort_values(["year", "timestamp_utc"]).reset_index(drop=True)
    sub.to_parquet(OUTPUT_ERA5_DELIV, index=False)
    return len(sub)


def write_taxonomy_csv() -> None:
    df = pd.DataFrame.from_records(TAXONOMY_ROWS)
    df.to_csv(OUTPUT_TAXONOMY_CSV, index=False)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    df = build_training_frame()
    df.to_parquet(OUTPUT_TRAINING_PARQUET, index=False)

    n_era5 = write_era5_resource_deliverable()

    genmix = pd.read_parquet(GENMIX_FILE)
    dukes_cap = pd.read_parquet(DUKES_CAP_FILE)
    dukes_lf = pd.read_parquet(DUKES_LF_FILE)
    cal = _build_tech_year_calibration(genmix, dukes_cap, dukes_lf)
    cal.to_csv(OUTPUT_CAL_CSV, index=False)

    profile = pd.read_parquet(PROFILE_LIB_SRC)
    enriched = _enrich_profile_library(profile)
    enriched.to_parquet(OUTPUT_PROFILE_LIB, index=False)

    write_taxonomy_csv()

    print("Task 1 -- preparation and calibration")
    print(f"  Training table          : {OUTPUT_TRAINING_PARQUET.name}    rows={len(df):,}")
    print(f"  ERA5 resource deliverable : {OUTPUT_ERA5_DELIV.name}        rows={n_era5:,}")
    print(f"  Tech-year calibration   : {OUTPUT_CAL_CSV.name}           rows={len(cal):,}")
    print(f"  Profile library copy    : {OUTPUT_PROFILE_LIB.name}     rows={len(enriched):,}")
    print(f"  Taxonomy map            : {OUTPUT_TAXONOMY_CSV.name}      rows={len(TAXONOMY_ROWS)}")


if __name__ == "__main__":
    main()
