"""
Task 5 -- FES anchoring, DUKES load-factor QA, and final wide export.

Consolidated final stage of the Objective 1 pipeline. Takes the unscaled
hourly supply produced by Task 4, anchors each FES-anchored technology to
its annual FES TWh target, runs the integration QA suite (calendar coverage,
sign convention, FES reconciliation, DUKES 6.3 / 5.10B load-factor envelope),
and exports the final wide parquet with the renamed schema columns.

Pipeline
--------
1. **FES anchoring** -- per ``(year, fes_scenario, weather_year, model_tech)``
   compute the explicit multiplier from unscaled annual TWh to the FES TWh
   target for that technology. Non-anchored techs (coal, imports baseline)
   carry through with multiplier = 1.0. Audit table written to
   ``fes_anchoring_multipliers.csv``.

2. **Pivot to wide schema** with the post-review column renames:
     wind_mw, solar_mw, nuclear_existing_mw, biomass_mw, hydro_mw, other_mw,
     storage_net_mw, gas_reference_mw, coal_reference_mw,
     imports_net_baseline_mw.
   Keyed by ``(timestamp_utc, fes_scenario, weather_year)``.

3. **Integration QA** -- hour-count check, duplicate-key check, sign
   convention check, FES annual reconciliation (per anchored column).

4. **DUKES load-factor envelope check**
   -- *STRICT* on renewables: ML predicted CF (per representative weather
     year) must lie within DUKES 6.3 [min - 5pp, max + 5pp].
   -- *INFO* on anchored fleets (nuclear, hydro, biomass): implied annual LF
     vs DUKES 5.10B / 6.3 envelope, reported but not failed because FES
     capacity is not available in this workflow.

5. **Validation plots** -- ERA5 fit, annual pathway by tech, mean diurnal 2030.

6. **QA notes markdown** -- ``qa_notes.md`` summarising every gate.

Outputs
-------
- ``future_supply_hourly_fes_anchored.parquet``  (long form, anchored MW)
- ``generation_future_hourly_2030_2045.parquet`` (final wide, MW per tech)
- ``fes_anchoring_multipliers.csv``              (per-key multiplier audit)
- ``qa_dukes_loadfactor_check.csv``              (STRICT + INFO LF envelope)
- ``qa_fes_reconciliation.csv``                  (anchored TWh vs FES TWh)
- ``qa_notes.md``                                (gate summary)
- ``qa_plots/qa_era5_fit.png``
- ``qa_plots/qa_annual_pathway.png``
- ``qa_plots/qa_diurnal_2030.png``
"""

from __future__ import annotations

import calendar
import sys
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Importing task2_ml_training registers the legacy ``clipped_regressor``
# sys.modules alias so the joblib pickle loads cleanly.
import task2_ml_training  # noqa: F401

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PREPROCESSED = PROJECT_ROOT / "data" / "processed" / "preprocessing"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "objective1_generation"
PROCESSED_OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "objective1_generation"
PLOTS_DIR = OUTPUT_DIR / "qa_plots"

UNSCALED_HOURLY_PATH = OUTPUT_DIR / "future_supply_hourly_unscaled.parquet"
FES_SUPPLY_PATH = DATA_PREPROCESSED / "fes_supply_annual_2030_2045.parquet"
DUKES_LF_PATH = DATA_PREPROCESSED / "dukes_loadfactor_hist_2010_2024.parquet"
ERA5_HIST_PATH = DATA_PREPROCESSED / "era5_resource_hourly_gb_2010_2024.parquet"
TRAINING_PATH = OUTPUT_DIR / "supply_model_training_ready.parquet"
MODELS_PATH = OUTPUT_DIR / "era5_renewable_models.joblib"
CALIBRATION_PATH = OUTPUT_DIR / "tech_year_calibration.csv"

ANCHORED_LONG_PATH = OUTPUT_DIR / "future_supply_hourly_fes_anchored.parquet"
WIDE_EXPORT_PATH = PROCESSED_OUTPUT_DIR / "generation_future_hourly_2030_2045.parquet"
LEGACY_WIDE_EXPORT_PATH = OUTPUT_DIR / "generation_future_hourly_2030_2045.parquet"
MULTIPLIERS_AUDIT_CSV = OUTPUT_DIR / "fes_anchoring_multipliers.csv"
QA_LF_CSV = OUTPUT_DIR / "qa_dukes_loadfactor_check.csv"
QA_FES_CSV = OUTPUT_DIR / "qa_fes_reconciliation.csv"
QA_NOTES_MD = OUTPUT_DIR / "qa_notes.md"

FES_SCENARIOS: tuple[str, ...] = ("Electric Engagement", "Holistic Transition")

# Per unscaled-tech: model role + FES tech labels to sum + anchor flag.
# FES tech labels match the new preprocessed ``fes_supply_annual`` schema:
# biomass, biomass_ccs, gas, gas_ccs, hydro, hydrogen, nuclear,
# other_renewable, other_thermal, solar, storage, waste, wind_total.
TECH_ANCHORING: dict[str, dict] = {
    "unscaled_wind_mw": {
        "model_role": "renewable",
        "fes_techs": ["wind_total"],
        "is_anchored_to_fes": True,
    },
    "unscaled_solar_mw": {
        "model_role": "renewable",
        "fes_techs": ["solar"],
        "is_anchored_to_fes": True,
    },
    "nuclear_mw": {
        "model_role": "dispatchable_existing",
        "fes_techs": ["nuclear"],
        "is_anchored_to_fes": True,
    },
    "biomass_mw": {
        "model_role": "dispatchable",
        "fes_techs": ["biomass", "biomass_ccs", "waste"],
        "is_anchored_to_fes": True,
    },
    "hydro_mw": {
        "model_role": "dispatchable",
        "fes_techs": ["hydro"],
        "is_anchored_to_fes": True,
    },
    "other_mw": {
        "model_role": "dispatchable",
        "fes_techs": ["other_renewable", "other_thermal", "hydrogen"],
        "is_anchored_to_fes": True,
    },
    "storage_net_mw": {
        "model_role": "storage",
        "fes_techs": ["storage"],
        "is_anchored_to_fes": True,
    },
    "gas_mw": {
        "model_role": "reference",
        "fes_techs": ["gas", "gas_ccs"],
        # Diagnostic ceiling -- Obj 3 treats gas as the system rebalance source.
        "is_anchored_to_fes": True,
    },
    "coal_mw": {
        "model_role": "reference",
        "fes_techs": [],
        "is_anchored_to_fes": False,
    },
    "imports_net_mw": {
        "model_role": "imports_baseline",
        "fes_techs": [],
        "is_anchored_to_fes": False,
    },
}

# Long ``tech`` -> wide column name (post-review schema renames).
TECH_TO_WIDE_COLUMN: dict[str, str] = {
    "unscaled_wind_mw": "wind_mw",
    "unscaled_solar_mw": "solar_mw",
    "biomass_mw": "biomass_mw",
    "coal_mw": "coal_reference_mw",
    "gas_mw": "gas_reference_mw",
    "hydro_mw": "hydro_mw",
    "imports_net_mw": "imports_net_baseline_mw",
    "nuclear_mw": "nuclear_existing_mw",
    "other_mw": "other_mw",
    "storage_net_mw": "storage_net_mw",
}

# Wide column -> FES techs to sum (for QA reconciliation). Same labels as
# in ``TECH_ANCHORING`` above (post-rewrite lowercase FES schema).
WIDE_COLUMN_TO_FES_TECHS: dict[str, list[str]] = {
    "wind_mw": ["wind_total"],
    "solar_mw": ["solar"],
    "biomass_mw": ["biomass", "biomass_ccs", "waste"],
    "gas_reference_mw": ["gas", "gas_ccs"],
    "hydro_mw": ["hydro"],
    "nuclear_existing_mw": ["nuclear"],
    "other_mw": ["other_renewable", "other_thermal", "hydrogen"],
    "storage_net_mw": ["storage"],
}

NON_ANCHORED_DIAGNOSTIC_COLS: tuple[str, ...] = (
    "coal_reference_mw",
    "imports_net_baseline_mw",
)

# DUKES 5.10B / 6.3 ``tech`` labels (new preprocessed schema is lowercase).
DUKES_LF_LABELS: dict[str, str] = {
    "wind_mw": "wind_total",
    "solar_mw": "solar",
    "nuclear_existing_mw": "nuclear",
    "hydro_mw": "hydro",
    "biomass_mw": "biomass",
}

LF_TOLERANCE_PP = 5.0
RTOL_TWH = 1e-9
ATOL_TWH = 1e-6


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------


def _ensure_utc(s: pd.Series) -> pd.Series:
    if not pd.api.types.is_datetime64_any_dtype(s):
        return pd.to_datetime(s, utc=True)
    if s.dt.tz is None:
        return s.dt.tz_localize("UTC")
    return s.dt.tz_convert("UTC")


def _expected_hours(year: int) -> int:
    return 8784 if calendar.isleap(year) else 8760


# ----------------------------------------------------------------------
# Step 1 -- FES anchoring
# ----------------------------------------------------------------------


def _aggregate_fes_twh(fes: pd.DataFrame) -> pd.DataFrame:
    if "value" not in fes.columns:
        raise ValueError("FES supply table must contain a `value` column (TWh).")
    return (
        fes.groupby(["year", "fes_scenario", "tech"], as_index=False)["value"]
        .sum()
        .rename(columns={"value": "value_twh"})
    )


def _fes_targets_per_unscaled_tech(fes_agg: pd.DataFrame) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for unscaled_tech, spec in TECH_ANCHORING.items():
        if not spec["is_anchored_to_fes"] or not spec["fes_techs"]:
            continue
        sub = fes_agg[fes_agg["tech"].isin(spec["fes_techs"])]
        summed = sub.groupby(["year", "fes_scenario"], as_index=False)["value_twh"].sum()
        summed["tech"] = unscaled_tech
        summed["model_role"] = spec["model_role"]
        parts.append(summed)
    return pd.concat(parts, ignore_index=True)


def _compute_multipliers(
    annual_unscaled: pd.DataFrame, fes_targets: pd.DataFrame
) -> pd.DataFrame:
    merged = fes_targets.merge(
        annual_unscaled, on=["year", "weather_year", "tech"], how="left"
    )
    mult = np.ones(len(merged), dtype=np.float64)
    has_target = merged["value_twh"].notna()
    pos_denom = merged["unscaled_annual_twh"] > 1e-12
    use_ratio = has_target & pos_denom
    mult[use_ratio.to_numpy()] = (
        merged.loc[use_ratio, "value_twh"].to_numpy()
        / merged.loc[use_ratio, "unscaled_annual_twh"].to_numpy()
    )
    zero_shape = has_target & ~pos_denom
    mult[zero_shape.to_numpy()] = 0.0
    merged["multiplier"] = mult
    return merged[
        [
            "year",
            "weather_year",
            "fes_scenario",
            "tech",
            "model_role",
            "value_twh",
            "unscaled_annual_twh",
            "multiplier",
        ]
    ]


def _anchor_to_fes(unscaled: pd.DataFrame, fes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    annual = (
        unscaled.groupby(["year", "weather_year", "tech"], as_index=False)[
            "unscaled_generation_mw"
        ]
        .sum()
        .rename(columns={"unscaled_generation_mw": "unscaled_annual_mwh"})
    )
    annual["unscaled_annual_twh"] = annual["unscaled_annual_mwh"] / 1_000_000.0
    annual_twh = annual[["year", "weather_year", "tech", "unscaled_annual_twh"]]

    fes_agg = _aggregate_fes_twh(fes)
    unknown = set(fes_agg["fes_scenario"].unique()) - set(FES_SCENARIOS)
    if unknown:
        raise ValueError(f"Unexpected FES scenarios: {sorted(unknown)}")

    fes_targets = _fes_targets_per_unscaled_tech(fes_agg)
    weather_years = sorted(unscaled["weather_year"].unique())
    targets_full = (
        fes_targets.assign(_key=1)
        .merge(pd.DataFrame({"weather_year": weather_years, "_key": 1}), on="_key")
        .drop(columns="_key")
    )
    multipliers = _compute_multipliers(annual_twh, targets_full)

    # Carry-through rows for non-anchored techs (coal, imports): multiplier = 1.
    non_anchored = [t for t, s in TECH_ANCHORING.items() if not s["is_anchored_to_fes"]]
    years = sorted(unscaled["year"].unique())
    carry_rows: list[dict] = []
    for tech in non_anchored:
        spec = TECH_ANCHORING[tech]
        ann = annual_twh[annual_twh["tech"] == tech]
        for wy in weather_years:
            for yr in years:
                for sc in FES_SCENARIOS:
                    a = ann[(ann["weather_year"] == wy) & (ann["year"] == yr)]
                    unscaled_twh = float(a["unscaled_annual_twh"].iloc[0]) if len(a) else 0.0
                    carry_rows.append(
                        {
                            "year": yr,
                            "weather_year": wy,
                            "fes_scenario": sc,
                            "tech": tech,
                            "model_role": spec["model_role"],
                            "value_twh": np.nan,
                            "unscaled_annual_twh": unscaled_twh,
                            "multiplier": 1.0,
                        }
                    )
    if carry_rows:
        multipliers = pd.concat([multipliers, pd.DataFrame(carry_rows)], ignore_index=True)
    multipliers["is_anchored_to_fes"] = multipliers["tech"].map(
        lambda t: TECH_ANCHORING[t]["is_anchored_to_fes"]
    )

    # Apply multipliers onto the hourly long table.
    long_anchored = unscaled.merge(
        multipliers[
            [
                "year",
                "weather_year",
                "fes_scenario",
                "tech",
                "model_role",
                "is_anchored_to_fes",
                "multiplier",
            ]
        ],
        on=["year", "weather_year", "tech"],
        how="left",
    )
    if long_anchored["multiplier"].isna().any():
        raise ValueError("NaN multiplier after join; some tech/year is missing a row.")
    long_anchored["anchored_mw"] = (
        long_anchored["unscaled_generation_mw"] * long_anchored["multiplier"]
    )
    long_anchored = long_anchored[
        [
            "timestamp_utc",
            "year",
            "fes_scenario",
            "weather_year",
            "weather_year_role",
            "tech",
            "model_role",
            "is_anchored_to_fes",
            "anchored_mw",
        ]
    ].sort_values(
        ["fes_scenario", "weather_year", "timestamp_utc", "tech"], kind="mergesort"
    ).reset_index(drop=True)
    return long_anchored, multipliers


# ----------------------------------------------------------------------
# Step 2 -- pivot to wide schema
# ----------------------------------------------------------------------


def _pivot_to_wide(long_anchored: pd.DataFrame) -> pd.DataFrame:
    missing_map = set(long_anchored["tech"].unique()) - set(TECH_TO_WIDE_COLUMN)
    if missing_map:
        raise ValueError(
            f"Unknown tech labels (add to TECH_TO_WIDE_COLUMN): {sorted(missing_map)}"
        )
    df = long_anchored.copy()
    df["wide_column"] = df["tech"].map(TECH_TO_WIDE_COLUMN)
    wide = df.pivot_table(
        index=["timestamp_utc", "year", "fes_scenario", "weather_year", "weather_year_role"],
        columns="wide_column",
        values="anchored_mw",
        aggfunc="first",
        observed=True,
    ).reset_index()
    wide.columns.name = None
    id_cols = ["timestamp_utc", "year", "fes_scenario", "weather_year", "weather_year_role"]
    tech_cols = sorted([c for c in wide.columns if c.endswith("_mw")])
    wide = wide[id_cols + tech_cols]
    wide["year"] = wide["timestamp_utc"].dt.year.astype(np.int32)
    return wide


# ----------------------------------------------------------------------
# Step 3 -- integration QA gates
# ----------------------------------------------------------------------


def _hour_count_qa(wide: pd.DataFrame) -> tuple[bool, list[str]]:
    lines: list[str] = [
        "QA Check 1 -- Hour counts (per year x scenario x weather_year, UTC)",
        "-" * 78,
    ]
    ok = True
    for year in sorted(wide["year"].unique()):
        expected = _expected_hours(int(year))
        for scenario in sorted(wide["fes_scenario"].unique()):
            for wy in sorted(wide["weather_year"].unique()):
                n = int(
                    wide.loc[
                        (wide["year"] == year)
                        & (wide["fes_scenario"] == scenario)
                        & (wide["weather_year"] == wy),
                        "timestamp_utc",
                    ].shape[0]
                )
                leap = calendar.isleap(int(year))
                status = "PASS" if n == expected else "FAIL"
                if n != expected:
                    ok = False
                lines.append(
                    f"  {year}  {scenario[:18]:<18}  wy={wy}  rows={n:5d}  "
                    f"expected={expected:4d}  leap={str(leap):5s}  [{status}]"
                )
    lines.append("")
    return ok, lines


def _no_dup_keys_qa(wide: pd.DataFrame) -> tuple[bool, list[str]]:
    n_dup = int(
        wide.duplicated(subset=["timestamp_utc", "fes_scenario", "weather_year"]).sum()
    )
    status = "PASS" if n_dup == 0 else "FAIL"
    return n_dup == 0, [
        "QA Check 2 -- No duplicate (timestamp_utc, fes_scenario, weather_year) keys",
        "-" * 78,
        f"  duplicates = {n_dup}  [{status}]",
        "",
    ]


def _physical_sign_qa(wide: pd.DataFrame) -> tuple[bool, list[str]]:
    lines: list[str] = ["QA Check 3 -- Physical / sign convention checks", "-" * 78]
    ok = True
    must_be_nonneg = [
        "wind_mw",
        "solar_mw",
        "nuclear_existing_mw",
        "biomass_mw",
        "hydro_mw",
        "other_mw",
        "gas_reference_mw",
        "coal_reference_mw",
    ]
    for col in must_be_nonneg:
        if col not in wide.columns:
            continue
        n_neg = int((wide[col] < 0).sum())
        status = "PASS" if n_neg == 0 else "FAIL"
        if n_neg > 0:
            ok = False
        lines.append(f"  {col:<26}  negative_hours={n_neg:8d}  [{status}]")
    if "imports_net_baseline_mw" in wide.columns:
        n_neg = int((wide["imports_net_baseline_mw"] < 0).sum())
        lines.append(
            f"  {'imports_net_baseline_mw':<26}  negative_hours={n_neg:8d}  "
            f"[INFO -- exports allowed by sign convention]"
        )
    if "storage_net_mw" in wide.columns:
        n_neg = int((wide["storage_net_mw"] < 0).sum())
        lines.append(
            f"  {'storage_net_mw':<26}  negative_hours={n_neg:8d}  "
            f"[INFO -- charging allowed by sign convention]"
        )
    lines.append("")
    return ok, lines


def _aggregate_fes_by_wide_column(fes: pd.DataFrame) -> pd.DataFrame:
    agg = (
        fes.groupby(["year", "fes_scenario", "tech"], as_index=False)["value"]
        .sum()
        .rename(columns={"value": "fes_twh"})
    )
    rows: list[pd.DataFrame] = []
    for col, fes_techs in WIDE_COLUMN_TO_FES_TECHS.items():
        sub = agg[agg["tech"].isin(fes_techs)]
        g = sub.groupby(["year", "fes_scenario"], as_index=False)["fes_twh"].sum()
        g["wide_column"] = col
        rows.append(g)
    return pd.concat(rows, ignore_index=True)


def _fes_reconciliation_qa(
    wide: pd.DataFrame, fes: pd.DataFrame
) -> tuple[bool, list[str], pd.DataFrame]:
    lines: list[str] = ["QA Check 4 -- Annual energy vs FES (TWh)", "-" * 78]
    ok = True
    tech_cols = [c for c in wide.columns if c.endswith("_mw")]
    annual = (
        wide.groupby(["year", "fes_scenario", "weather_year"], as_index=False)[tech_cols]
        .sum()
        .melt(
            id_vars=["year", "fes_scenario", "weather_year"],
            var_name="wide_column",
            value_name="sum_mwh",
        )
    )
    annual["model_twh"] = annual["sum_mwh"] / 1_000_000.0

    fes_targets = _aggregate_fes_by_wide_column(fes)
    merged = annual.merge(
        fes_targets, on=["year", "fes_scenario", "wide_column"], how="left"
    )
    merged["abs_diff_twh"] = (merged["model_twh"] - merged["fes_twh"]).abs()

    sample_years = sorted(set([int(merged["year"].min()), int(merged["year"].max())]))
    for year in sample_years:
        for scenario in sorted(merged["fes_scenario"].unique()):
            grp = merged[(merged["year"] == year) & (merged["fes_scenario"] == scenario)]
            lines.append(f"  Year {year} -- {scenario}")
            for col in sorted(grp["wide_column"].unique()):
                sub = grp[grp["wide_column"] == col]
                if col in WIDE_COLUMN_TO_FES_TECHS:
                    for _, row in sub.iterrows():
                        m = float(row["model_twh"])
                        f = float(row["fes_twh"]) if pd.notna(row["fes_twh"]) else 0.0
                        close = np.isclose(m, f, rtol=RTOL_TWH, atol=ATOL_TWH)
                        status = "PASS" if close else "FAIL"
                        if not close:
                            ok = False
                        lines.append(
                            f"    {col:<26}  wy={int(row['weather_year'])}  "
                            f"model={m:11.6f}  fes={f:11.6f}  [{status}]"
                        )
                elif col in NON_ANCHORED_DIAGNOSTIC_COLS:
                    for _, row in sub.iterrows():
                        m = float(row["model_twh"])
                        lines.append(
                            f"    {col:<26}  wy={int(row['weather_year'])}  "
                            f"model={m:11.6f}                  [INFO -- not anchored]"
                        )
    lines.append("")
    fes_csv = merged[
        ["year", "fes_scenario", "weather_year", "wide_column", "model_twh", "fes_twh", "abs_diff_twh"]
    ]
    return ok, lines, fes_csv


# ----------------------------------------------------------------------
# Step 4 -- DUKES load-factor envelope check
# ----------------------------------------------------------------------


def _dukes_loadfactor_envelope() -> pd.DataFrame:
    lf = pd.read_parquet(DUKES_LF_PATH)
    rows: list[dict] = []
    for col, dukes_label in DUKES_LF_LABELS.items():
        sub = lf[lf["tech"].astype(str).str.strip().str.lower() == dukes_label.strip().lower()]
        if sub.empty:
            continue
        rows.append(
            {
                "wide_column": col,
                "dukes_lf_label": dukes_label,
                "lf_min_pct": float(sub["load_factor_pct"].min()),
                "lf_median_pct": float(sub["load_factor_pct"].median()),
                "lf_max_pct": float(sub["load_factor_pct"].max()),
                "n_years": int(sub["year"].nunique()),
            }
        )
    return pd.DataFrame.from_records(rows)


def _capacity_for_lf_check() -> pd.DataFrame:
    cal = pd.read_csv(CALIBRATION_PATH)
    grouped = cal.groupby("internal_name")["capacity_mw"]
    rows: list[dict] = []
    for internal, wide_col in (("wind_total_mw", "wind_mw"), ("solar_mw", "solar_mw")):
        rows.append(
            {
                "wide_column": wide_col,
                "scope": "unscaled_shape",
                "ref_capacity_mw": float(grouped.get_group(internal).median()),
            }
        )
    latest = (
        cal.sort_values(["internal_name", "year"])
        .groupby("internal_name")
        .tail(1)
        .set_index("internal_name")["capacity_mw"]
    )
    for internal, wide_col in (
        ("nuclear_mw", "nuclear_existing_mw"),
        ("hydro_mw", "hydro_mw"),
    ):
        rows.append(
            {
                "wide_column": wide_col,
                "scope": "anchored_with_historical_capacity",
                "ref_capacity_mw": float(latest.loc[internal]),
            }
        )
    return pd.DataFrame.from_records(rows)


def _dukes_lf_constraint_check(wide: pd.DataFrame) -> pd.DataFrame:
    envelope = _dukes_loadfactor_envelope()
    cap = _capacity_for_lf_check()

    bundle = joblib.load(MODELS_PATH)
    wind_model = bundle["wind"]
    solar_model = bundle["solar"]

    era5 = pd.read_parquet(ERA5_HIST_PATH)
    era5["timestamp_utc"] = _ensure_utc(era5["timestamp_utc"])
    era5["year"] = era5["timestamp_utc"].dt.year.astype(int)

    rows: list[dict] = []

    # STRICT: ML predicted CF (per representative weather year) vs DUKES envelope.
    for weather_year in sorted(wide["weather_year"].unique()):
        wy_data = era5[era5["year"] == weather_year].copy()
        if wy_data.empty:
            continue
        pred_w = np.asarray(
            wind_model.predict(wy_data[["wind_speed_100m_ms"]]), dtype=np.float64
        )
        pred_s = np.asarray(
            solar_model.predict(wy_data[["ssrd_j_m2"]]), dtype=np.float64
        )
        for wide_col, mean_cf in (
            ("wind_mw", float(pred_w.mean())),
            ("solar_mw", float(pred_s.mean())),
        ):
            env_match = envelope[envelope["wide_column"] == wide_col]
            if env_match.empty:
                continue
            env = env_match.iloc[0]
            implied_lf_pct = mean_cf * 100.0
            within = (
                (env["lf_min_pct"] - LF_TOLERANCE_PP)
                <= implied_lf_pct
                <= (env["lf_max_pct"] + LF_TOLERANCE_PP)
            )
            rows.append(
                {
                    "wide_column": wide_col,
                    "scope": "ml_predicted_cf_vs_dukes_envelope",
                    "fes_scenario": "n/a",
                    "weather_year": int(weather_year),
                    "year": int(weather_year),
                    "ref_capacity_mw": np.nan,
                    "annual_twh": np.nan,
                    "implied_lf_pct": implied_lf_pct,
                    "dukes_lf_min_pct": float(env["lf_min_pct"]),
                    "dukes_lf_max_pct": float(env["lf_max_pct"]),
                    "within_envelope": bool(within),
                    "check_type": "STRICT",
                }
            )

    # INFO: anchored techs (nuclear, hydro, biomass) vs latest DUKES capacity.
    for wide_col in ("nuclear_existing_mw", "hydro_mw", "biomass_mw"):
        if wide_col not in wide.columns:
            continue
        env_match = envelope[envelope["wide_column"] == wide_col]
        if env_match.empty:
            continue
        env = env_match.iloc[0]
        cap_match = cap[
            (cap["wide_column"] == wide_col)
            & (cap["scope"] == "anchored_with_historical_capacity")
        ]
        if cap_match.empty:
            continue
        ref_cap = float(cap_match["ref_capacity_mw"].iloc[0])
        ann = (
            wide.groupby(["fes_scenario", "weather_year", "year"], as_index=False)[wide_col]
            .sum()
            .rename(columns={wide_col: "annual_mwh"})
        )
        ann["hours"] = ann["year"].apply(_expected_hours)
        ann["implied_lf_pct"] = ann["annual_mwh"] / (ref_cap * ann["hours"]) * 100.0
        for _, r in ann.iterrows():
            within = (
                (env["lf_min_pct"] - LF_TOLERANCE_PP)
                <= r["implied_lf_pct"]
                <= (env["lf_max_pct"] + LF_TOLERANCE_PP)
            )
            rows.append(
                {
                    "wide_column": wide_col,
                    "scope": "anchored_with_historical_capacity",
                    "fes_scenario": str(r["fes_scenario"]),
                    "weather_year": int(r["weather_year"]),
                    "year": int(r["year"]),
                    "ref_capacity_mw": ref_cap,
                    "annual_twh": float(r["annual_mwh"]) / 1_000_000.0,
                    "implied_lf_pct": float(r["implied_lf_pct"]),
                    "dukes_lf_min_pct": float(env["lf_min_pct"]),
                    "dukes_lf_max_pct": float(env["lf_max_pct"]),
                    "within_envelope": bool(within),
                    "check_type": "INFO",
                }
            )

    return pd.DataFrame.from_records(rows)


# ----------------------------------------------------------------------
# Step 5 -- validation plots
# ----------------------------------------------------------------------


def _plot_era5_fit() -> None:
    bundle = joblib.load(MODELS_PATH)
    wind_model = bundle["wind"]
    solar_model = bundle["solar"]
    training = pd.read_parquet(TRAINING_PATH)

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))

    pred_w = np.asarray(wind_model.predict(training[["wind_speed_100m_ms"]]), dtype=np.float64)
    axes[0].scatter(training["wind_cf"], pred_w, s=2, alpha=0.04, color="#1f77b4")
    axes[0].plot([0, 1], [0, 1], "k--", lw=1)
    axes[0].set_xlabel("Observed wind CF (DUKES + genmix)")
    axes[0].set_ylabel("Predicted wind CF (ERA5 model)")
    axes[0].set_title("Wind: ERA5-linked model fit")
    axes[0].set_xlim(0, 1)
    axes[0].set_ylim(0, 1)
    axes[0].grid(True, alpha=0.3)

    pred_s = np.asarray(solar_model.predict(training[["ssrd_j_m2"]]), dtype=np.float64)
    axes[1].scatter(training["solar_cf"], pred_s, s=2, alpha=0.04, color="#ff7f0e")
    axes[1].plot([0, 1], [0, 1], "k--", lw=1)
    axes[1].set_xlabel("Observed solar CF (DUKES + genmix)")
    axes[1].set_ylabel("Predicted solar CF (ERA5 model)")
    axes[1].set_title("Solar: ERA5-linked model fit")
    axes[1].set_xlim(0, 1)
    axes[1].set_ylim(0, 1)
    axes[1].grid(True, alpha=0.3)

    fig.suptitle("Historic vs ERA5-linked CF fit (training data)")
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "qa_era5_fit.png", dpi=140, bbox_inches="tight")
    plt.close(fig)


def _plot_annual_pathway(wide: pd.DataFrame) -> None:
    fes = pd.read_parquet(FES_SUPPLY_PATH)
    fes_tot = fes.groupby(["year", "fes_scenario", "tech"], as_index=False)["value"].sum()

    techs = list(WIDE_COLUMN_TO_FES_TECHS.keys())
    n = len(techs)
    cols = 4
    rows = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(4.2 * cols, 3.0 * rows), sharex=True)
    axes = np.array(axes).reshape(-1)

    for i, col in enumerate(techs):
        ax = axes[i]
        if col not in wide.columns:
            ax.set_visible(False)
            continue
        m = (
            wide.groupby(["year", "fes_scenario", "weather_year"], as_index=False)[col]
            .sum()
            .assign(model_twh=lambda d: d[col] / 1_000_000.0)
        )
        for scen, color in (
            ("Holistic Transition", "#1f77b4"),
            ("Electric Engagement", "#d62728"),
        ):
            sub = m[m["fes_scenario"] == scen]
            band = sub.groupby("year")["model_twh"].agg(["min", "max", "mean"])
            ax.fill_between(band.index, band["min"], band["max"], alpha=0.20, color=color)
            ax.plot(band.index, band["mean"], color=color, lw=1.4, label=f"{scen} (model)")

        f = (
            fes_tot[fes_tot["tech"].isin(WIDE_COLUMN_TO_FES_TECHS[col])]
            .groupby(["year", "fes_scenario"], as_index=False)["value"]
            .sum()
        )
        for scen, color, marker in (
            ("Holistic Transition", "#1f77b4", "o"),
            ("Electric Engagement", "#d62728", "s"),
        ):
            sub = f[f["fes_scenario"] == scen]
            ax.plot(
                sub["year"],
                sub["value"],
                color=color,
                marker=marker,
                markersize=3,
                lw=0.0,
                label=f"{scen} (FES)",
            )
        ax.set_title(col)
        ax.set_ylabel("TWh")
        ax.grid(True, alpha=0.3)
        if i == 0:
            ax.legend(fontsize=7, loc="best")
    for j in range(len(techs), len(axes)):
        axes[j].set_visible(False)

    fig.suptitle("Annual pathway by technology -- model band (3 weather years) vs FES")
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "qa_annual_pathway.png", dpi=140, bbox_inches="tight")
    plt.close(fig)


def _plot_diurnal_2030(wide: pd.DataFrame) -> None:
    sub = wide[wide["year"] == 2030].copy()
    sub["hour"] = sub["timestamp_utc"].dt.hour
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))
    for col, ax in zip(("wind_mw", "solar_mw"), axes):
        if col not in sub.columns:
            continue
        for wy in sorted(sub["weather_year"].unique()):
            d = sub[(sub["weather_year"] == wy)]
            mean_diurnal = d.groupby("hour")[col].mean()
            ax.plot(mean_diurnal.index, mean_diurnal.values, label=f"weather_year={wy}", lw=1.5)
        ax.set_title(f"Mean diurnal {col} (2030, both scenarios)")
        ax.set_xlabel("Hour (UTC)")
        ax.set_ylabel("MW")
        ax.grid(True, alpha=0.3)
        ax.legend()
    fig.suptitle("Mean diurnal shape, 2030 -- by representative weather year")
    fig.tight_layout()
    fig.savefig(PLOTS_DIR / "qa_diurnal_2030.png", dpi=140, bbox_inches="tight")
    plt.close(fig)


# ----------------------------------------------------------------------
# Step 6 -- QA notes markdown
# ----------------------------------------------------------------------


def _calendar_notes(wide: pd.DataFrame) -> dict:
    notes: list[str] = []
    n_dup = int(
        wide.duplicated(subset=["timestamp_utc", "fes_scenario", "weather_year"]).sum()
    )
    notes.append(f"Duplicate (timestamp_utc, fes_scenario, weather_year) rows: {n_dup}")

    miss: list[tuple] = []
    for year in sorted(wide["year"].unique()):
        expected = _expected_hours(int(year))
        for scen in sorted(wide["fes_scenario"].unique()):
            for wy in sorted(wide["weather_year"].unique()):
                n = int(
                    (
                        (wide["year"] == year)
                        & (wide["fes_scenario"] == scen)
                        & (wide["weather_year"] == wy)
                    ).sum()
                )
                if n != expected:
                    miss.append((year, scen, wy, n, expected))
    notes.append(f"Year-scenario-weather_year groups with bad row count: {len(miss)}")
    if miss:
        notes.append(f"  first bad: {miss[0]}")

    ts = wide["timestamp_utc"]
    tz_ok = ts.dt.tz is not None and str(ts.dt.tz) == "UTC"
    notes.append(f"All timestamps UTC: {tz_ok}")

    nan_per_col = wide.isna().sum()
    nan_cols = nan_per_col[nan_per_col > 0]
    notes.append(f"Columns with NaN: {dict(nan_cols)}")
    return {"notes": notes, "n_dup": n_dup, "n_bad_groups": len(miss)}


def _write_qa_notes(cal_qa: dict, lf_df: pd.DataFrame, fes_df: pd.DataFrame) -> None:
    strict = lf_df[lf_df.get("check_type", "STRICT") == "STRICT"] if len(lf_df) else lf_df
    info = lf_df[lf_df.get("check_type", "INFO") == "INFO"] if len(lf_df) else lf_df
    n_strict = len(strict)
    n_strict_fail = int((~strict["within_envelope"]).sum()) if n_strict else 0
    n_info_outside = int((~info["within_envelope"]).sum()) if len(info) else 0
    fes_anchored = fes_df.dropna(subset=["fes_twh"])
    fes_max_diff = float(fes_anchored["abs_diff_twh"].max()) if len(fes_anchored) else 0.0

    txt = f"""# Objective 1 -- QA notes

Auto-generated by ``src/task5_fes_anchoring_and_export.py``. Companion CSVs:
``qa_dukes_loadfactor_check.csv`` and ``qa_fes_reconciliation.csv``. Plots:
``outputs/objective1_generation/qa_plots/``.

## Calendar QA

{chr(10).join('- ' + n for n in cal_qa['notes'])}

## DUKES load-factor envelope check

### STRICT -- renewables (ML predicted CF vs DUKES envelope)

- Constraint: ML model annual-mean predicted CF (per representative weather
  year) must lie within ``[DUKES_min - {LF_TOLERANCE_PP}pp,
  DUKES_max + {LF_TOLERANCE_PP}pp]`` from DUKES 6.3.
- Total strict checks: {n_strict}
- Out-of-envelope strict checks: **{n_strict_fail}**

### INFO -- anchored dispatchables (latest DUKES capacity reference)

- Reports implied LF (anchored MW / latest DUKES capacity / hours). FES
  capacity is unknown in this workflow, so out-of-envelope rows generally
  signal projected fleet expansion under FES, not a model error.
- Out-of-envelope info checks: {n_info_outside} (informational only).
- Detailed table: ``qa_dukes_loadfactor_check.csv``.

## FES reconciliation

- Per (year, scenario, weather_year, anchored column) absolute |model - FES| TWh.
- Maximum |diff| across all anchored rows: **{fes_max_diff:.6e} TWh**
  (anchored columns should match to numerical tolerance; non-anchored
  diagnostic columns are excluded from this check).
- Detailed table: ``qa_fes_reconciliation.csv``.

## Units, sign convention, timezone

- Power columns are **MW** (instantaneous hourly mean). Annual energy
  derived by hourly sum is **MWh**; divide by 1e6 for **TWh**.
- ``timestamp_utc`` is **timezone-aware UTC** in every output parquet.
- Sign convention:
  - ``storage_net_mw``: + = generation/discharge to grid, - = pumping/charge.
  - ``imports_net_baseline_mw``: + = import to GB, - = export from GB.
- Leap years (2032, 2036, 2040, 2044) carry **8784** rows per scenario per
  weather year; non-leap years carry **8760**.

## Validation plots

- ``qa_plots/qa_era5_fit.png`` -- historic vs ERA5-linked CF fit (training data).
- ``qa_plots/qa_annual_pathway.png`` -- annual TWh by technology, model band
  (3 weather years) overlaid on FES targets per scenario.
- ``qa_plots/qa_diurnal_2030.png`` -- mean diurnal wind / solar MW for 2030
  by weather year.
"""
    QA_NOTES_MD.write_text(txt, encoding="utf-8")


# ----------------------------------------------------------------------
# Driver
# ----------------------------------------------------------------------


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    # 1. FES anchoring.
    unscaled = pd.read_parquet(UNSCALED_HOURLY_PATH)
    unscaled["timestamp_utc"] = _ensure_utc(unscaled["timestamp_utc"])
    unscaled["year"] = unscaled["timestamp_utc"].dt.year.astype(np.int32)

    fes = pd.read_parquet(FES_SUPPLY_PATH)
    long_anchored, multipliers = _anchor_to_fes(unscaled, fes)
    long_anchored.to_parquet(ANCHORED_LONG_PATH, index=False)
    multipliers.to_csv(MULTIPLIERS_AUDIT_CSV, index=False)

    # 2. Pivot to wide.
    wide = _pivot_to_wide(long_anchored)
    wide.to_parquet(WIDE_EXPORT_PATH, index=False)
    wide.to_parquet(LEGACY_WIDE_EXPORT_PATH, index=False)

    # 3. Integration QA.
    qa1_ok, qa1_lines = _hour_count_qa(wide)
    qa2_ok, qa2_lines = _no_dup_keys_qa(wide)
    qa3_ok, qa3_lines = _physical_sign_qa(wide)
    qa4_ok, qa4_lines, fes_recon_csv = _fes_reconciliation_qa(wide, fes)

    # 4. DUKES load-factor envelope.
    lf_df = _dukes_lf_constraint_check(wide)
    lf_df.to_csv(QA_LF_CSV, index=False)
    fes_recon_csv.to_csv(QA_FES_CSV, index=False)

    # 5. Validation plots.
    _plot_era5_fit()
    _plot_annual_pathway(wide)
    _plot_diurnal_2030(wide)

    # 6. QA notes.
    cal_qa = _calendar_notes(wide)
    _write_qa_notes(cal_qa, lf_df, fes_recon_csv)

    # Print summary banner.
    print("\n" + "=" * 78)
    print(" Task 5 -- FES anchoring + final wide export + integration QA")
    print("=" * 78)
    print(f"\nSaved: {WIDE_EXPORT_PATH}")
    print(f"Saved: {ANCHORED_LONG_PATH}")
    print(f"Saved: {MULTIPLIERS_AUDIT_CSV}")
    print(f"Saved: {QA_LF_CSV}")
    print(f"Saved: {QA_FES_CSV}")
    print(f"Saved: {QA_NOTES_MD}\n")
    print(f"Schema columns: {list(wide.columns)}")
    print(f"Rows: {len(wide):,}")
    print(f"Date range: {wide['timestamp_utc'].min()} -> {wide['timestamp_utc'].max()}")
    print(f"Scenarios: {sorted(wide['fes_scenario'].unique())}")
    print(f"Weather years: {sorted(wide['weather_year'].unique())}\n")

    for line in qa1_lines + qa2_lines + qa3_lines + qa4_lines:
        print(line)

    strict = lf_df[lf_df["check_type"] == "STRICT"] if "check_type" in lf_df.columns else lf_df
    info = (
        lf_df[lf_df["check_type"] == "INFO"]
        if "check_type" in lf_df.columns
        else lf_df.iloc[0:0]
    )
    print(
        f"DUKES LF envelope (STRICT): "
        f"{(~strict['within_envelope']).sum()}/{len(strict)} renewable CF checks "
        f"outside [DUKES_min-{LF_TOLERANCE_PP}pp, DUKES_max+{LF_TOLERANCE_PP}pp]."
    )
    print(
        f"DUKES LF envelope (INFO): "
        f"{(~info['within_envelope']).sum()}/{len(info)} anchored-tech rows outside envelope "
        f"(expected -- FES projects fleet expansion not in DUKES)."
    )

    all_ok = qa1_ok and qa2_ok and qa3_ok and qa4_ok
    print("=" * 78)
    print("INTEGRATION CHECK PASS" if all_ok else "INTEGRATION CHECK FAIL -- review QA above")
    print("=" * 78 + "\n")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
