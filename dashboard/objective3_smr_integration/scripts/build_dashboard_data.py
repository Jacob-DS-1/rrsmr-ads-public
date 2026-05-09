"""Build compact dashboard-ready data from repo-generated Objective 3 outputs.

Run from the repository root after the complete model workflow has generated
Objective 3 outputs.

Generated files are written under outputs/dashboard/objective3_smr_integration/data
and should not be committed.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


EXPECTED_WEATHER_ROLES = ["average_wind", "high_wind", "low_wind"]
EXPECTED_SMR_CASES = ["simultaneous_commissioning", "staggered_commissioning"]

HOURLY_COLUMNS = [
    "timestamp_utc",
    "year",
    "fes_scenario",
    "climate_member",
    "weather_year_role",
    "smr_case",
    "demand_mw",
    "wind_mw",
    "exogenous_supply_mw",
    "imports_net_baseline_mw",
    "smr_total_delivered_mw",
    "unit1_delivered_mw",
    "unit2_delivered_mw",
    "unit3_delivered_mw",
    "residual_before_smr_mw",
    "residual_after_smr_mw",
    "gas_needed_before_mw",
    "gas_needed_after_mw",
    "gas_displacement_proxy_mw",
    "surplus_after_smr_mw",
    "residual_reduction_mw",
    "low_wind_flag",
    "low_wind_support_flag",
]

SCENARIO_KEYS = [
    "timestamp_utc",
    "year",
    "fes_scenario",
    "climate_member",
    "weather_year_role",
    "smr_case",
]

UNIT_COLUMNS = [
    "timestamp_utc",
    "year",
    "fes_scenario",
    "climate_member",
    "weather_year_role",
    "smr_case",
    "unit1_delivered_mw",
    "unit2_delivered_mw",
    "unit3_delivered_mw",
]


def repo_root_from_script() -> Path:
    return Path(__file__).resolve().parents[3]


def default_output_dir(repo_root: Path) -> Path:
    return repo_root / "outputs" / "dashboard" / "objective3_smr_integration" / "data"


def read_parquet(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required parquet input: {path}")
    return pd.read_parquet(path, columns=columns)


def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing required CSV input: {path}")
    return pd.read_csv(path)


def assert_values(df: pd.DataFrame, column: str, expected: list[str], label: str) -> None:
    actual = sorted(str(value) for value in df[column].dropna().unique())
    if actual != sorted(expected):
        raise ValueError(f"{label}: expected {column} values {sorted(expected)}, found {actual}")


def downcast_for_dashboard(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for column in out.select_dtypes(include=["float64"]).columns:
        out[column] = out[column].astype("float32")

    for column in ["fes_scenario", "climate_member", "weather_year_role", "smr_case"]:
        if column in out.columns:
            out[column] = out[column].astype("category")

    if "year" in out.columns:
        out["year"] = out["year"].astype("int16")

    for column in ["low_wind_flag", "low_wind_support_flag"]:
        if column in out.columns:
            out[column] = out[column].astype("bool")

    return out


def build_hourly(repo_root: Path, output_dir: Path) -> pd.DataFrame:
    output_dir.mkdir(parents=True, exist_ok=True)
    hourly_path = repo_root / "data" / "processed" / "objective3_smr_integration" / "system_impact_hourly_2030_2045"
    grid_path = repo_root / "data" / "processed" / "objective3_smr_integration" / "grid_master_hourly_2030_2045"

    hourly = read_parquet(hourly_path)
    grid_units = read_parquet(grid_path, columns=UNIT_COLUMNS)

    hourly = hourly.merge(grid_units, on=SCENARIO_KEYS, how="left", validate="one_to_one")

    missing_units = int(hourly[["unit1_delivered_mw", "unit2_delivered_mw", "unit3_delivered_mw"]].isna().sum().sum())
    if missing_units:
        raise ValueError(f"Unit-level SMR columns have {missing_units} missing values after merge.")

    assert_values(hourly, "weather_year_role", EXPECTED_WEATHER_ROLES, "hourly dashboard data")
    assert_values(hourly, "smr_case", EXPECTED_SMR_CASES, "hourly dashboard data")

    duplicate_count = int(hourly.duplicated(SCENARIO_KEYS).sum())
    if duplicate_count:
        raise ValueError(f"Hourly dashboard data has {duplicate_count} duplicate scenario-hour keys.")

    selected_columns = [column for column in HOURLY_COLUMNS if column in hourly.columns]
    hourly = downcast_for_dashboard(hourly[selected_columns])

    output_path = output_dir / "hourly_metrics_dashboard.parquet"
    hourly.to_parquet(output_path, index=False, engine="pyarrow", compression="zstd")
    print(f"Wrote {output_path} ({len(hourly):,} rows)")
    return hourly


def build_annual(repo_root: Path, output_dir: Path) -> pd.DataFrame:
    output_dir.mkdir(parents=True, exist_ok=True)
    source = repo_root / "data" / "processed" / "objective3_smr_integration" / "system_impact_summary_annual_2030_2045.csv"
    annual = read_csv(source).rename(
        columns={
            "annual_smr_delivered_energy_twh": "annual_smr_energy_twh",
        }
    )

    assert_values(annual, "weather_year_role", EXPECTED_WEATHER_ROLES, "annual dashboard data")
    assert_values(annual, "smr_case", EXPECTED_SMR_CASES, "annual dashboard data")

    output_path = output_dir / "annual_summary.csv"
    annual.to_csv(output_path, index=False)
    print(f"Wrote {output_path} ({len(annual):,} rows)")
    return annual


def build_period(repo_root: Path, output_dir: Path) -> pd.DataFrame:
    output_dir.mkdir(parents=True, exist_ok=True)
    source = repo_root / "data" / "processed" / "objective3_smr_integration" / "system_impact_summary_period_2030_2045.csv"
    period = read_csv(source).rename(
        columns={
            "cumulative_smr_delivered_energy_twh": "cumulative_smr_energy_twh",
        }
    )

    assert_values(period, "weather_year_role", EXPECTED_WEATHER_ROLES, "period dashboard data")
    assert_values(period, "smr_case", EXPECTED_SMR_CASES, "period dashboard data")

    output_path = output_dir / "period_summary.csv"
    period.to_csv(output_path, index=False)
    print(f"Wrote {output_path} ({len(period):,} rows)")
    return period


def build_low_wind_rankings(hourly: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    output_dir.mkdir(parents=True, exist_ok=True)
    low_wind = hourly.loc[hourly["weather_year_role"].astype(str).eq("low_wind")].copy()
    low_wind["date"] = pd.to_datetime(low_wind["timestamp_utc"], utc=True).dt.date

    daily = (
        low_wind.groupby(["date", "fes_scenario", "climate_member", "smr_case"], observed=True)
        .agg(gas_displacement_mwh=("gas_displacement_proxy_mw", "sum"))
        .reset_index()
    )

    pivot = daily.pivot_table(
        index=["date", "fes_scenario", "climate_member"],
        columns="smr_case",
        values="gas_displacement_mwh",
        aggfunc="first",
        observed=True,
    ).reset_index()

    for case in EXPECTED_SMR_CASES:
        if case not in pivot.columns:
            pivot[case] = 0.0

    pivot["simultaneous_minus_staggered_mwh"] = (
        pivot["simultaneous_commissioning"] - pivot["staggered_commissioning"]
    )
    pivot = pivot.sort_values("simultaneous_minus_staggered_mwh", ascending=False)

    output_path = output_dir / "low_wind_case_study_selection_rankings.csv"
    pivot.to_csv(output_path, index=False)
    print(f"Wrote {output_path} ({len(pivot):,} rows)")
    return pivot


def build_low_wind_case_day(hourly: pd.DataFrame, rankings: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    output_dir.mkdir(parents=True, exist_ok=True)
    if rankings.empty:
        raise ValueError("Cannot build low-wind case day because rankings are empty.")

    selected = rankings.iloc[0]
    hourly = hourly.copy()
    hourly["date"] = pd.to_datetime(hourly["timestamp_utc"], utc=True).dt.date

    case_day = hourly.loc[
        hourly["date"].eq(selected["date"])
        & hourly["fes_scenario"].astype(str).eq(str(selected["fes_scenario"]))
        & hourly["climate_member"].astype(str).eq(str(selected["climate_member"]))
        & hourly["weather_year_role"].astype(str).eq("low_wind")
    ].copy()

    if case_day.empty:
        raise ValueError("Selected low-wind case day has no matching hourly rows.")

    output_path = output_dir / "low_wind_case_study_pressure_day.csv"
    case_day.drop(columns=["date"], errors="ignore").to_csv(output_path, index=False)
    print(f"Wrote {output_path} ({len(case_day):,} rows)")
    return case_day


def build_qa_checks(hourly: pd.DataFrame, annual: pd.DataFrame, period: pd.DataFrame, output_dir: Path) -> pd.DataFrame:
    output_dir.mkdir(parents=True, exist_ok=True)
    checks = []

    def add_check(check_name: str, status: str, expected: str, observed: str, notes: str) -> None:
        checks.append(
            {
                "check_name": check_name,
                "status": status,
                "expected": expected,
                "observed": observed,
                "notes": notes,
            }
        )

    hourly_key_duplicates = int(hourly.duplicated(SCENARIO_KEYS).sum())
    weather_roles = sorted(str(value) for value in annual["weather_year_role"].dropna().unique())
    smr_cases = sorted(str(value) for value in annual["smr_case"].dropna().unique())

    add_check(
        "dashboard_hourly_row_count",
        "pass" if len(hourly) == 5_049_216 else "fail",
        "5,049,216 rows",
        f"{len(hourly):,} rows",
        "Expected count equals timestamps times FES pathways times climate members times weather roles times SMR cases.",
    )
    add_check(
        "duplicate_scenario_hour_keys",
        "pass" if hourly_key_duplicates == 0 else "fail",
        "0 duplicate scenario-hour keys",
        str(hourly_key_duplicates),
        "Each scenario-hour combination appears once.",
    )
    add_check(
        "missing_values",
        "pass" if int(hourly.isna().sum().sum()) == 0 else "fail",
        "0 missing values in dashboard hourly table",
        str(int(hourly.isna().sum().sum())),
        "Dashboard-ready metric columns should be complete.",
    )
    add_check(
        "supply_weather_case_coverage",
        "pass" if weather_roles == EXPECTED_WEATHER_ROLES else "fail",
        ", ".join(EXPECTED_WEATHER_ROLES),
        ", ".join(weather_roles),
        "The dashboard preserves average-, high-, and low-wind supply/weather cases from the canonical Objective 3 outputs.",
    )
    add_check(
        "smr_case_coverage",
        "pass" if smr_cases == EXPECTED_SMR_CASES else "fail",
        ", ".join(EXPECTED_SMR_CASES),
        ", ".join(smr_cases),
        "Staggered commissioning and simultaneous commissioning stress-test are both available.",
    )
    add_check(
        "annual_summary_rows",
        "pass" if len(annual) == 576 else "fail",
        "576 rows",
        f"{len(annual):,} rows",
        "Annual summaries cover every scenario combination from 2030 to 2045.",
    )
    add_check(
        "whole_period_summary_rows",
        "pass" if len(period) == 36 else "fail",
        "36 rows",
        f"{len(period):,} rows",
        "Whole-period summaries cover every scenario combination.",
    )

    qa = pd.DataFrame(checks)
    output_path = output_dir / "qa_checks.csv"
    qa.to_csv(output_path, index=False)
    print(f"Wrote {output_path} ({len(qa):,} rows)")
    return qa


def build_sensitivity_definitions(output_dir: Path) -> pd.DataFrame:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "dimension": "Main model",
            "definition": "average_wind + staggered_commissioning",
            "notes": "Central supply/weather case combined with phased SMR deployment.",
        },
        {
            "dimension": "SMR deployment stress-test",
            "definition": "simultaneous_commissioning",
            "notes": "All three SMR units are treated as available from the first commissioning year to test faster deployment.",
        },
        {
            "dimension": "High-wind supply sensitivity",
            "definition": "weather_year_role == high_wind",
            "notes": "Higher-wind supply case retained from the canonical Objective 3 scenario cube.",
        },
        {
            "dimension": "Low-wind supply stress case",
            "definition": "weather_year_role == low_wind",
            "notes": "Used for low-wind pressure-day and resilience analysis.",
        },
    ]
    df = pd.DataFrame(rows)
    output_path = output_dir / "sensitivity_definitions.csv"
    df.to_csv(output_path, index=False)
    print(f"Wrote {output_path} ({len(df):,} rows)")
    return df


def copy_smr_assumptions(repo_root: Path, output_dir: Path) -> pd.DataFrame:
    output_dir.mkdir(parents=True, exist_ok=True)
    source = repo_root / "config" / "smr_assumptions.csv"
    assumptions = read_csv(source)

    if "net_delivery_factor" in assumptions.columns:
        bad_derating = assumptions["net_delivery_factor"].astype(float).lt(1.0).any()
        if bad_derating:
            raise ValueError("SMR assumptions contain net_delivery_factor below 1.0. Expected current explicit availability model.")

    output_path = output_dir / "smr_assumptions.csv"
    assumptions.to_csv(output_path, index=False)
    print(f"Wrote {output_path} ({len(assumptions):,} rows)")
    return assumptions


def build_dashboard_data(repo_root: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    hourly = build_hourly(repo_root, output_dir)
    annual = build_annual(repo_root, output_dir)
    period = build_period(repo_root, output_dir)
    rankings = build_low_wind_rankings(hourly, output_dir)
    build_low_wind_case_day(hourly, rankings, output_dir)
    build_qa_checks(hourly, annual, period, output_dir)
    build_sensitivity_definitions(output_dir)
    copy_smr_assumptions(repo_root, output_dir)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Objective 3 dashboard-ready data from repo-generated outputs.")
    parser.add_argument(
        "--repo-root",
        default=str(repo_root_from_script()),
        help="Repository root. Defaults to the root inferred from this script path.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Dashboard data output directory. Defaults to outputs/dashboard/objective3_smr_integration/data.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    repo_root = Path(args.repo_root).resolve()
    output_dir = Path(args.output_dir).resolve() if args.output_dir else default_output_dir(repo_root)

    build_dashboard_data(repo_root=repo_root, output_dir=output_dir)
    print()
    print("Dashboard data build complete.")
    print(f"Data directory: {output_dir}")


if __name__ == "__main__":
    main()
