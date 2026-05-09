from __future__ import annotations

import csv
import subprocess
from collections import Counter
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def read_yaml(relative_path: str) -> dict:
    with (PROJECT_ROOT / relative_path).open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def read_csv_dicts(relative_path: str) -> list[dict[str, str]]:
    with (PROJECT_ROOT / relative_path).open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_objective3_smr_assumptions_define_three_470_mwe_units_per_case() -> None:
    rows = read_csv_dicts("config/smr_assumptions.csv")

    required_columns = {
        "unit_id",
        "smr_case",
        "unit_name",
        "nameplate_mwe",
        "net_delivery_factor",
        "commissioning_date",
        "planned_outage_window",
        "forced_outage_rate",
        "notes",
    }
    assert rows
    assert required_columns.issubset(rows[0].keys())

    case_counts = Counter(row["smr_case"] for row in rows)
    assert case_counts == {
        "staggered_commissioning": 3,
        "simultaneous_commissioning": 3,
    }

    for row in rows:
        assert row["unit_id"] in {"unit_1", "unit_2", "unit_3"}
        assert row["unit_name"].startswith("wylfa_smr_")
        assert float(row["nameplate_mwe"]) == 470.0
        assert float(row["net_delivery_factor"]) > 0.0
        assert float(row["forced_outage_rate"]) >= 0.0
        assert row["commissioning_date"]

    staggered_dates = {
        row["unit_id"]: row["commissioning_date"]
        for row in rows
        if row["smr_case"] == "staggered_commissioning"
    }
    assert staggered_dates == {
        "unit_1": "2035-01-01",
        "unit_2": "2036-01-01",
        "unit_3": "2037-01-01",
    }

    simultaneous_dates = {
        row["unit_id"]: row["commissioning_date"]
        for row in rows
        if row["smr_case"] == "simultaneous_commissioning"
    }
    assert set(simultaneous_dates.values()) == {"2035-01-01"}


def test_objective3_scenario_config_has_required_project_scope() -> None:
    scenarios = read_yaml("config/scenarios.yaml")

    official_names = {
        item["official_name"]
        for item in scenarios["fes_scenarios"].values()
    }
    assert official_names == {"Electric Engagement", "Holistic Transition"}

    assert scenarios["future_years"] == {"start": 2030, "end": 2045}
    assert scenarios["time"]["canonical_column"] == "timestamp_utc"
    assert scenarios["time"]["timezone"] == "UTC"
    assert scenarios["time"]["resolution"] == "hourly"
    assert scenarios["geography"]["system_boundary"] == "GB"
    assert "Northern Ireland" in scenarios["geography"]["exclude"]

    assert set(scenarios["smr_cases"]) == {
        "staggered_commissioning",
        "simultaneous_commissioning",
    }


def test_objective3_paths_are_declared() -> None:
    paths = read_yaml("config/paths.yaml")
    model_outputs = paths["model_outputs"]

    assert model_outputs["demand_future_hourly"] == (
        "data/processed/objective2_demand/demand_future_hourly_2030_2045"
    )
    assert model_outputs["generation_future_hourly"] == (
        "data/processed/objective1_generation/generation_future_hourly_2030_2045"
    )
    assert model_outputs["smr_fleet_hourly"] == (
        "data/processed/objective3_smr_integration/smr_fleet_hourly_2030_2045"
    )
    assert model_outputs["grid_master_hourly"] == (
        "data/processed/objective3_smr_integration/grid_master_hourly_2030_2045"
    )
    assert model_outputs["annual_summary"] == (
        "outputs/tables/system_impact_summary_annual_2030_2045.csv"
    )
    assert model_outputs["period_summary"] == (
        "outputs/tables/system_impact_summary_period_2030_2045.csv"
    )


def test_objective3_docs_and_schema_exist() -> None:
    required_files = [
        "docs/planning/objective3_smr_integration_scaffold.md",
        "docs/decisions/objective3_rule_based_balancing.md",
        "docs/validation/integration-schema.md",
    ]

    for relative_path in required_files:
        assert (PROJECT_ROOT / relative_path).is_file(), relative_path

    schema = (PROJECT_ROOT / "docs/validation/integration-schema.md").read_text(
        encoding="utf-8"
    )
    required_terms = [
        "smr_fleet_hourly_2030_2045",
        "grid_master_hourly_2030_2045",
        "smr_total_delivered_mw",
        "residual_before_smr_mw",
        "residual_after_smr_mw",
        "gas_needed_before_mw",
        "gas_needed_after_mw",
        "gas_displacement_proxy_mw",
        "surplus_after_smr_mw",
    ]

    for term in required_terms:
        assert term in schema


def test_objective3_generated_outputs_are_ignored_by_git() -> None:
    result = subprocess.run(
        [
            "git",
            "check-ignore",
            "data/processed/objective3_smr_integration/smr_fleet_hourly_2030_2045/example.parquet",
            "data/processed/objective3_smr_integration/grid_master_hourly_2030_2045/example.parquet",
            "outputs/tables/system_impact_summary_annual_2030_2045.csv",
            "outputs/tables/system_impact_summary_period_2030_2045.csv",
        ],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    ignored_paths = set(result.stdout.splitlines())
    assert "data/processed/objective3_smr_integration/smr_fleet_hourly_2030_2045/example.parquet" in ignored_paths
    assert "data/processed/objective3_smr_integration/grid_master_hourly_2030_2045/example.parquet" in ignored_paths
    assert "outputs/tables/system_impact_summary_annual_2030_2045.csv" in ignored_paths
    assert "outputs/tables/system_impact_summary_period_2030_2045.csv" in ignored_paths


def test_forbidden_generated_artifacts_are_not_tracked() -> None:
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    tracked_files = result.stdout.splitlines()
    forbidden_suffixes = (".parquet", ".pkl", ".joblib", ".pyc")

    offenders = [
        path
        for path in tracked_files
        if path.endswith(forbidden_suffixes)
        or "__pycache__/" in path
        or ".egg-info/" in path
    ]

    assert offenders == []
