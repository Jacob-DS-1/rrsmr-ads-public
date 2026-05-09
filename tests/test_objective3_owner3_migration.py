from __future__ import annotations

import ast
import csv
import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_ROOT / "src" / "rrsmr_ads" / "objective3_smr_integration" / "owner3_data_integration.py"
VALIDATION_DIR = PROJECT_ROOT / "docs" / "validation" / "objective3_smr_integration"


def read_csv_dicts(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_owner3_script_exists_and_compiles() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    ast.parse(source)


def test_owner3_script_uses_repo_relative_paths_not_old_handoff_paths() -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    required_terms = [
        "data",
        "processed",
        "objective1_generation",
        "objective2_demand",
        "objective3_smr_integration",
        "docs",
        "validation",
    ]
    for term in required_terms:
        assert term in source

    forbidden_terms = [
        "/Users/",
        "Objective 1 Supply Modeling Outputs",
        "Objective 2 Demand Modeling Outputs",
        "Jacob",
        "Task 8",
        "object3_outputs_extracted",
        "PYTHONPATH",
        ".local_pydeps",
    ]
    for term in forbidden_terms:
        assert term not in source


def test_owner3_validation_evidence_exists() -> None:
    expected_files = [
        "owner3_data_integration_README.md",
        "input_alignment_qa_log.md",
        "grid_master_schema.csv",
        "integration_summary.csv",
        "duplicate_key_check.csv",
        "missing_value_check.csv",
        "owner3_decision_register.csv",
        "row_count_reconciliation.csv",
        "scenario_coverage_check.csv",
        "timestamp_coverage_check.csv",
    ]

    for filename in expected_files:
        assert (VALIDATION_DIR / filename).is_file(), filename


def test_owner3_integration_summary_records_completed_master_output() -> None:
    rows = read_csv_dicts(VALIDATION_DIR / "integration_summary.csv")
    by_check = {row["check_name"]: row for row in rows}

    assert by_check["final_rows"]["status"] == "pass"
    assert by_check["final_rows"]["observed"] == "2524608"

    assert by_check["final_key"]["status"] == "pass"
    assert by_check["final_key"]["observed"] == (
        "timestamp_utc + fes_scenario + climate_member + weather_year_role + smr_case"
    )



def test_owner3_qa_evidence_records_no_duplicates_or_missing_values() -> None:
    duplicate_rows = read_csv_dicts(VALIDATION_DIR / "duplicate_key_check.csv")
    assert duplicate_rows
    for row in duplicate_rows:
        assert row["status"] == "pass"
        assert row["duplicate_count"] == "0"

    missing_rows = read_csv_dicts(VALIDATION_DIR / "missing_value_check.csv")
    assert missing_rows
    for row in missing_rows:
        assert row["status"] == "pass"
        assert row["missing_values"] == "0"


def test_owner3_scenario_and_timestamp_coverage() -> None:
    scenario_rows = read_csv_dicts(VALIDATION_DIR / "scenario_coverage_check.csv")
    scenario_values = {row["dimension"]: row for row in scenario_rows}

    assert scenario_values["fes_scenario"]["values"] == (
        "Electric Engagement; Holistic Transition"
    )
    assert scenario_values["climate_member"]["values"] == (
        "member_06; member_12; member_13"
    )
    assert scenario_values["weather_year_role"]["values"] == (
        "average_wind; high_wind; low_wind"
    )
    assert scenario_values["smr_case"]["values"] == (
        "simultaneous_commissioning; staggered_commissioning"
    )

    timestamp_rows = read_csv_dicts(VALIDATION_DIR / "timestamp_coverage_check.csv")
    for row in timestamp_rows:
        assert row["expected_start_utc"] == "2030-01-01 00:00:00+00:00"
        assert row["expected_end_utc"] == "2045-12-31 23:00:00+00:00"
        assert row["unique_timestamps"] == "140256"
        assert row["missing_timestamps"] == "0"
        assert row["extra_timestamps"] == "0"
        assert row["status"] == "pass"


def test_owner3_readme_is_cleaned_for_repo_migration() -> None:
    readme = (VALIDATION_DIR / "owner3_data_integration_README.md").read_text(
        encoding="utf-8"
    )

    required_terms = [
        "data/processed/objective1_generation/generation_future_hourly_2030_2045",
        "data/processed/objective2_demand/demand_future_hourly_2030_2045",
        "data/processed/objective3_smr_integration/smr_fleet_hourly_2030_2045",
    ]
    for term in required_terms:
        assert term in readme

    forbidden_terms = [
        "/Users/",
        "PYTHONPATH",
        ".local_pydeps",
        "Objective 1 Supply Modeling Outputs",
        "Objective 2 Demand Modeling Outputs",
        "Jacob/Task 8",
    ]
    for term in forbidden_terms:
        assert term not in readme


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
