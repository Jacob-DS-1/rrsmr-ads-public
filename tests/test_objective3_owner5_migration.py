from __future__ import annotations

import ast
import csv
import subprocess
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_ROOT / "src" / "rrsmr_ads" / "objective3_smr_integration" / "owner5_visualisations_final_qa.py"
VALIDATION_DIR = PROJECT_ROOT / "docs" / "validation" / "objective3_smr_integration"


def read_csv_dicts(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_owner5_script_exists_and_compiles() -> None:
    source = SCRIPT.read_text(encoding="utf-8")
    ast.parse(source)


def test_owner5_script_uses_repo_relative_paths_not_old_handoff_paths() -> None:
    source = SCRIPT.read_text(encoding="utf-8")

    required_terms = [
        "data",
        "processed",
        "objective3_smr_integration",
        "outputs",
        "figures",
        "docs",
        "validation",
        "config",
        "smr_assumptions.csv",
    ]
    for term in required_terms:
        assert term in source

    forbidden_terms = [
        "/Users/",
        "PYTHONPATH",
        ".local_pydeps",
        "Objective 3 SMR Integration Outputs",
        "Owner 1 SMR Logic & Unit Library Generation",
        "Owner 2 SMR Fleet Scenario Expansion",
        "Owner 3 Data Integration",
        "Owner 4 System Impact Metrics Calculation",
        "Owner 5 Visualisations Final QA & Project Packaging",
    ]
    for term in forbidden_terms:
        assert term not in source


def test_owner5_validation_evidence_exists() -> None:
    expected_files = [
        "objective3_master_README.md",
        "owner5_QA_reconciliation_report.md",
        "owner5_input_manifest.csv",
        "owner5_sensitivity_definitions.csv",
        "owner5_system_impact_summary_annual_sensitivity_2030_2045.csv",
        "owner5_system_impact_summary_period_sensitivity_2030_2045.csv",
        "owner5_qa_checks.csv",
    ]

    for filename in expected_files:
        assert (VALIDATION_DIR / filename).is_file(), filename


def test_owner5_qa_checks_pass() -> None:
    rows = read_csv_dicts(VALIDATION_DIR / "owner5_qa_checks.csv")
    assert len(rows) == 10

    by_check = {row["check_name"]: row for row in rows}
    expected_checks = {
        "owner4_hourly_case_coverage",
        "owner5_metrics_row_count",
        "owner5_metrics_duplicate_key",
        "owner5_metrics_missing_values",
        "owner4_hourly_recomputed_alignment",
        "weather_role_coverage",
        "smr_case_coverage",
        "annual_summary_rows",
        "period_summary_rows",
        "figure_outputs",
    }
    assert set(by_check) == expected_checks

    for row in rows:
        assert row["status"] == "pass"

    assert by_check["owner5_metrics_row_count"]["observed"] == "3366144"
    assert by_check["owner5_metrics_duplicate_key"]["observed"] == "0"
    assert by_check["owner5_metrics_missing_values"]["observed"] == "0"
    assert by_check["annual_summary_rows"]["observed"] == "384"
    assert by_check["period_summary_rows"]["observed"] == "24"


def test_owner5_sensitivity_definitions_document_scope() -> None:
    rows = read_csv_dicts(VALIDATION_DIR / "owner5_sensitivity_definitions.csv")
    definitions = {row["dimension"]: row for row in rows}

    assert definitions["main_model"]["definition"] == (
        "average_wind + staggered_commissioning"
    )
    assert definitions["smr_deployment_sensitivity"]["definition"] == (
        "simultaneous_commissioning"
    )
    assert definitions["supply_sensitivity"]["definition"] == (
        "weather_year_role == low_wind"
    )


def test_owner5_summary_evidence_shapes_and_cases() -> None:
    annual = read_csv_dicts(
        VALIDATION_DIR / "owner5_system_impact_summary_annual_sensitivity_2030_2045.csv"
    )
    period = read_csv_dicts(
        VALIDATION_DIR / "owner5_system_impact_summary_period_sensitivity_2030_2045.csv"
    )

    assert len(annual) == 384
    assert len(period) == 24

    annual_cases = {row["smr_case"] for row in annual}
    period_cases = {row["smr_case"] for row in period}
    assert annual_cases == {"staggered_commissioning", "simultaneous_commissioning"}
    assert period_cases == {"staggered_commissioning", "simultaneous_commissioning"}

    assert {row["weather_year_role"] for row in annual} == {"average_wind", "low_wind"}
    assert {row["weather_year_role"] for row in period} == {"average_wind", "low_wind"}


def test_owner5_readme_is_cleaned_for_repo_migration() -> None:
    readme = (VALIDATION_DIR / "objective3_master_README.md").read_text(
        encoding="utf-8"
    )

    required_terms = [
        "data/processed/objective3_smr_integration",
        "outputs/objective3_smr_integration",
        "outputs/figures/objective3_smr_integration",
        "docs/validation/objective3_smr_integration",
        "python src/rrsmr_ads/objective3_smr_integration/owner5_visualisations_final_qa.py --build",
    ]
    for term in required_terms:
        assert term in readme

    forbidden_terms = [
        "/Users/",
        "PYTHONPATH",
        ".local_pydeps",
        "Objective 3 SMR Integration Outputs",
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
    forbidden_suffixes = (".parquet", ".pkl", ".joblib", ".pyc", ".png", ".jpg", ".jpeg")

    offenders = [
        path
        for path in tracked_files
        if path.endswith(forbidden_suffixes)
        or "__pycache__/" in path
        or ".egg-info/" in path
        or ".DS_Store" in path
        or ".Rhistory" in path
    ]

    assert offenders == []
