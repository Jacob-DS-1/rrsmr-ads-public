import csv
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


EXPECTED_OBJECTIVE3_FILES = {
    "config/smr_assumptions.csv",
    "docs/decisions/objective3_rule_based_balancing.md",
    "docs/decisions/objective3_smr_assumptions_reconciliation.md",
    "docs/planning/objective3_smr_integration_scaffold.md",
    "docs/validation/objective3_smr_integration/migration_completeness.md",
    "docs/validation/objective3_smr_integration/owner3_data_integration_README.md",
    "docs/validation/objective3_smr_integration/owner4_system_impact_metrics_README.md",
    "docs/validation/objective3_smr_integration/owner4_system_impact_summary_annual_2030_2045.csv",
    "docs/validation/objective3_smr_integration/owner4_system_impact_summary_period_2030_2045.csv",
    "docs/validation/objective3_smr_integration/objective3_master_README.md",
    "docs/validation/objective3_smr_integration/owner5_QA_reconciliation_report.md",
    "src/rrsmr_ads/objective3_smr_integration/__init__.py",
    "src/rrsmr_ads/objective3_smr_integration/owner3_data_integration.py",
    "src/rrsmr_ads/objective3_smr_integration/owner5_visualisations_final_qa.py",
    "tests/test_objective3_static_contract.py",
    "tests/test_objective3_owner3_migration.py",
    "tests/test_objective3_owner4_summary_evidence.py",
    "tests/test_objective3_owner5_migration.py",
    "tests/test_objective3_smr_assumptions_reconciliation.py",
}


FORBIDDEN_TRACKED_SUFFIXES = {
    ".parquet",
    ".pkl",
    ".joblib",
    ".pyc",
    ".png",
    ".jpg",
    ".jpeg",
}


FORBIDDEN_TRACKED_PARTS = {
    "__pycache__",
    ".egg-info",
    ".DS_Store",
    ".Rhistory",
}


def test_objective3_expected_migration_files_exist() -> None:
    missing = [
        path
        for path in sorted(EXPECTED_OBJECTIVE3_FILES)
        if not (REPO_ROOT / path).exists()
    ]

    assert missing == []


def test_objective3_completion_doc_records_no_owner4_script_recreation() -> None:
    text = (
        REPO_ROOT
        / "docs"
        / "validation"
        / "objective3_smr_integration"
        / "migration_completeness.md"
    ).read_text(encoding="utf-8")

    assert "No Owner 4 source script or notebook was present" in text
    assert "migrated as summary evidence only" in text
    assert "generated artefacts: intentionally excluded" in text


def test_objective3_reconciled_smr_assumptions_are_not_placeholders() -> None:
    path = REPO_ROOT / "config" / "smr_assumptions.csv"

    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    assert len(rows) == 6
    assert "Placeholder" not in path.read_text(encoding="utf-8")

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

    for row in rows:
        assert row["nameplate_mwe"] == "470"
        assert row["net_delivery_factor"] == "1.0"
        assert row["planned_outage_window"] == "18"
        assert row["planned_outage_frequency_months"] == "24"
        assert row["forced_outage_rate"] == "0.02"


def test_no_forbidden_generated_artifacts_are_tracked() -> None:
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    tracked_files = result.stdout.splitlines()

    forbidden = [
        path
        for path in tracked_files
        if Path(path).suffix.lower() in FORBIDDEN_TRACKED_SUFFIXES
        or any(part in path for part in FORBIDDEN_TRACKED_PARTS)
    ]

    assert forbidden == []
