import json
from pathlib import Path


NOTEBOOKS = [
    Path("notebooks/objective3_smr_integration/task1_smr_unit_library.ipynb"),
    Path("notebooks/objective3_smr_integration/task2_smr_fleet_scenarios.ipynb"),
    Path("notebooks/objective3_smr_integration/task4_system_impact_metrics.ipynb"),
]

INVENTORY = Path("docs/validation/objective3_smr_integration/missing_source_inventory.md")
COMPLETENESS = Path("docs/validation/objective3_smr_integration/migration_completeness.md")
ASSUMPTIONS = Path("config/smr_assumptions.csv")


def test_objective3_missing_source_notebooks_are_migrated():
    for path in NOTEBOOKS:
        assert path.exists(), f"Missing migrated notebook: {path}"


def test_objective3_migrated_notebooks_are_source_only_without_saved_outputs():
    for path in NOTEBOOKS:
        notebook = json.loads(path.read_text(encoding="utf-8"))
        for cell in notebook.get("cells", []):
            if cell.get("cell_type") == "code":
                assert cell.get("outputs", []) == []
                assert cell.get("execution_count") is None


def test_objective3_missing_source_inventory_documents_source_paths_and_hashes():
    text = INVENTORY.read_text(encoding="utf-8")

    required = [
        "files/part1 SMR Logic & Unit Library Generation/object3_part1.ipynb",
        "files/part2 SMR Fleet Scenario Expansion/object3_part2.ipynb",
        "files/part4 System Impact Metrics Calculation/object3_part4.ipynb",
        "task1_smr_unit_library.ipynb",
        "task2_smr_fleet_scenarios.ipynb",
        "task4_system_impact_metrics.ipynb",
        "SHA-256",
    ]

    for item in required:
        assert item in text


def test_objective3_inventory_documents_assumptions_decision():
    text = INVENTORY.read_text(encoding="utf-8")

    assert "This file was inspected but not copied over `config/smr_assumptions.csv`" in text
    assert "reconciled source of truth" in text
    assert "simultaneous commissioning stress-test case" in text


def test_reconciled_smr_assumptions_keep_base_and_sensitivity_cases():
    text = ASSUMPTIONS.read_text(encoding="utf-8")

    assert "staggered_commissioning" in text
    assert "simultaneous_commissioning" in text
    assert "unit_name" in text
    assert "notes" in text


def test_objective3_completeness_doc_records_received_source_but_not_full_rerun_claim():
    text = COMPLETENESS.read_text(encoding="utf-8")

    assert "received Owner/Part 1, 2, and 4 source notebooks" in text
    assert "fully rerunnable" in text
    assert "runner/audit" in text
