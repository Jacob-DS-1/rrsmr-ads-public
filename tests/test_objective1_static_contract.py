from pathlib import Path
import ast
import py_compile
import subprocess
import tempfile


REPO_ROOT = Path(__file__).resolve().parents[1]

OBJECTIVE1_SCRIPTS = [
    REPO_ROOT / "src" / "task1_prep_and_calibration.py",
    REPO_ROOT / "src" / "task2_ml_training.py",
    REPO_ROOT / "src" / "task3_baseline_and_weather_scaffold.py",
    REPO_ROOT / "src" / "task4_weather_adjustment.py",
    REPO_ROOT / "src" / "task5_fes_anchoring_and_export.py",
]

FINAL_SCHEMA_COLUMNS = {
    "timestamp_utc",
    "year",
    "fes_scenario",
    "weather_year",
    "weather_year_role",
    "wind_mw",
    "solar_mw",
    "nuclear_existing_mw",
    "biomass_mw",
    "hydro_mw",
    "other_mw",
    "storage_net_mw",
    "gas_reference_mw",
    "coal_reference_mw",
    "imports_net_baseline_mw",
}

REQUIRED_TECH_TO_WIDE_COLUMN_VALUES = {
    "wind_mw",
    "solar_mw",
    "nuclear_existing_mw",
    "biomass_mw",
    "hydro_mw",
    "other_mw",
    "storage_net_mw",
    "gas_reference_mw",
    "coal_reference_mw",
    "imports_net_baseline_mw",
}


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _assigned_names(path: Path) -> set[str]:
    tree = ast.parse(_read(path), filename=str(path))
    names: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name):
                names.add(node.target.id)

    return names


def test_objective1_scripts_exist():
    for script in OBJECTIVE1_SCRIPTS:
        assert script.exists(), f"Missing Objective 1 script: {script}"


def test_objective1_scripts_compile_without_execution():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        for script in OBJECTIVE1_SCRIPTS:
            cfile = tmpdir_path / f"{script.stem}.pyc"
            py_compile.compile(str(script), cfile=str(cfile), doraise=True)


def test_task5_declares_required_contract_constants():
    task5 = REPO_ROOT / "src" / "task5_fes_anchoring_and_export.py"
    assigned_names = _assigned_names(task5)

    assert "TECH_TO_WIDE_COLUMN" in assigned_names
    assert "TECH_ANCHORING" in assigned_names


def test_task5_contains_expected_final_schema_columns():
    task5_source = _read(REPO_ROOT / "src" / "task5_fes_anchoring_and_export.py")

    missing = sorted(
        column for column in FINAL_SCHEMA_COLUMNS
        if column not in task5_source
    )

    assert missing == []


def test_task5_contains_required_tech_to_wide_column_values():
    task5_source = _read(REPO_ROOT / "src" / "task5_fes_anchoring_and_export.py")

    missing = sorted(
        column for column in REQUIRED_TECH_TO_WIDE_COLUMN_VALUES
        if column not in task5_source
    )

    assert missing == []


def test_objective1_validation_evidence_files_exist():
    expected_files = [
        "docs/planning/objective1_master_readme.md",
        "docs/decisions/objective1_peer_review_updates.md",
        "docs/validation/objective1_generation/qa_notes.md",
        "docs/validation/objective1_generation/renewable_model_README.md",
        "docs/validation/objective1_generation/supply_model_training_README.md",
        "docs/validation/objective1_generation/fes_anchoring_multipliers.csv",
        "docs/validation/objective1_generation/qa_dukes_loadfactor_check.csv",
        "docs/validation/objective1_generation/qa_fes_reconciliation.csv",
        "docs/validation/objective1_generation/tech_year_calibration.csv",
        "config/taxonomies/genmix_taxonomy_map.csv",
    ]

    missing = [
        path for path in expected_files
        if not (REPO_ROOT / path).exists()
    ]

    assert missing == []


def test_no_objective1_generated_artifacts_are_tracked():
    forbidden_suffixes = {
        ".parquet",
        ".joblib",
        ".pyc",
    }

    tracked_files = subprocess.run(
        ["git", "ls-files"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()

    offenders = []

    for file_path in tracked_files:
        path = Path(file_path)

        if any(part == "__pycache__" for part in path.parts):
            offenders.append(file_path)
        elif any(part.endswith(".egg-info") for part in path.parts):
            offenders.append(file_path)
        elif path.suffix in forbidden_suffixes:
            offenders.append(file_path)

    assert offenders == []