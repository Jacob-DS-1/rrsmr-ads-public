"""Static contract tests for Objective 2 demand migration.

These tests intentionally do not execute the Objective 2 notebooks.
They check repository structure, notebook parseability, path conventions,
validation evidence, final schema references, and forbidden tracked artefacts.
"""

from __future__ import annotations

import ast
import json
import re
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

NOTEBOOK_DIR = REPO_ROOT / "notebooks" / "objective2_demand"
VALIDATION_DIR = REPO_ROOT / "docs" / "validation" / "objective2_demand"

REQUIRED_NOTEBOOKS = [
    "task1_demand_model_training_data.ipynb",
    "task2_weather_feature_engineering.ipynb",
    "task3_demand_model_integration.ipynb",
    "task4_ukcp18_member_selection.ipynb",
    "task5_future_daily_climate_demand.ipynb",
    "task6_fes_annual_anchoring.ipynb",
    "task7_hourly_disaggregation.ipynb",
    "task8_final_demand_qa_export.ipynb",
]

REQUIRED_EVIDENCE_FILES = [
    "docs/planning/objective2_demand_migration_notes.md",
    "docs/decisions/objective2_task3_demand_model_integration.md",
    "docs/validation/objective2_demand/demand_model_training_README.md",
    "docs/validation/objective2_demand/weather_features_README.md",
    "docs/validation/objective2_demand/demand_model_README.md",
    "docs/validation/objective2_demand/ukcp18_member_selection_note_README.md",
    "docs/validation/objective2_demand/future_daily_demand_climate_only_README.md",
    "docs/validation/objective2_demand/demand_future_hourly_2030_2045_README.md",
    "docs/validation/objective2_demand/demand_future_hourly_2030_2045_QA_summary.csv",
    "docs/validation/objective2_demand/ukcp18_member_selection.csv",
    "docs/validation/objective2_demand/ukcp18_member_summary_2030_2045.csv",
]

FINAL_DEMAND_COLUMNS = [
    "timestamp_utc",
    "year",
    "fes_scenario",
    "climate_member",
    "demand_mw",
]


def _read_notebook(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _notebook_code_cells(path: Path) -> list[str]:
    notebook = _read_notebook(path)
    return [
        "".join(cell.get("source", []))
        for cell in notebook.get("cells", [])
        if cell.get("cell_type") == "code"
    ]


def _all_notebook_text(path: Path) -> str:
    notebook = _read_notebook(path)
    return "\n".join("".join(cell.get("source", [])) for cell in notebook.get("cells", []))


def test_required_objective2_notebooks_exist() -> None:
    missing = [
        notebook_name
        for notebook_name in REQUIRED_NOTEBOOKS
        if not (NOTEBOOK_DIR / notebook_name).exists()
    ]
    assert not missing, f"Missing Objective 2 notebooks: {missing}"


def test_objective2_notebook_code_cells_parse_without_execution() -> None:
    failures: list[str] = []

    for notebook_name in REQUIRED_NOTEBOOKS:
        notebook_path = NOTEBOOK_DIR / notebook_name
        for cell_number, source in enumerate(_notebook_code_cells(notebook_path), start=1):
            stripped = source.strip()
            if not stripped:
                continue

            # Skip IPython shell/magic cells if any are introduced later.
            if stripped.startswith(("%", "!", "?")):
                continue

            try:
                ast.parse(source)
            except SyntaxError as exc:
                failures.append(f"{notebook_name} cell {cell_number}: {exc}")

    assert not failures, "Notebook parse failures:\n" + "\n".join(failures)


def test_notebooks_do_not_use_old_local_input_output_path_contract() -> None:
    old_variable_pattern = re.compile(
        r"(?<![A-Za-z0-9_])(BASE_DIR|INPUT_DIR|OUTPUT_DIR)(?![A-Za-z0-9_])"
    )

    forbidden_literals = [
        "/Users/",
        '"Inputs"',
        '"Input"',
        '"Outputs"',
        '"Output"',
        "'Inputs'",
        "'Input'",
        "'Outputs'",
        "'Output'",
    ]

    failures: list[str] = []

    for notebook_name in REQUIRED_NOTEBOOKS:
        notebook_path = NOTEBOOK_DIR / notebook_name

        for cell_number, source in enumerate(_notebook_code_cells(notebook_path), start=1):
            if old_variable_pattern.search(source):
                failures.append(f"{notebook_name} cell {cell_number}: old BASE/INPUT/OUTPUT variable")

            for literal in forbidden_literals:
                if literal in source:
                    failures.append(f"{notebook_name} cell {cell_number}: old local path literal {literal}")

    assert not failures, "Old local path assumptions found:\n" + "\n".join(failures)


def test_objective2_notebooks_use_repo_relative_runtime_paths() -> None:
    path_expectations = {
        "task1_demand_model_training_data.ipynb": [
            "PREPROCESSING_DATA_DIR",
            "OBJ2_DATA_DIR",
        ],
        "task2_weather_feature_engineering.ipynb": [
            "PREPROCESSING_DATA_DIR",
            "OBJ2_DATA_DIR",
        ],
        "task3_demand_model_integration.ipynb": [
            "PROJECT_ROOT",
            "WRITE_LOCAL_OUTPUTS = False",
            "outputs",
            "objective2_demand",
        ],
        "task4_ukcp18_member_selection.ipynb": [
            "OBJ2_DATA_DIR",
            "OBJ2_VALIDATION_DIR",
        ],
        "task5_future_daily_climate_demand.ipynb": [
            "TRAIN_PATH = OBJ2_DATA_DIR",
            "WEATHER_FUTURE_PATH = OBJ2_DATA_DIR",
            "MODEL_PATH = OBJ2_MODEL_DIR",
            "FUTURE_DAILY_CLIMATE_ONLY_PATH = OBJ2_DATA_DIR",
        ],
        "task6_fes_annual_anchoring.ipynb": [
            "FES_PATH = PREPROCESSING_DATA_DIR",
            "FUTURE_DAILY_CLIMATE_ONLY_PATH = OBJ2_DATA_DIR",
            "FUTURE_DAILY_FES_ANCHORED_PATH = OBJ2_DATA_DIR",
        ],
        "task7_hourly_disaggregation.ipynb": [
            "SHAPE_PATH = PREPROCESSING_DATA_DIR",
            "FUTURE_DAILY_FES_ANCHORED_PATH = OBJ2_DATA_DIR",
            "FUTURE_DEMAND_HOURLY_RAW_PATH = OBJ2_DATA_DIR",
        ],
        "task8_final_demand_qa_export.ipynb": [
            "FES_PATH = PREPROCESSING_DATA_DIR",
            "FUTURE_DEMAND_HOURLY_RAW_PATH = OBJ2_DATA_DIR",
            "DEMAND_FUTURE_HOURLY_PATH = OBJ2_DATA_DIR",
        ],
    }

    failures: list[str] = []

    for notebook_name, expected_snippets in path_expectations.items():
        text = _all_notebook_text(NOTEBOOK_DIR / notebook_name)
        for snippet in expected_snippets:
            if snippet not in text:
                failures.append(f"{notebook_name}: missing path snippet `{snippet}`")

    assert not failures, "Missing expected repo-relative path conventions:\n" + "\n".join(failures)


def test_task3_documents_model_comparison_and_selected_xgboost_model() -> None:
    text = _all_notebook_text(NOTEBOOK_DIR / "task3_demand_model_integration.ipynb").lower()

    required_terms = [
        "linear",
        "gam",
        "random forest",
        "xgboost",
        "xgb_model_3",
    ]

    missing = [term for term in required_terms if term not in text]
    assert not missing, f"Task 3 integration notebook is missing expected modelling terms: {missing}"


def test_final_demand_schema_is_represented_in_task8() -> None:
    text = _all_notebook_text(NOTEBOOK_DIR / "task8_final_demand_qa_export.ipynb")
    missing = [column for column in FINAL_DEMAND_COLUMNS if column not in text]
    assert not missing, f"Task 8 does not represent final demand schema columns: {missing}"


def test_required_objective2_validation_and_decision_evidence_exists() -> None:
    missing = [
        rel_path
        for rel_path in REQUIRED_EVIDENCE_FILES
        if not (REPO_ROOT / rel_path).exists()
    ]
    assert not missing, f"Missing Objective 2 evidence files: {missing}"


def test_objective2_generated_outputs_are_ignored() -> None:
    gitignore_text = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8")

    required_ignored_paths = [
        "data/processed/objective2_demand/",
        "outputs/objective2_demand/",
    ]

    missing = [path for path in required_ignored_paths if path not in gitignore_text]
    assert not missing, f"Missing Objective 2 generated-output ignore rules: {missing}"


def test_forbidden_generated_artefacts_are_not_tracked_by_git() -> None:
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    tracked_files = result.stdout.splitlines()

    forbidden_suffixes = (
        ".parquet",
        ".pkl",
        ".joblib",
        ".pyc",
    )

    forbidden_parts = (
        "__pycache__",
        ".egg-info",
        "data/processed/objective2_demand/",
        "outputs/objective2_demand/",
    )

    offenders = [
        path
        for path in tracked_files
        if path.endswith(forbidden_suffixes)
        or any(part in path for part in forbidden_parts)
    ]

    assert not offenders, "Forbidden generated artefacts are tracked:\n" + "\n".join(offenders)
