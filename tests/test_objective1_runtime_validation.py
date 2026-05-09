from pathlib import Path


def test_objective1_runtime_validation_note_exists_and_records_clean_rerun():
    note = Path("docs/validation/objective1_generation/runtime_validation_qa.md")
    assert note.exists()

    text = note.read_text(encoding="utf-8")

    required_phrases = [
        "data/processed/objective1_generation/generation_future_hourly_2030_2045",
        "rows: 841536",
        "timestamp_utc + year + fes_scenario + weather_year",
        "timestamp_utc + year + fes_scenario + weather_year_role",
        "140256 hours x 2 FES scenarios x 3 weather years = 841536 rows",
        "pytest -q: 61 passed",
        "content_sha256: a8feb70e7b7a27ec2c7087fed6486f133f109f949227429a4c2ca8a6a2073d99",
        "preprocessing prerequisites",
    ]

    missing = [phrase for phrase in required_phrases if phrase not in text]
    assert not missing, "Runtime validation note is missing expected evidence: " + "; ".join(missing)


def test_objective1_runtime_validation_documents_non_unique_short_key():
    text = Path("docs/validation/objective1_generation/runtime_validation_qa.md").read_text(
        encoding="utf-8"
    )

    assert "timestamp_utc + year + fes_scenario: 561024 duplicate rows" in text
    assert "The shorter key below is intentionally not unique" in text


def test_objective1_runtime_dependency_declares_lightgbm():
    requirements = Path("requirements.txt").read_text(encoding="utf-8").lower()
    assert "lightgbm" in requirements


def test_validation_qa_checklist_uses_weather_year_role_for_generation_and_grid_master():
    text = Path("docs/validation/qa-checklist.md").read_text(encoding="utf-8")

    assert (
        "| Generation future hourly | `timestamp_utc`, `year`, `fes_scenario`, `weather_year_role` |"
        in text
    )
    assert (
        "| Grid master hourly | `timestamp_utc`, `year`, `fes_scenario`, `climate_member`, `weather_year_role`, `smr_case` |"
        in text
    )
