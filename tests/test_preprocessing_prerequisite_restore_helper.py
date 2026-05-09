from pathlib import Path


REQUIRED_OBJECTIVE1_PREPROCESSING_FILES = [
    "genmix_hist_hourly.parquet",
    "era5_resource_hourly_gb_2010_2024.parquet",
    "dukes_capacity_hist_2010_2024.parquet",
    "dukes_loadfactor_hist_2010_2024.parquet",
    "genmix_profile_library.parquet",
    "fes_supply_annual_2030_2045.parquet",
    "interconnector_annual_hist_2010_2024.parquet",
]


def test_objective1_preprocessing_restore_script_documents_required_inputs():
    script = Path("scripts/restore_objective1_preprocessing_inputs.sh")
    assert script.exists()

    text = script.read_text(encoding="utf-8")

    for filename in REQUIRED_OBJECTIVE1_PREPROCESSING_FILES:
        assert filename in text

    assert "data/processed/preprocessing" in text
    assert "source_inputs" in text
    assert "cp -v" in text


def test_objective1_preprocessing_prerequisite_note_matches_restore_script():
    note = Path("docs/validation/preprocessing/objective1_preprocessing_prerequisites.md")
    assert note.exists()

    text = note.read_text(encoding="utf-8")

    for filename in REQUIRED_OBJECTIVE1_PREPROCESSING_FILES:
        assert filename in text

    assert "scripts/restore_objective1_preprocessing_inputs.sh" in text
    assert "docs/reproducibility/preprocessing_workflow.md" in text
    assert "stage-specific missing-input message" in text
