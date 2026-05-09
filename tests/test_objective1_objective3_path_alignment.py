from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TASK5_PATH = REPO_ROOT / "src" / "task5_fes_anchoring_and_export.py"
OWNER3_PATH = (
    REPO_ROOT
    / "src"
    / "rrsmr_ads"
    / "objective3_smr_integration"
    / "owner3_data_integration.py"
)


def test_objective1_final_generation_output_matches_objective3_expected_input() -> None:
    task5_text = TASK5_PATH.read_text(encoding="utf-8")
    owner3_text = OWNER3_PATH.read_text(encoding="utf-8")

    assert 'PROCESSED_OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" / "objective1_generation"' in task5_text
    assert 'WIDE_EXPORT_PATH = PROCESSED_OUTPUT_DIR / "generation_future_hourly_2030_2045.parquet"' in task5_text

    assert "resolve_generated_parquet_path" in owner3_text
    assert "obj1_supply" in owner3_text
    assert '"objective1_generation"' in owner3_text
    assert '"generation_future_hourly_2030_2045"' in owner3_text


def test_objective1_legacy_output_copy_is_retained_for_qa_continuity() -> None:
    task5_text = TASK5_PATH.read_text(encoding="utf-8")

    assert 'LEGACY_WIDE_EXPORT_PATH = OUTPUT_DIR / "generation_future_hourly_2030_2045.parquet"' in task5_text
    assert "wide.to_parquet(LEGACY_WIDE_EXPORT_PATH, index=False)" in task5_text
