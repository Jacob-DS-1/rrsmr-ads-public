from pathlib import Path


RUNNER = Path("scripts/run_objective3_integration.sh")
DOC = Path("docs/validation/objective3_smr_integration/owner3_integration_runner.md")
OWNER3 = Path("src/rrsmr_ads/objective3_smr_integration/owner3_data_integration.py")


def test_objective3_integration_runner_exists():
    assert RUNNER.exists()


def test_objective3_integration_runner_regenerates_smr_fleet_then_integrates():
    text = RUNNER.read_text(encoding="utf-8")

    assert "scripts/run_objective3_smr_fleet.sh --clean" in text
    assert "owner3_data_integration.py --integrate" in text
    assert text.index("scripts/run_objective3_smr_fleet.sh --clean") < text.index(
        "owner3_data_integration.py --integrate"
    )


def test_owner3_writes_canonical_and_legacy_grid_master_outputs():
    text = OWNER3.read_text(encoding="utf-8")

    assert 'output_path = paths.outputs_dir / "grid_master_hourly_2030_2045"' in text
    assert 'legacy_output_path = paths.outputs_dir / "grid_master_hourly_2030_2045.parquet"' in text
    assert "write_parquet(master[final_cols], output_path)" in text
    assert "write_parquet(master[final_cols], legacy_output_path)" in text


def test_objective3_integration_runner_doc_records_contract():
    text = DOC.read_text(encoding="utf-8")

    required = [
        "scripts/run_objective3_integration.sh",
        "data/processed/objective1_generation/generation_future_hourly_2030_2045",
        "data/processed/objective2_demand/demand_future_hourly_2030_2045",
        "data/processed/objective3_smr_integration/smr_fleet_hourly_2030_2045",
        "data/processed/objective3_smr_integration/grid_master_hourly_2030_2045",
        "data/processed/objective3_smr_integration/grid_master_hourly_2030_2045.parquet",
        "timestamp_utc",
        "weather_year_role",
        "climate_member",
        "smr_case",
        "5049216",
    ]

    for item in required:
        assert item in text


def test_owner3_cli_uses_integrate_not_build():
    text = OWNER3.read_text(encoding="utf-8")

    assert "--integrate" in text
    assert "--stage-check" in text
