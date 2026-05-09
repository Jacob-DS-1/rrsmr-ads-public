from pathlib import Path


def test_objective1_runner_script_exists_and_runs_tasks_in_order():
    script = Path("scripts/run_objective1_generation.sh")
    assert script.exists()

    text = script.read_text(encoding="utf-8")

    expected_order = [
        "src/task1_prep_and_calibration.py",
        "src/task2_ml_training.py",
        "src/task3_baseline_and_weather_scaffold.py",
        "src/task4_weather_adjustment.py",
        "src/task5_fes_anchoring_and_export.py",
    ]

    positions = [text.index(item) for item in expected_order]
    assert positions == sorted(positions)

    assert "PYTHONHASHSEED=0 python" in text
    assert "--clean" in text
    assert "data/processed/objective1_generation" in text
    assert "generation_future_hourly_2030_2045.parquet" in text


def test_objective1_runner_checks_required_preprocessing_inputs():
    text = Path("scripts/run_objective1_generation.sh").read_text(encoding="utf-8")

    required_inputs = [
        "genmix_hist_hourly.parquet",
        "era5_resource_hourly_gb_2010_2024.parquet",
        "dukes_capacity_hist_2010_2024.parquet",
        "dukes_loadfactor_hist_2010_2024.parquet",
        "genmix_profile_library.parquet",
        "fes_supply_annual_2030_2045.parquet",
        "interconnector_annual_hist_2010_2024.parquet",
    ]

    for filename in required_inputs:
        assert filename in text

    assert "restore_objective1_preprocessing_inputs.sh" in text
