from pathlib import Path


def test_objective2_runner_script_exists_and_runs_notebooks_in_order():
    script = Path("scripts/run_objective2_demand.sh")
    assert script.exists()

    text = script.read_text(encoding="utf-8")

    expected_order = [
        "notebooks/objective2_demand/task1_demand_model_training_data.ipynb",
        "notebooks/objective2_demand/task2_weather_feature_engineering.ipynb",
        "notebooks/objective2_demand/task3_demand_model_integration.ipynb",
        "notebooks/objective2_demand/task4_ukcp18_member_selection.ipynb",
        "notebooks/objective2_demand/task5_future_daily_climate_demand.ipynb",
        "notebooks/objective2_demand/task6_fes_annual_anchoring.ipynb",
        "notebooks/objective2_demand/task7_hourly_disaggregation.ipynb",
        "notebooks/objective2_demand/task8_final_demand_qa_export.ipynb",
    ]

    positions = [text.index(item) for item in expected_order]
    assert positions == sorted(positions)

    assert "jupyter nbconvert" in text
    assert "--execute" in text
    assert "--clean" in text
    assert "EXECUTED_DIR" in text
    assert "KERNEL_NAME" in text
    assert '--ExecutePreprocessor.kernel_name="$KERNEL_NAME"' in text
    assert "/tmp/rrsmr-ads-objective2-rerun-notebooks" in text


def test_objective2_runner_documents_generated_output_locations():
    text = Path("scripts/run_objective2_demand.sh").read_text(encoding="utf-8")

    assert "data/processed/objective2_demand" in text
    assert "outputs/objective2_demand" in text
    assert "demand_future_hourly_2030_2045.parquet" in text
