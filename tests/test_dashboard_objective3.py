from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd


BUILDER = Path("dashboard/objective3_smr_integration/scripts/build_dashboard_data.py")
APP = Path("dashboard/objective3_smr_integration/app.py")


def load_builder():
    spec = importlib.util.spec_from_file_location("dashboard_builder", BUILDER)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_dashboard_source_files_exist() -> None:
    assert APP.exists()
    assert BUILDER.exists()
    assert Path("dashboard/objective3_smr_integration/README.md").exists()
    assert Path("dashboard/objective3_smr_integration/dashboard_data_dictionary.md").exists()


def test_dashboard_builder_uses_canonical_repo_outputs_not_owner5_subset() -> None:
    text = BUILDER.read_text()

    assert "system_impact_hourly_2030_2045" in text
    assert "grid_master_hourly_2030_2045" in text
    assert "system_impact_summary_annual_2030_2045.csv" in text
    assert "system_impact_summary_period_2030_2045.csv" in text
    assert "config" in text
    assert "smr_assumptions.csv" in text

    assert "owner5_sensitivity" not in text
    assert "system_impact_metrics_hourly_owner5_sensitivity" not in text


def test_dashboard_app_reads_generated_output_dir_with_environment_override() -> None:
    text = APP.read_text()

    assert "RRSMR_DASHBOARD_DATA_DIR" in text
    assert "outputs" in text
    assert "dashboard" in text
    assert "build_dashboard_data.py" in text


def test_dashboard_builder_rejects_stale_smr_derating(tmp_path: Path) -> None:
    builder = load_builder()

    repo = tmp_path / "repo"
    output_dir = tmp_path / "dashboard_data"
    config = repo / "config"
    config.mkdir(parents=True)
    pd.DataFrame(
        {
            "unit_id": [1],
            "smr_case": ["staggered_commissioning"],
            "nameplate_mwe": [470],
            "net_delivery_factor": [0.9],
            "forced_outage_rate": [0.05],
        }
    ).to_csv(config / "smr_assumptions.csv", index=False)

    try:
        builder.copy_smr_assumptions(repo, output_dir)
    except ValueError as exc:
        assert "net_delivery_factor below 1.0" in str(exc)
    else:
        raise AssertionError("Expected stale derating assumptions to be rejected.")


def test_dashboard_builder_tiny_fixture_preserves_three_weather_roles(tmp_path: Path) -> None:
    builder = load_builder()

    repo = tmp_path / "repo"
    processed = repo / "data" / "processed" / "objective3_smr_integration"
    processed.mkdir(parents=True)
    output_dir = tmp_path / "dashboard_data"

    rows = []
    for weather_role in ["average_wind", "high_wind", "low_wind"]:
        for smr_case in ["simultaneous_commissioning", "staggered_commissioning"]:
            rows.append(
                {
                    "timestamp_utc": "2030-01-01 00:00:00+00:00",
                    "year": 2030,
                    "fes_scenario": "Electric Engagement",
                    "climate_member": "member_06",
                    "weather_year_role": weather_role,
                    "smr_case": smr_case,
                    "demand_mw": 1000.0,
                    "wind_mw": 100.0,
                    "exogenous_supply_mw": 500.0,
                    "imports_net_baseline_mw": 50.0,
                    "smr_total_delivered_mw": 470.0,
                    "residual_before_smr_mw": 450.0,
                    "residual_after_smr_mw": -20.0,
                    "gas_needed_before_mw": 450.0,
                    "gas_needed_after_mw": 0.0,
                    "gas_displacement_proxy_mw": 450.0,
                    "surplus_after_smr_mw": 20.0,
                    "residual_reduction_mw": 470.0,
                    "low_wind_flag": weather_role == "low_wind",
                    "low_wind_support_flag": weather_role == "low_wind",
                }
            )

    hourly = pd.DataFrame(rows)
    hourly.to_parquet(processed / "system_impact_hourly_2030_2045", index=False)

    grid = hourly[
        [
            "timestamp_utc",
            "year",
            "fes_scenario",
            "climate_member",
            "weather_year_role",
            "smr_case",
        ]
    ].copy()
    grid["unit1_delivered_mw"] = 470.0
    grid["unit2_delivered_mw"] = 0.0
    grid["unit3_delivered_mw"] = 0.0
    grid.to_parquet(processed / "grid_master_hourly_2030_2045", index=False)

    built = builder.build_hourly(repo, output_dir)

    assert len(built) == 6
    assert sorted(str(v) for v in built["weather_year_role"].astype(str).unique()) == [
        "average_wind",
        "high_wind",
        "low_wind",
    ]
    assert (output_dir / "hourly_metrics_dashboard.parquet").exists()
