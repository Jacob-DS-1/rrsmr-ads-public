from pathlib import Path

import pandas as pd

from rrsmr_ads.objective3_smr_integration.system_impact_metrics import (
    ANNUAL_OUTPUT_NAME,
    HOURLY_OUTPUT_NAME,
    LEGACY_HOURLY_OUTPUT_NAME,
    PERIOD_OUTPUT_NAME,
    build_annual_summary,
    build_hourly_metrics,
    build_period_summary,
)


def _sample_grid_master() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(
                ["2030-01-01 00:00:00+00:00", "2030-01-01 01:00:00+00:00"]
            ),
            "year": [2030, 2030],
            "fes_scenario": ["Electric Engagement", "Electric Engagement"],
            "climate_member": ["member_06", "member_06"],
            "weather_year_role": ["average_wind", "average_wind"],
            "weather_year": [2014, 2014],
            "smr_case": ["staggered_commissioning", "staggered_commissioning"],
            "demand_mw": [100.0, 50.0],
            "wind_mw": [10.0, 1.0],
            "solar_mw": [20.0, 20.0],
            "nuclear_existing_mw": [20.0, 20.0],
            "biomass_mw": [5.0, 5.0],
            "hydro_mw": [3.0, 3.0],
            "other_mw": [2.0, 2.0],
            "imports_net_baseline_mw": [10.0, 10.0],
            "smr_total_delivered_mw": [20.0, 20.0],
        }
    )


def test_system_impact_hourly_metrics_apply_rule_based_balancing() -> None:
    hourly = build_hourly_metrics(_sample_grid_master(), low_wind_quantile=0.5)

    first = hourly.iloc[0]
    assert first["exogenous_supply_mw"] == 60.0
    assert first["residual_before_smr_mw"] == 30.0
    assert first["residual_after_smr_mw"] == 10.0
    assert first["gas_needed_before_mw"] == 30.0
    assert first["gas_needed_after_mw"] == 10.0
    assert first["gas_displacement_proxy_mw"] == 20.0
    assert first["surplus_after_smr_mw"] == 0.0

    second = hourly.iloc[1]
    assert second["residual_before_smr_mw"] == -11.0
    assert second["residual_after_smr_mw"] == -31.0
    assert second["gas_needed_before_mw"] == 0.0
    assert second["gas_needed_after_mw"] == 0.0
    assert second["gas_displacement_proxy_mw"] == 0.0
    assert second["surplus_after_smr_mw"] == 31.0


def test_system_impact_summaries_aggregate_hourly_metrics() -> None:
    hourly = build_hourly_metrics(_sample_grid_master(), low_wind_quantile=0.5)

    annual = build_annual_summary(hourly)
    assert len(annual) == 1

    row = annual.iloc[0]
    assert row["annual_smr_delivered_energy_twh"] == 40.0 / 1_000_000
    assert row["annual_gas_displacement_twh"] == 20.0 / 1_000_000
    assert row["annual_surplus_energy_twh"] == 31.0 / 1_000_000
    assert row["surplus_hours_count"] == 1
    assert row["low_wind_hours_count"] == 1
    assert row["low_wind_support_hours"] == 0

    period = build_period_summary(hourly)
    assert len(period) == 1

    period_row = period.iloc[0]
    assert period_row["cumulative_smr_delivered_energy_twh"] == 40.0 / 1_000_000
    assert period_row["cumulative_gas_displacement_twh"] == 20.0 / 1_000_000
    assert period_row["total_surplus_hours"] == 1
    assert period_row["total_low_wind_hours"] == 1


def test_system_impact_runner_declares_repo_path_outputs() -> None:
    runner = Path("scripts/run_objective3_system_impact.sh").read_text(encoding="utf-8")
    docs = Path(
        "docs/validation/objective3_smr_integration/system_impact_runner.md"
    ).read_text(encoding="utf-8")

    assert "grid_master_hourly_2030_2045" in runner
    assert "system_impact_metrics.py" in runner

    for output_name in [
        HOURLY_OUTPUT_NAME,
        LEGACY_HOURLY_OUTPUT_NAME,
        ANNUAL_OUTPUT_NAME,
        PERIOD_OUTPUT_NAME,
    ]:
        assert output_name in docs
