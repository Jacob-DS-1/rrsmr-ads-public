import importlib.util
import sys
from pathlib import Path

import pandas as pd


MODULE_PATH = Path("src/rrsmr_ads/objective3_smr_integration/smr_fleet.py")
RUNNER = Path("scripts/run_objective3_smr_fleet.sh")
DOC = Path("docs/validation/objective3_smr_integration/smr_fleet_runner.md")


def load_module():
    spec = importlib.util.spec_from_file_location("smr_fleet", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_smr_fleet_runner_files_exist():
    assert MODULE_PATH.exists()
    assert RUNNER.exists()
    assert DOC.exists()


def test_smr_fleet_module_uses_repo_paths_not_source_population_paths():
    text = MODULE_PATH.read_text(encoding="utf-8")
    assert "/Users/" not in text
    assert "ADS_Project_Files" not in text
    assert "repo_population" not in text
    assert "config" in text
    assert "smr_assumptions.csv" in text
    assert "data" in text
    assert "processed" in text


def test_smr_fleet_runner_wraps_python_module():
    text = RUNNER.read_text(encoding="utf-8")
    assert "src/rrsmr_ads/objective3_smr_integration/smr_fleet.py" in text
    assert '"$@"' in text


def test_build_unit_library_uses_reconciled_cases_and_commissioning_dates():
    module = load_module()
    assumptions = module.load_assumptions(Path("config/smr_assumptions.csv"))

    timestamps = pd.DatetimeIndex(
        [
            pd.Timestamp("2035-01-01 00:00:00", tz="UTC"),
            pd.Timestamp("2035-01-19 00:00:00", tz="UTC"),
            pd.Timestamp("2036-01-19 00:00:00", tz="UTC"),
            pd.Timestamp("2037-01-19 00:00:00", tz="UTC"),
        ],
        name="timestamp_utc",
    )

    library = module.build_unit_library(assumptions, timestamps)

    assert sorted(library["smr_case"].unique()) == [
        "simultaneous_commissioning",
        "staggered_commissioning",
    ]
    assert sorted(library["unit_id"].unique()) == ["unit_1", "unit_2", "unit_3"]

    jan_19_2035 = library[
        library["timestamp_utc"].eq(pd.Timestamp("2035-01-19 00:00:00", tz="UTC"))
    ]

    staggered = jan_19_2035[
        jan_19_2035["smr_case"].eq("staggered_commissioning")
    ].set_index("unit_id")
    simultaneous = jan_19_2035[
        jan_19_2035["smr_case"].eq("simultaneous_commissioning")
    ].set_index("unit_id")

    assert staggered.loc["unit_1", "delivered_mw"] == 470.0
    assert staggered.loc["unit_2", "delivered_mw"] == 0.0
    assert staggered.loc["unit_3", "delivered_mw"] == 0.0

    assert simultaneous.loc["unit_1", "delivered_mw"] == 470.0
    assert simultaneous.loc["unit_2", "delivered_mw"] == 470.0
    assert simultaneous.loc["unit_3", "delivered_mw"] == 470.0


def test_build_fleet_scenarios_schema_and_keys():
    module = load_module()
    assumptions = module.load_assumptions(Path("config/smr_assumptions.csv"))
    timestamps = pd.date_range(
        "2035-01-19 00:00:00",
        periods=2,
        freq="h",
        tz="UTC",
        name="timestamp_utc",
    )

    library = module.build_unit_library(assumptions, timestamps)
    fleet = module.build_fleet_scenarios(library)

    required_columns = {
        "timestamp_utc",
        "fes_scenario",
        "smr_case",
        "year",
        "unit_1_mw",
        "unit_2_mw",
        "unit_3_mw",
        "total_fleet_mw",
        "unit1_delivered_mw",
        "unit2_delivered_mw",
        "unit3_delivered_mw",
        "smr_total_delivered_mw",
    }

    assert required_columns.issubset(fleet.columns)
    assert int(fleet.duplicated(["timestamp_utc", "fes_scenario", "smr_case"]).sum()) == 0
    assert sorted(fleet["fes_scenario"].unique()) == [
        "Electric Engagement",
        "Holistic Transition",
    ]

    assert (
        fleet["total_fleet_mw"]
        == fleet["unit_1_mw"] + fleet["unit_2_mw"] + fleet["unit_3_mw"]
    ).all()
    assert (fleet["smr_total_delivered_mw"] == fleet["total_fleet_mw"]).all()


def test_smr_fleet_runner_doc_records_output_contract():
    text = DOC.read_text(encoding="utf-8")

    required = [
        "scripts/run_objective3_smr_fleet.sh",
        "config/smr_assumptions.csv",
        "data/processed/objective3_smr_integration/smr_fleet_hourly_2030_2045",
        "rows: 841536",
        "rows: 561024",
        "staggered_commissioning",
        "simultaneous_commissioning",
        "Forced outages are applied deterministically",
    ]

    for item in required:
        assert item in text
