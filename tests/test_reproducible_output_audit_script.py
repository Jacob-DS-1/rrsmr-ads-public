import importlib.util
import json
import subprocess
import sys
from pathlib import Path


SCRIPT = Path("scripts/audit_reproducible_outputs.py")


def load_module():
    spec = importlib.util.spec_from_file_location(
        "audit_reproducible_outputs", SCRIPT
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_audit_script_exists():
    assert SCRIPT.exists()


def test_audit_script_defines_objective1_and_objective2_contracts():
    module = load_module()

    objective1 = module.EXPECTED_DATASETS["objective1_generation"]
    objective2 = module.EXPECTED_DATASETS["objective2_demand"]

    assert objective1["expected_rows"] == 841536
    assert objective2["expected_rows"] == 841536

    assert objective1["unique_key"] == [
        "timestamp_utc",
        "year",
        "fes_scenario",
        "weather_year",
    ]
    assert objective2["unique_key"] == [
        "timestamp_utc",
        "year",
        "fes_scenario",
        "climate_member",
    ]


def test_audit_script_defines_objective3_contracts():
    module = load_module()

    expected_names = {
        "objective3_smr_fleet",
        "objective3_grid_master",
        "objective3_system_impact_hourly",
        "objective3_system_impact_annual",
        "objective3_system_impact_period",
        "objective3_owner5_hourly",
        "objective3_owner5_annual",
        "objective3_owner5_period",
        "objective3_owner5_figures",
    }

    assert expected_names.issubset(module.EXPECTED_DATASETS)

    assert module.EXPECTED_DATASETS["objective3_smr_fleet"]["expected_rows"] == 561024
    assert module.EXPECTED_DATASETS["objective3_grid_master"]["expected_rows"] == 5049216
    assert module.EXPECTED_DATASETS["objective3_system_impact_hourly"]["expected_rows"] == 5049216
    assert module.EXPECTED_DATASETS["objective3_system_impact_annual"]["expected_rows"] == 576
    assert module.EXPECTED_DATASETS["objective3_system_impact_period"]["expected_rows"] == 36
    assert module.EXPECTED_DATASETS["objective3_owner5_hourly"]["expected_rows"] == 3366144
    assert module.EXPECTED_DATASETS["objective3_owner5_annual"]["expected_rows"] == 384
    assert module.EXPECTED_DATASETS["objective3_owner5_period"]["expected_rows"] == 24
    assert module.EXPECTED_DATASETS["objective3_owner5_figures"]["expected_file_count"] == 5
    assert module.EXPECTED_DATASETS["objective3_owner5_hourly"]["expected_values"]["weather_year_role"] == [
        "average_wind",
        "low_wind",
    ]

    assert module.EXPECTED_DATASETS["objective3_smr_fleet"]["unique_key"] == [
        "timestamp_utc",
        "year",
        "fes_scenario",
        "smr_case",
    ]

    assert module.EXPECTED_DATASETS["objective3_grid_master"]["unique_key"] == [
        "timestamp_utc",
        "year",
        "fes_scenario",
        "climate_member",
        "weather_year_role",
        "smr_case",
    ]


def test_audit_script_records_reference_fingerprints():
    module = load_module()

    assert (
        module.EXPECTED_DATASETS["objective1_generation"]["reference_content_sha256"]
        == "a8feb70e7b7a27ec2c7087fed6486f133f109f949227429a4c2ca8a6a2073d99"
    )
    assert (
        module.EXPECTED_DATASETS["objective2_demand"]["reference_content_sha256"]
        == "0ea51272fb868017b33a3d3feea15221693066f950027b8a78944cc545537bc8"
    )


def test_audit_script_allows_missing_outputs_for_fresh_clone(tmp_path):
    module = load_module()

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--repo-root",
            str(tmp_path),
            "--allow-missing",
            "--json",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr

    payload = json.loads(result.stdout)
    assert payload["overall_status"] == "missing_allowed"
    assert {item["status"] for item in payload["results"]} == {"missing"}
    assert {item["name"] for item in payload["results"]} == set(module.EXPECTED_DATASETS)
