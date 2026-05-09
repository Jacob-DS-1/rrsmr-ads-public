import importlib.util
import sys
from pathlib import Path

import pandas as pd


SCRIPT = Path("src/rrsmr_ads/objective3_smr_integration/owner3_data_integration.py")


def load_module():
    spec = importlib.util.spec_from_file_location("owner3_data_integration", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_owner3_normalises_smr_fleet_with_legacy_and_canonical_columns():
    module = load_module()

    smr = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(["2035-01-19 00:00:00"], utc=True),
            "fes_scenario": ["Electric Engagement"],
            "smr_case": ["staggered_commissioning"],
            "year": [2035],
            "unit_1_mw": [423.0],
            "unit_2_mw": [0.0],
            "unit_3_mw": [0.0],
            "total_fleet_mw": [423.0],
            "unit1_delivered_mw": [423.0],
            "unit2_delivered_mw": [0.0],
            "unit3_delivered_mw": [0.0],
            "smr_total_delivered_mw": [423.0],
        }
    )

    normalised = module.normalise_smr_fleet_schema(smr)

    assert "unit_1_mw" not in normalised.columns
    assert "total_fleet_mw" not in normalised.columns
    assert list(normalised.columns).count("smr_total_delivered_mw") == 1
    assert normalised["smr_total_delivered_mw"].iloc[0] == 423.0


def test_owner3_normalises_smr_fleet_from_legacy_only_columns():
    module = load_module()

    smr = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(["2035-01-19 00:00:00"], utc=True),
            "fes_scenario": ["Electric Engagement"],
            "smr_case": ["simultaneous_commissioning"],
            "year": [2035],
            "unit_1_mw": [423.0],
            "unit_2_mw": [423.0],
            "unit_3_mw": [423.0],
            "total_fleet_mw": [1269.0],
        }
    )

    normalised = module.normalise_smr_fleet_schema(smr)

    assert normalised["unit1_delivered_mw"].iloc[0] == 423.0
    assert normalised["unit2_delivered_mw"].iloc[0] == 423.0
    assert normalised["unit3_delivered_mw"].iloc[0] == 423.0
    assert normalised["smr_total_delivered_mw"].iloc[0] == 1269.0
    assert "total_fleet_mw" not in normalised.columns
