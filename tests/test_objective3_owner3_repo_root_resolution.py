import importlib.util
import sys
from pathlib import Path


SCRIPT = Path("src/rrsmr_ads/objective3_smr_integration/owner3_data_integration.py")
REPO_ROOT = Path.cwd()

def expected_generated_path(base_path: Path) -> Path:
    """Match Owner 3 generated-output resolution before or after workflow execution."""
    parquet_path = base_path.with_suffix(".parquet")
    return parquet_path if parquet_path.exists() else base_path



def load_module():
    spec = importlib.util.spec_from_file_location("owner3_data_integration", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_owner3_resolves_repo_root_from_config_paths_yaml():
    module = load_module()

    assert module.resolve_repo_root() == REPO_ROOT


def test_owner3_resolve_paths_stays_inside_repository():
    module = load_module()
    paths = module.resolve_paths()

    expected_processed = REPO_ROOT / "data" / "processed" / "objective3_smr_integration"

    assert paths.smr_fleet_expected == expected_processed / "smr_fleet_hourly_2030_2045"
    assert paths.smr_fleet_extracted == expected_processed / "smr_hourly_fleet_scenarios.parquet"
    assert paths.outputs_dir == expected_processed
    assert paths.team_folder == REPO_ROOT


def test_owner3_source_does_not_use_brittle_parent_index_for_repo_root():
    text = SCRIPT.read_text(encoding="utf-8")

    assert "code_dir.parents[3]" not in text
    assert "Path(__file__).resolve().parents[4]" not in text
    assert "Path(__file__).resolve().parents[3]" not in text
    assert "config" in text
    assert "paths.yaml" in text

def test_owner3_resolve_paths_finds_current_objective1_and_objective2_parquet_outputs():
    module = load_module()
    paths = module.resolve_paths()

    obj1_contract_path = (
        REPO_ROOT
        / "data"
        / "processed"
        / "objective1_generation"
        / "generation_future_hourly_2030_2045"
    )
    obj2_contract_path = (
        REPO_ROOT
        / "data"
        / "processed"
        / "objective2_demand"
        / "demand_future_hourly_2030_2045"
    )

    assert paths.obj1_supply == expected_generated_path(obj1_contract_path)
    assert paths.obj2_demand == expected_generated_path(obj2_contract_path)
