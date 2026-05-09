from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER = REPO_ROOT / "scripts" / "run_complete_model.sh"


RUNNER_NAMES = [
    "run_preprocessing.sh",
    "run_objective1_generation.sh",
    "run_objective2_demand.sh",
    "run_objective3_smr_fleet.sh",
    "run_objective3_integration.sh",
    "run_objective3_system_impact.sh",
    "run_objective3_final_qa.sh",
]


OBJECTIVE3_CLEAN_TARGETS = [
    "data/processed/objective3_smr_integration/smr_hourly_library_2030_2045",
    "data/processed/objective3_smr_integration/smr_fleet_hourly_2030_2045",
    "data/processed/objective3_smr_integration/smr_hourly_fleet_scenarios.parquet",
    "data/processed/objective3_smr_integration/grid_master_hourly_2030_2045",
    "data/processed/objective3_smr_integration/grid_master_hourly_2030_2045.parquet",
    "data/processed/objective3_smr_integration/grid_master_hourly_2030_2045_partitioned",
    "data/processed/objective3_smr_integration/system_impact_hourly_2030_2045",
    "data/processed/objective3_smr_integration/system_impact_metrics_hourly_2030_2045.parquet",
    "data/processed/objective3_smr_integration/system_impact_summary_annual_2030_2045.csv",
    "data/processed/objective3_smr_integration/system_impact_summary_period_2030_2045.csv",
    "outputs/objective3_smr_integration",
    "outputs/figures/objective3_smr_integration",
]


def make_fake_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    scripts_dir = repo / "scripts"
    scripts_dir.mkdir(parents=True)
    (repo / "config").mkdir()

    shutil.copy2(RUNNER, scripts_dir / "run_complete_model.sh")
    os.chmod(scripts_dir / "run_complete_model.sh", 0o755)

    stub = """#!/usr/bin/env bash
set -euo pipefail
: "${RRSMR_COMPLETE_RUN_LOG:?}"
echo "$(basename "$0") $*" >> "$RRSMR_COMPLETE_RUN_LOG"
"""

    for name in RUNNER_NAMES:
        path = scripts_dir / name
        path.write_text(stub)
        os.chmod(path, 0o755)

    audit = scripts_dir / "audit_reproducible_outputs.py"
    audit.write_text(
        """#!/usr/bin/env python3
from pathlib import Path
import os
log_path = Path(os.environ["RRSMR_COMPLETE_RUN_LOG"])
with log_path.open("a") as handle:
    handle.write("audit_reproducible_outputs.py\\n")
"""
    )
    os.chmod(audit, 0o755)

    return repo


def run_fake_complete_model(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    log_path = repo / "calls.log"
    env = os.environ.copy()
    env.pop("KERNEL_NAME", None)
    env["RRSMR_COMPLETE_RUN_LOG"] = str(log_path)

    return subprocess.run(
        ["bash", "scripts/run_complete_model.sh", *args],
        cwd=repo,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def read_calls(repo: Path) -> list[str]:
    return (repo / "calls.log").read_text().splitlines()


def test_complete_runner_orders_full_workflow_and_passes_preprocessing_args(tmp_path: Path) -> None:
    repo = make_fake_repo(tmp_path)

    result = run_fake_complete_model(
        repo,
        "--source-dir",
        "/source/raw",
        "--era5-source-dir",
        "/source/era5",
        "--kernel-name",
        "test-kernel",
    )

    assert result.returncode == 0, result.stderr

    assert read_calls(repo) == [
        "run_preprocessing.sh --source-dir /source/raw --era5-source-dir /source/era5 --kernel-name test-kernel",
        "run_objective1_generation.sh ",
        "run_objective2_demand.sh ",
        "run_objective3_smr_fleet.sh ",
        "run_objective3_integration.sh --skip-smr-fleet",
        "run_objective3_system_impact.sh ",
        "run_objective3_final_qa.sh ",
        "audit_reproducible_outputs.py",
    ]


def test_complete_runner_clean_skip_preprocessing_flags_and_objective3_cleanup(tmp_path: Path) -> None:
    repo = make_fake_repo(tmp_path)

    for relative in OBJECTIVE3_CLEAN_TARGETS:
        target = repo / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.suffix:
            target.write_text("generated")
        else:
            target.mkdir(parents=True, exist_ok=True)
            (target / "part.txt").write_text("generated")

    result = run_fake_complete_model(
        repo,
        "--skip-preprocessing",
        "--clean",
        "--kernel-name",
        "clean-kernel",
    )

    assert result.returncode == 0, result.stderr

    assert read_calls(repo) == [
        "run_objective1_generation.sh --clean",
        "run_objective2_demand.sh --clean",
        "run_objective3_smr_fleet.sh --clean",
        "run_objective3_integration.sh --skip-smr-fleet",
        "run_objective3_system_impact.sh --clean",
        "run_objective3_final_qa.sh --clean",
        "audit_reproducible_outputs.py",
    ]

    for relative in OBJECTIVE3_CLEAN_TARGETS:
        assert not (repo / relative).exists()


def test_complete_runner_requires_source_dir_unless_preprocessing_is_skipped(tmp_path: Path) -> None:
    repo = make_fake_repo(tmp_path)

    result = run_fake_complete_model(repo)

    assert result.returncode == 2
    assert "--source-dir" in result.stderr
