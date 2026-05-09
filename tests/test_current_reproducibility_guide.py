from pathlib import Path


GUIDE = Path("docs/reproducibility/current_reproducibility_guide.md")


def test_current_reproducibility_guide_exists():
    assert GUIDE.exists()


def test_current_reproducibility_guide_uses_plain_indented_commands():
    text = GUIDE.read_text(encoding="utf-8")
    assert "```" not in text


def test_current_reproducibility_guide_mentions_core_runners_and_restore_helper():
    text = GUIDE.read_text(encoding="utf-8")
    required = [
        "scripts/run_objective1_generation.sh",
        "scripts/run_objective2_demand.sh",
        "scripts/restore_objective1_preprocessing_inputs.sh",
        "config/paths.yaml",
        "scripts/audit_reproducible_outputs.py",
    ]
    for item in required:
        assert item in text


def test_current_reproducibility_guide_mentions_final_outputs_and_fingerprints():
    text = GUIDE.read_text(encoding="utf-8")
    required = [
        "data/processed/objective1_generation/generation_future_hourly_2030_2045",
        "data/processed/objective2_demand/demand_future_hourly_2030_2045",
        "841536",
        "a8feb70e7b7a27ec2c7087fed6486f133f109f949227429a4c2ca8a6a2073d99",
        "0ea51272fb868017b33a3d3feea15221693066f950027b8a78944cc545537bc8",
    ]
    for item in required:
        assert item in text


def test_current_reproducibility_guide_reflects_current_status():
    text = GUIDE.read_text(encoding="utf-8")
    required = [
        "A preprocessing runner is available for rebuilding or verifying shared cleaned preprocessing inputs",
        "Objective 3 is covered by repository-path runners",
        "audit helper now checks the main Objective 1, Objective 2, Objective 3, Owner 5 sensitivity, and figure-output contracts",
        "Do not claim Objective 3 outputs are byte-for-byte unchanged from the original local handoff",
    ]
    for item in required:
        assert item in text
