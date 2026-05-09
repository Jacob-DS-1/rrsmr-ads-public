import csv
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
ASSUMPTIONS_PATH = REPO_ROOT / "config" / "smr_assumptions.csv"


def read_rows() -> list[dict[str, str]]:
    with ASSUMPTIONS_PATH.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_objective3_smr_assumptions_match_reconciled_decision() -> None:
    rows = read_rows()

    expected = {
        ("staggered_commissioning", "unit_1"): "2035-01-01",
        ("staggered_commissioning", "unit_2"): "2036-01-01",
        ("staggered_commissioning", "unit_3"): "2037-01-01",
        ("simultaneous_commissioning", "unit_1"): "2035-01-01",
        ("simultaneous_commissioning", "unit_2"): "2035-01-01",
        ("simultaneous_commissioning", "unit_3"): "2035-01-01",
    }

    actual = {
        (row["smr_case"], row["unit_id"]): row["commissioning_date"]
        for row in rows
    }

    assert actual == expected

    for row in rows:
        assert row["nameplate_mwe"] == "470"
        assert row["net_delivery_factor"] == "1.0"
        assert row["planned_outage_window"] == "18"
        assert row["planned_outage_frequency_months"] == "24"
        assert row["forced_outage_rate"] == "0.02"


def test_objective3_smr_assumptions_are_not_placeholder_values() -> None:
    text = ASSUMPTIONS_PATH.read_text(encoding="utf-8")

    assert "Placeholder assumption" not in text
    assert "Placeholder sensitivity case" not in text
    assert "planned_outage_frequency_months" in text
    assert ",1.0," in text
    assert ",0.02," in text
    assert ",0.0," not in text
