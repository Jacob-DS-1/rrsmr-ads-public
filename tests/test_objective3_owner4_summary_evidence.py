import csv
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
VALIDATION_DIR = REPO_ROOT / "docs" / "validation" / "objective3_smr_integration"

ANNUAL_PATH = VALIDATION_DIR / "owner4_system_impact_summary_annual_2030_2045.csv"
PERIOD_PATH = VALIDATION_DIR / "owner4_system_impact_summary_period_2030_2045.csv"
README_PATH = VALIDATION_DIR / "owner4_system_impact_metrics_README.md"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_owner4_summary_evidence_files_exist() -> None:
    assert ANNUAL_PATH.exists()
    assert PERIOD_PATH.exists()
    assert README_PATH.exists()


def test_owner4_annual_summary_schema_and_coverage() -> None:
    rows = read_csv(ANNUAL_PATH)

    assert len(rows) == 96
    assert set(rows[0]) == {
        "year",
        "fes_scenario",
        "climate_member",
        "weather_year_role",
        "smr_case",
        "annual_gas_displacement_twh",
        "surplus_hours_count",
        "low_wind_support_hours",
    }

    assert sorted({int(row["year"]) for row in rows}) == list(range(2030, 2046))
    assert {row["fes_scenario"] for row in rows} == {
        "Electric Engagement",
        "Holistic Transition",
    }
    assert {row["climate_member"] for row in rows} == {
        "member_06",
        "member_12",
        "member_13",
    }
    assert {row["weather_year_role"] for row in rows} == {"average_wind"}
    assert {row["smr_case"] for row in rows} == {"staggered_commissioning"}

    keys = {
        (
            row["year"],
            row["fes_scenario"],
            row["climate_member"],
            row["weather_year_role"],
            row["smr_case"],
        )
        for row in rows
    }
    assert len(keys) == len(rows)


def test_owner4_period_summary_schema_and_coverage() -> None:
    rows = read_csv(PERIOD_PATH)

    assert len(rows) == 2
    assert set(rows[0]) == {
        "fes_scenario",
        "smr_case",
        "cumulative_gas_displacement_twh",
        "average_residual_demand_reduction_mw",
    }

    assert {row["fes_scenario"] for row in rows} == {
        "Electric Engagement",
        "Holistic Transition",
    }
    assert {row["smr_case"] for row in rows} == {"staggered_commissioning"}


def test_owner4_summary_evidence_has_no_missing_required_values() -> None:
    for path in [ANNUAL_PATH, PERIOD_PATH]:
        for row in read_csv(path):
            for key, value in row.items():
                assert value != "", f"{path.name} has missing value in {key}"


def test_owner4_readme_documents_excluded_hourly_parquet() -> None:
    text = README_PATH.read_text(encoding="utf-8")

    assert "system_impact_metrics_hourly_2030_2045.parquet" in text
    assert "intentionally not committed" in text
    assert "No Owner 4 source script or notebook" in text
