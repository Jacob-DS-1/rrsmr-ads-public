from pathlib import Path


SCRIPT = Path("src/rrsmr_ads/objective3_smr_integration/owner5_visualisations_final_qa.py")
RUNNER = Path("scripts/run_objective3_final_qa.sh")
DOC = Path("docs/validation/objective3_smr_integration/owner5_final_qa_runner.md")


def test_owner5_uses_repo_generated_part4_outputs() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert 'owner4_hourly=processed_obj3 / "system_impact_hourly_2030_2045"' in text
    assert 'owner4_annual=processed_obj3 / "system_impact_summary_annual_2030_2045.csv"' in text
    assert 'owner4_period=processed_obj3 / "system_impact_summary_period_2030_2045.csv"' in text

    forbidden = [
        "owner4_outputs.zip",
        "extract_owner4_zip",
        "local_owner4_handoff",
        "compressed system impact metrics handoff",
        'repo_root / "outputs" / "tables"',
    ]
    for needle in forbidden:
        assert needle not in text


def test_owner5_filters_repo_part4_hourly_to_base_weather_for_reconciliation() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert '"weather_year_role"' in text
    assert "owner4_hourly_base" in text
    assert 'owner4_hourly["weather_year_role"].eq(BASE_WEATHER_ROLE)' in text
    assert "derive_owner4_low_wind_threshold" in text


def test_owner5_final_qa_runner_documents_ordered_inputs() -> None:
    runner = RUNNER.read_text(encoding="utf-8")
    docs = DOC.read_text(encoding="utf-8")

    for expected in [
        "system_impact_hourly_2030_2045",
        "system_impact_summary_annual_2030_2045.csv",
        "system_impact_summary_period_2030_2045.csv",
    ]:
        assert expected in runner
        assert expected in docs

    assert "owner5_visualisations_final_qa.py --build" in runner
    assert "scripts/run_objective3_final_qa.sh --clean" in docs


def test_owner5_resolves_repo_root_from_config_paths() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "def resolve_repo_root(" in text
    assert '"config" / "paths.yaml"' in text
    assert "repo_root = resolve_repo_root(code_dir)" in text
    assert "code_dir.parents[3]" not in text


def test_owner5_does_not_force_fastparquet_engine() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert 'engine="fastparquet"' not in text
    assert "return pd.read_parquet(path, columns=columns)" in text


def test_owner5_alignment_comparison_uses_weather_role_key() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    start = text.index("def compare_owner4_alignment")
    block = text[start : start + 2500]

    assert '"weather_year_role"' in block
    assert '"smr_case"' in block
    assert "validate=\"one_to_one\"" in block


def test_owner5_filters_grid_master_to_base_smr_case_before_sensitivity_package() -> None:
    text = SCRIPT.read_text(encoding="utf-8")
    start = text.index("def build_owner5_metrics")
    block = text[start : start + 2500]

    assert 'master["smr_case"].eq(BASE_SMR_CASE)' in block
    assert "staggered = master.copy()" in block


def test_owner5_rel_prefers_repo_relative_paths_for_output_artifacts() -> None:
    text = SCRIPT.read_text(encoding="utf-8")

    assert "for candidate in [resolved_base, *resolved_base.parents]" in text
    assert "return str(path)" in text
