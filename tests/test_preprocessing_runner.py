from pathlib import Path

from rrsmr_ads.preprocessing import runner


def test_preprocessing_stage_order_is_stable():
    assert [stage.name for stage in runner.STAGES] == [
        "holiday_calendar",
        "hourly_calendar",
        "historic_demand",
        "generation_mix",
        "dukes_reference_tables",
        "fes_annual_tables",
        "weather_tables",
        "era5_resource",
    ]


def test_canonical_outputs_include_shared_preprocessing_contract():
    outputs = set(runner.CANONICAL_OUTPUTS)

    expected = {
        "demand_hist_hourly.parquet",
        "demand_hist_daily.parquet",
        "demand_hourly_shape_library.parquet",
        "genmix_hist_hourly.parquet",
        "genmix_profile_library.parquet",
        "fes_demand_annual_2030_2045.parquet",
        "fes_supply_annual_2030_2045.parquet",
        "weather_hist_daily.parquet",
        "weather_future_daily_ukcp18.parquet",
        "era5_resource_hourly_gb_2010_2024.parquet",
        "calendar_hourly_2010_2045.parquet",
        "dukes_capacity_hist_2010_2024.parquet",
        "dukes_loadfactor_hist_2010_2024.parquet",
        "interconnector_annual_hist_2010_2024.parquet",
        "holiday_calendar_2010_2045.csv",
    }

    assert expected.issubset(outputs)


def test_missing_input_report_is_specific(tmp_path):
    missing = runner.collect_missing_inputs(tmp_path)

    assert "historic_demand" in missing
    assert "era5_resource" in missing

    message = runner.format_missing_inputs(tmp_path, missing)
    assert "Missing preprocessing source inputs" in message
    assert "NESO historic demand annual CSVs" in message
    assert "ERA5 surface solar radiation NetCDFs" in message
    assert "scripts/run_preprocessing.sh --source-dir /path/to/source/raw" in message


def test_shell_wrapper_uses_strict_mode_and_module_entrypoint():
    wrapper = Path("scripts/run_preprocessing.sh").read_text()

    assert "set -euo pipefail" in wrapper
    assert "python -m rrsmr_ads.preprocessing.runner" in wrapper


def test_generation_mix_script_accepts_runner_environment_overrides():
    script = Path("scripts/process_genmix.py").read_text()

    assert "RRSMR_GENMIX_INPUT_CSV" in script
    assert "RRSMR_GENMIX_OUTPUT_DIR" in script

def test_era5_source_dir_can_be_separate(tmp_path):
    main_source = tmp_path / "main_raw"
    era5_source = tmp_path / "era5"
    main_source.mkdir()
    era5_source.mkdir()

    for year_chunk in [
        "2010_2011",
        "2012_2013",
        "2014_2015",
        "2016_2017",
        "2018_2019",
        "2020_2021",
        "2022_2023",
        "2024",
    ]:
        (era5_source / f"100m_u_wind_{year_chunk}.nc").write_text("")
        (era5_source / f"100m_v_wind_{year_chunk}.nc").write_text("")
        (era5_source / f"ssrd_{year_chunk}.nc").write_text("")

    (era5_source / "repd.xlsx").write_text("")

    missing_without_era5_dir = runner.collect_missing_inputs(main_source)
    missing_with_era5_dir = runner.collect_missing_inputs(main_source, era5_source)

    assert "era5_resource" in missing_without_era5_dir
    assert "era5_resource" not in missing_with_era5_dir


def test_notebook_patcher_removes_optional_seaborn_import():
    import nbformat

    nb = nbformat.v4.new_notebook()
    nb.cells = [
        nbformat.v4.new_code_cell(
            "import pandas as pd\n"
            "import seaborn as sns\n"
            "sns.set(style='whitegrid')\n"
            "%load_ext watermark\n"
            "%watermark --python --packages numpy,pandas\n"
            "df['hour_utc'] = df['timestamp_utc'].dt.floor(\"H\")\n"
            "rng = pd.date_range('2010-01-01', periods=2, freq=\"H\")\n"
            "        .apply(align_360day_year)\n"
            "print('ok')"
        )
    ]

    stage = next(stage for stage in runner.STAGES if stage.name == "holiday_calendar")
    patched = runner._patch_notebook_for_stage(nb, stage)

    source = patched.cells[0]["source"]
    assert "import seaborn as sns" not in source
    assert "sns.set" not in source
    assert "%load_ext watermark" not in source
    assert "%watermark" not in source
    assert "Optional plotting dependency removed" in source
    assert "Optional notebook metadata dependency removed" in source
    assert '.dt.floor("H")' not in source
    assert 'freq="H"' not in source
    assert '.dt.floor("h")' in source
    assert 'freq="h"' in source
    assert ".apply(align_360day_year)" not in source
    assert "group.assign(region=group.name[0], climate_member=group.name[1], year=group.name[2])" in source
    assert "import pandas as pd" in source
