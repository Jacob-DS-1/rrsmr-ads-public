# Objective 1 runtime validation QA

This note records a clean local rerun of the Objective 1 generation workflow from the repository scripts.

## Runtime context

- Branch base: main
- Clean working tree before rerun: yes
- Python environment used: rrsmr-ads
- Objective 1 scripts executed in order:
  1. src/task1_prep_and_calibration.py
  2. src/task2_ml_training.py
  3. src/task3_baseline_and_weather_scaffold.py
  4. src/task4_weather_adjustment.py
  5. src/task5_fes_anchoring_and_export.py

## Upstream preprocessing prerequisite

Objective 1 currently depends on ignored preprocessing artifacts under:

- data/processed/preprocessing/

For this validation run, the supply-side preprocessing artifacts were restored locally from migrated owner evidence in:

- data/processed/preprocessing/

Required supply-side preprocessing inputs restored for the run:

- genmix_hist_hourly.parquet
- era5_resource_hourly_gb_2010_2024.parquet
- dukes_capacity_hist_2010_2024.parquet
- dukes_loadfactor_hist_2010_2024.parquet
- genmix_profile_library.parquet
- fes_supply_annual_2030_2045.parquet
- interconnector_annual_hist_2010_2024.parquet

This means Objective 1 is reproducible from repo scripts once preprocessing outputs exist locally, but the full raw-to-final workflow still needs a formal preprocessing runner or documented preprocessing command sequence.

## Final output

Primary output:

- data/processed/objective1_generation/generation_future_hourly_2030_2045

Legacy compatibility output:

- outputs/objective1_generation/generation_future_hourly_2030_2045

Both files were generated successfully.

## Output fingerprint

- rows: 841536
- date_min: 2030-01-01 00:00:00+00:00
- date_max: 2045-12-31 23:00:00+00:00
- years: 2030-2045
- fes_scenarios: Electric Engagement; Holistic Transition
- weather_year_roles: low_wind; average_wind; high_wind
- weather_years: 2010; 2014; 2015
- required column parse/null failures: 0
- missing values in required/model columns: 0
- negative values in MW columns: 0
- content_sha256: a8feb70e7b7a27ec2c7087fed6486f133f109f949227429a4c2ca8a6a2073d99

Columns:

- timestamp_utc
- year
- fes_scenario
- weather_year
- weather_year_role
- biomass_mw
- coal_reference_mw
- gas_reference_mw
- hydro_mw
- imports_net_baseline_mw
- nuclear_existing_mw
- other_mw
- solar_mw
- storage_net_mw
- wind_mw

## Key contract

Objective 1 includes three representative supply-weather variants per hour and FES scenario.

The correct unique generated-output key is:

- timestamp_utc + year + fes_scenario + weather_year

The semantic integration key used by Objective 3 is:

- timestamp_utc + year + fes_scenario + weather_year_role

The shorter key below is intentionally not unique:

- timestamp_utc + year + fes_scenario

Expected row count:

- 140256 hours x 2 FES scenarios x 3 weather years = 841536 rows

Observed duplicate-key checks:

- timestamp_utc + year + fes_scenario: 561024 duplicate rows
- timestamp_utc + year + fes_scenario + weather_year: 0 duplicate rows
- timestamp_utc + year + fes_scenario + weather_year + weather_year_role: 0 duplicate rows

## Test/hygiene result

After the clean rerun:

- pytest -q: 61 passed
- git status --short --untracked-files=all: clean
- forbidden tracked generated artifacts: none

## Known remaining reproducibility gap

Objective 1 is now validated as rerunnable from repository scripts after its preprocessing prerequisites are present. The remaining gap is to make the preprocessing layer itself reproducible from source notebooks/scripts or provide a documented restore/build command sequence for the ignored preprocessing artifacts.
