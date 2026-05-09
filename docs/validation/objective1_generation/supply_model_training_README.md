# Supply model training dataset (Task 1)

Output file: `supply_model_training_ready.parquet`

## Row count

- **Rows:** 131,496

## Time coverage

- **timestamp_utc min:** 2010-01-01 00:00:00+00:00
- **timestamp_utc max:** 2024-12-31 23:00:00+00:00
- **Calendar years present:** [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]

## Source inputs (read-only)

- `data/processed/preprocessing/genmix_hist_hourly.parquet`
- `data/processed/preprocessing/era5_resource_hourly_gb_2010_2024.parquet`
- `data/processed/preprocessing/dukes_capacity_hist_2010_2024.parquet`
- `data/processed/preprocessing/dukes_loadfactor_hist_2010_2024.parquet`
- `data/processed/preprocessing/genmix_profile_library.parquet`

## DUKES capacity mapping

- **wind_capacity_mw:** DUKES 6.2, `Wind:` total (MW) — the smaller of the
  large `Wind:` values per year (installed-capacity block; excludes generation
  and share rows).
- **solar_capacity_mw:** DUKES 6.2, `Solar photovoltaics` — maximum MW per year
  across duplicate blocks (excludes tiny share rows).

## Columns (training table)

- `timestamp_utc`, `year`, `wind_total_mw`, `solar_mw`, `wind_speed_100m_ms`,
  `ssrd_j_m2`, `wind_capacity_mw`, `solar_capacity_mw`, `wind_cf`, `solar_cf`

## Cleaning applied

- Inner join genmix and ERA5 on `timestamp_utc`; left capacity on `year`,
  keep only rows with positive wind and solar capacity denominators.
- Drop rows with negative generation; drop CF outside [0, 1]; drop NaNs in
  model columns.

## Module-3 deliverables also produced by this script

| File | Description |
|------|-------------|
| `era5_resource_hourly.parquet` | GB-mean hourly ERA5 wind speed (m/s) and SSRD (J/m^2) for the **three representative weather years** (2010 low, 2014 average, 2015 high). |
| `tech_year_calibration.csv` | DUKES annual capacity, observed genmix-derived generation (MWh/TWh), DUKES load factor (%), and implied load factor — per (tech, year). Used by QA for validation. |
| `genmix_profile_library.parquet` | Historic technology profile library (`tech x month x day_type x hour`, p10/p50/p90 MW), with `is_weekend` augmented for the Baseline + Adjustment join. |
| `genmix_taxonomy_map.csv` | Explicit raw NESO source -> internal name -> model column mapping with each tech's role (renewable/dispatchable/storage/imports/reference) and FES anchor target. |
