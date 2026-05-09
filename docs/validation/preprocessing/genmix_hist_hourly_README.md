# Historic GB Generation Mix

Data processing pipeline for GB (Great Britain) electricity generation mix. Converts NESO half-hourly data into cleaned hourly datasets and profile libraries.

## Overview

- **Source**: NESO (National Grid ESO) half-hourly generation mix data via CKAN (`data/df_fuel_ckan.csv`)
- **Date range**: 2010-01-01 to 2024-12-31
- **Resolution**: Hourly (aggregated from half-hourly using mean)
- **Unit**: MW (power)

## Directory Structure

```
Historic GB generation mix/
├── data/          # Raw input data
│   └── df_fuel_ckan.csv
├── scripts/       # Processing scripts
│   └── process_genmix.py
├── output/        # Generated outputs
│   ├── genmix_hist_hourly.parquet
│   ├── genmix_profile_library.parquet
│   └── genmix_taxonomy_map.csv
└── README.md
```

## Requirements

- Python 3.8+
- pandas
- pyarrow (for Parquet I/O)

```bash
pip install pandas pyarrow
```

## How to Run

From the project root directory:

```bash
python scripts/process_genmix.py
```

Output files are written to the `output/` directory.

## Processing Pipeline

1. **Load** — Read `data/df_fuel_ckan.csv`
2. **Timezone** — Convert `DATETIME` to UTC (`timestamp_utc`), handling DST safely via `pd.to_datetime(..., utc=True)`
3. **Date filter** — Keep only records from 2010-01-01 to 2024-12-31
4. **Wind aggregation** — Create `wind_total_mw` = `WIND` + `WIND_EMB` (transmission + embedded wind)
5. **Column mapping** — Rename and keep only technology columns; all columns use snake_case
6. **Resolution** — Aggregate half-hourly to hourly using `.mean()` (power in MW, so mean is correct; not `.sum()`)
7. **Output** — Generate three deliverable files

## Output Deliverables

| File | Description |
|------|--------------|
| `genmix_hist_hourly.parquet` | Cleaned hourly time series with `timestamp_utc` and all technology columns |
| `genmix_profile_library.parquet` | Long-format profile library: p10/p50/p90 percentiles by `tech`, `month`, `day_type` (weekday/weekend), `hour` |
| `genmix_taxonomy_map.csv` | Cross-source technology taxonomy mapping NESO columns and FES categories to the agreed model-side tech categories |

## Technology Columns

| Internal name | Description |
|---------------|-------------|
| `gas_mw` | Natural gas generation |
| `coal_mw` | Coal generation |
| `nuclear_mw` | Nuclear generation |
| `wind_total_mw` | Total wind (transmission + embedded) |
| `solar_mw` | Solar generation |
| `hydro_mw` | Hydro generation |
| `biomass_mw` | Biomass generation |
| `other_mw` | Other generation |
| `storage_net_mw` | Storage net (see sign convention below) |
| `imports_net_mw` | Imports net (see sign convention below) |

## Taxonomy Map Schema

`genmix_taxonomy_map.csv` contains:

| Column | Meaning |
|---|---|
| `tech` | Agreed model-side technology category used for joins/checks |
| `model_column` | Corresponding model output column where applicable |
| `source_system` | Source dataset family, e.g. NESO or FES |
| `source_name` | Source table/file name |
| `source_column_or_category` | Original source column or category name |
| `notes` | Inclusion rules, aggregation notes, or sign conventions |

The taxonomy deliberately includes both NESO historic generation columns and FES supply categories so that Person 3’s `fes_supply_annual_2030_2045.parquet` can align with the same technology definitions.

## Sign Conventions

- **storage_net_mw**: Positive = generation/discharge (adding to grid), Negative = pumping/charge (consuming from grid)
- **imports_net_mw**: Positive = importing electricity into GB, Negative = exporting electricity from GB

## Expected QA Metrics

After running the script, the hourly dataset should have:

| Metric | Value |
|--------|-------|
| Row count | ~131,500 |
| Date range | 2010-01-01 00:00:00 UTC to 2024-12-31 23:00:00 UTC |
| Missing values | 0 |
