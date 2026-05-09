# Objective 2 runtime validation QA

## Scope

This document records a local runtime validation of the migrated Objective 2 demand notebooks.

The notebooks were run in order from `notebooks/objective2_demand/` using local runtime inputs in:

- `data/processed/preprocessing/`

Generated Objective 2 runtime outputs were written locally to ignored paths:

- `data/processed/objective2_demand/`
- `outputs/objective2_demand/`

Generated Parquet files and model artefacts were not committed.

## Runtime output check

Final output checked:

`data/processed/objective2_demand/demand_future_hourly_2030_2045`

Basic QA results:

| Check | Result |
|---|---:|
| Rows | 841,536 |
| Missing required columns | 0 |
| Date minimum | 2030-01-01 00:00:00+00:00 |
| Date maximum | 2045-12-31 23:00:00+00:00 |
| Years | 2030-2045 |
| FES scenarios | Electric Engagement; Holistic Transition |
| Climate members | member_06; member_12; member_13 |
| Duplicate `(timestamp_utc, fes_scenario, climate_member)` keys | 0 |
| Missing values in final required columns | 0 |
| Minimum demand_mw | 22,514.595896840812 |
| Maximum demand_mw | 140,693.71629999016 |
| Positive demand_mw values | True |

Required final columns present:

- `timestamp_utc`
- `year`
- `fes_scenario`
- `climate_member`
- `demand_mw`

## Notes

This is a runtime/schema validation check. It confirms that the migrated Objective 2 notebook workflow can run locally and produce the expected final hourly demand schema.

It does not by itself prove model parity with earlier pre-migration local outputs. Task 3 parity and downstream sensitivity checks remain a separate validation step.
