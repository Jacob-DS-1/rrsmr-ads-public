# QA Checklist

Every cleaned dataset must document:

- Row count
- Date range
- Missing timestamp percentage
- Missing values by column
- Duplicate key count
- Timezone conversion method
- DST handling confirmation
- Unit conventions
- Aggregation method
- Source file or files
- Known exclusions
- Known limitations

## Required key checks

Time-series outputs must have no duplicate `timestamp_utc` keys unless additional scenario keys are present.

Scenario outputs must use the relevant full key:

| Dataset | Required key |
|---|---|
| Historic demand | `timestamp_utc` |
| Historic generation mix | `timestamp_utc` |
| Demand future hourly | `timestamp_utc`, `year`, `fes_scenario`, `climate_member` |
| Generation future hourly | `timestamp_utc`, `year`, `fes_scenario`, `weather_year_role` |
| SMR fleet hourly | `timestamp_utc`, `year`, `smr_case` |
| Grid master hourly | `timestamp_utc`, `year`, `fes_scenario`, `climate_member`, `weather_year_role`, `smr_case` |

## Required conventions

- Power: MW
- Energy: MWh, GWh or TWh, clearly labelled
- Temperature: degrees C
- Wind speed: m/s
- Solar radiation or resource: documented as J/m2 or converted equivalent
- Column naming: snake case
- Geography: GB only, excluding Northern Ireland
