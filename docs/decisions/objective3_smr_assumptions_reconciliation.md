# Objective 3 SMR assumptions reconciliation

This decision note records the reconciled SMR assumptions used by the repo-runnable Objective 3 workflow.

## Current assumptions

| unit_id | smr_case | nameplate_mwe | net_delivery_factor | commissioning_date | planned_outage_window_days | planned_outage_frequency_months | forced_outage_rate |
|---|---|---:|---:|---|---:|---:|---:|
| unit_1 | staggered_commissioning | 470 | 1.0 | 2035-01-01 | 18 | 24 | 0.02 |
| unit_2 | staggered_commissioning | 470 | 1.0 | 2036-01-01 | 18 | 24 | 0.02 |
| unit_3 | staggered_commissioning | 470 | 1.0 | 2037-01-01 | 18 | 24 | 0.02 |
| unit_1 | simultaneous_commissioning | 470 | 1.0 | 2035-01-01 | 18 | 24 | 0.02 |
| unit_2 | simultaneous_commissioning | 470 | 1.0 | 2035-01-01 | 18 | 24 | 0.02 |
| unit_3 | simultaneous_commissioning | 470 | 1.0 | 2035-01-01 | 18 | 24 | 0.02 |

## Decision

The repo version no longer treats `net_delivery_factor = 0.9` as a permanent output derating.

Instead:

- each SMR delivers 470 MW when online;
- planned outages are represented explicitly as 18-day refuelling outages every 24 months;
- forced outages are represented explicitly as a deterministic 2% loss of otherwise available hours;
- realised capacity factor is calculated from the generated hourly output.

## Rationale

The previous 0.9 net-delivery factor was a conservative simplification, but it mixed together online electrical output, planned outages, forced outages, and capacity factor. The revised approach separates those assumptions, making the SMR fleet model easier to audit and explain.

The previous 5% forced-outage value is better treated as a conservative stress assumption. In the base case, combining a 5% forced outage with planned refuelling outages would push realised availability materially below the high-availability assumption used for the central SMR analysis.

## Current validation result

After regenerating the SMR fleet with these assumptions, the generated fleet reaches:

- maximum fleet output: 1410 MW;
- realised post-commissioning unit capacity factors: approximately 95.3% to 95.6%;
- duplicate fleet keys: 0.

Downstream Objective 3 metrics should therefore be interpreted as the revised high-availability base case, not as a reproduction of the earlier 0.9-derated local handoff.
