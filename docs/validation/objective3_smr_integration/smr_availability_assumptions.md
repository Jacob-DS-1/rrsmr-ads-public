# SMR availability assumptions

This note documents the SMR fleet availability assumptions used by the Objective 3 repo workflow.

## Modelling choice

The SMR fleet model treats the Rolls-Royce SMR unit rating as the online delivered electrical capacity, rather than applying a permanent capacity derating. The hourly fleet output is then reduced by explicit outage scheduling.

Current base assumptions:

- Unit electrical capacity: 470 MW.
- Net delivery factor: 1.0.
- Planned refuelling outage: 18 days.
- Planned outage frequency: every 24 months.
- Forced outage rate: 2% of otherwise available hours.
- Base commissioning case: staggered commissioning of three Wylfa SMRs.
- Sensitivity commissioning case: simultaneous commissioning of all three Wylfa SMRs.

## Rationale

The previous `net_delivery_factor = 0.9` acted as a conservative derating shortcut. That made the model easy to implement but mixed together several different concepts: net electrical output, planned outages, forced outages, and capacity factor.

The revised approach keeps these assumptions separate:

1. The unit delivers 470 MW when online.
2. Planned refuelling outages are scheduled explicitly.
3. Forced outage loss is represented explicitly.
4. Realised capacity factor is calculated from the hourly output.

The previous 5% forced-outage value is not used as the base-case value because, combined with planned refuelling outages, it would make the base case materially more conservative than the high-availability assumption used for the central analysis. It is more suitable as a conservative stress sensitivity.

## Interpretation

The base case is not forced to equal a capacity-factor target hour by hour. Instead, the generated hourly output is checked after each run to confirm that realised post-commissioning capacity factors are consistent with the intended high-availability nuclear baseload assumption.
