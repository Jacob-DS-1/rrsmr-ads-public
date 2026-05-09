# Decision: Objective 3 Rule-Based Balancing Approach

## Status

Accepted for Objective 3 scaffold.

## Context

Objective 3 must evaluate the system impact of adding 3 Rolls-Royce SMRs at Wylfa to the future GB electricity system.

A simple merge of demand, supply, and SMR output would show additional generation, but it would not explain how the rest of the simplified system responds. The model therefore needs an explicit residual-demand calculation and a simple balancing rule.

## Decision

Objective 3 will use a rule-based balancing layer after merging:

- Objective 2 future hourly demand
- Objective 1 future hourly generation
- Objective 3 SMR fleet hourly output

The model will treat the following as exogenous or profile-based supply:

- wind
- solar
- existing nuclear
- biomass
- hydro
- other
- imports baseline

Gas is treated as the simplified balancing source.

For each hour:

    residual_before_smr = demand - exogenous_supply - imports_net_baseline
    residual_after_smr = residual_before_smr - smr_total_delivered
    gas_needed_before = max(residual_before_smr, 0)
    gas_needed_after = max(residual_after_smr, 0)
    gas_displacement_proxy = gas_needed_before - gas_needed_after
    surplus_after_smr = max(-residual_after_smr, 0)

This creates a transparent proxy for:

- residual-demand reduction
- gas displacement
- surplus or curtailment risk
- low-renewables support from SMRs

## SMR cases

The scaffold uses two cases:

- `staggered_commissioning`: base case with phased Wylfa unit commissioning
- `simultaneous_commissioning`: sensitivity case with all units commissioned together

Each case includes three 470 MWe units.

## Consequences

This approach is intentionally simplified. It does not claim to be a full dispatch, market, network, or unit-commitment model.

The benefit is that it is explainable, testable, and aligned with the project aim: estimating the grid-system impact of adding 3 Wylfa SMRs under future demand and supply scenarios.

## Out of scope

This decision does not introduce:

- economic dispatch optimisation
- nodal/network constraints
- detailed outage modelling
- storage dispatch optimisation
- interconnector market modelling
- full power-system adequacy modelling

Those may be discussed as limitations or future work.
