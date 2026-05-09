# demand_future_hourly_2030_2045_README.md

Purpose:
Final Objective 2 future hourly GB demand output for 2030-2045.

Output:
demand_future_hourly_2030_2045

Columns:
- timestamp_utc: hourly UTC timestamp
- year: calendar year
- fes_scenario: FES ED1 scenario, restricted to Holistic Transition and Electric Engagement
- climate_member: selected UKCP18 member
- demand_mw: hourly average GB National Demand in MW

Method:
1. Generated climate-only daily demand using the selected Task 3 XGBoost model.
2. Filtered to selected UKCP18 climate members only.
3. Anchored each year to FES ED1 annual demand totals.
4. Disaggregated daily MWh to hourly MW using the historic ND hourly shape library.
5. Reconciled hourly totals back to daily anchored demand and annual FES demand.

Units:
- Daily demand: MWh
- Annual FES demand: TWh, converted to MWh for scaling
- Hourly demand: MW, numerically equal to hourly MWh because each row is one hour

Boundary:
Great Britain only: England, Wales, and Scotland. Northern Ireland is excluded.

Core demand definition:
NESO National Demand.
