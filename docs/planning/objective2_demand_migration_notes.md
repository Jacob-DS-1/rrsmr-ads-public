# Objective 2 demand migration notes

## Scope

This migration brings the Objective 2 demand workflow into the repository as a curated notebook workflow.

Objective 2 produces future hourly GB demand for 2030-2045 using:

- historic NESO National Demand as the core demand series;
- HadUK-Grid historic daily weather;
- UKCP18 future daily climate members;
- FES ED1 annual demand anchors for `Holistic Transition` and `Electric Engagement`;
- historic hourly demand shape libraries for hourly disaggregation.

## Migrated notebooks

The migrated notebooks are stored in `notebooks/objective2_demand/`:

1. `task1_demand_model_training_data.ipynb`
2. `task2_weather_feature_engineering.ipynb`
3. `task3_demand_model_integration.ipynb`
4. `task4_ukcp18_member_selection.ipynb`
5. `task5_future_daily_climate_demand.ipynb`
6. `task6_fes_annual_anchoring.ipynb`
7. `task7_hourly_disaggregation.ipynb`
8. `task8_final_demand_qa_export.ipynb`

## Path convention

Notebook runtime inputs and generated outputs are repo-relative:

- preprocessing inputs: `data/processed/preprocessing/`
- Objective 2 generated data: `data/processed/objective2_demand/`
- Objective 2 local model/validation outputs: `outputs/objective2_demand/`

Generated Parquet files, trained model artefacts, and large local outputs are intentionally ignored by Git.

## Task 3 integration note

Task 3 demand modelling was integrated from parallel modelling work.

The integrated notebook preserves the model comparison workflow rather than only retaining the selected final model. It includes linear, GAM, random forest, and XGBoost comparison work, with the expanded-feature XGBoost model retained as the selected modelling approach.

Because Task 3 has been rationalised, downstream generated outputs should not be assumed byte-for-byte identical to previous local outputs until a parity or rerun validation check has been completed.
