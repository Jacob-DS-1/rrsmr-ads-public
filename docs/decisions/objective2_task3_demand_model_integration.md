# Objective 2 Task 3 demand model integration decision

## Decision

Use a curated integrated Task 3 notebook as the canonical Objective 2 demand modelling notebook:

`notebooks/objective2_demand/task3_demand_model_integration.ipynb`

The selected final model family is the expanded-feature XGBoost model.

## Reason

Task 3 was originally completed through parallel modelling work:

- one stream compared multiple linear regression and gradient boosted modelling;
- one stream compared GAM and random forest approaches;
- later work aligned feature sets and tested expanded feature sets before final model selection.

Keeping every exploratory notebook in the repository would preserve history but make the canonical workflow harder to follow. The integrated notebook keeps the comparison workflow while removing unnecessary duplication.

## Reproducibility caveat

The integrated notebook should be validated against previous Task 3 outputs before claiming exact output parity.

Recommended checks:

- compare selected feature set;
- compare selected model family;
- compare validation split assumptions;
- compare MAE, RMSE, and R2 metrics;
- compare 2024 holdout predictions where available;
- compare row counts, date coverage, and columns for `demand_model_train_ready.parquet`;
- rerun Tasks 5-8 deliberately if Task 3 outputs change materially.

## Commit policy

Do not commit generated model artefacts such as `.pkl`, `.joblib`, or generated Parquet outputs. These belong in ignored local runtime paths.
