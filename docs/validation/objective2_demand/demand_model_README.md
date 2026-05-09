# demand_model_README.md

## Purpose
This file documents the Task 3 daily climate-sensitive demand modelling workflow for Objective 2.

## Inputs
The following inputs were used:
- `demand_model_training_data.parquet`
- `weather_hist_features_daily.parquet`

These files were merged on `date` to create the final model-ready historic daily demand dataset.

## Merge and QA
The modelling dataset was created by joining the two input files on `date`. QA checks were carried out to confirm:
- expected date coverage
- no duplicate dates
- missing value counts by column
- valid merged schema

## Target variable
The primary modelling target was:
- `nd_daily_mwh`

## Feature selection logic
Exploratory analysis and correlation checks were used to guide feature selection.

Raw GB temperature variables (`tasmin_gb_c`, `tasmax_gb_c`, `tmean_gb_c`) were explored but not included in the primary model specification alongside `hdd` and `cdd`, because they were highly collinear with one another and with `hdd`. `HDD` and `CDD` were retained as the main weather variables because they provide a more demand-relevant representation of climate sensitivity.

`is_weekend` was not used alongside `day_of_week` in the primary model specification because weekend status is already embedded in day-of-week categories. `day_of_week` was retained as the richer weekly calendar variable.

## Additional engineered features
Two additional temporal features were engineered for XGBoost experiments:
- `time_index`: number of days since 2010-01-01
- `day_of_year`: calendar day number within the year

These were tested to assess whether they improved representation of long-run structural drift and within-year seasonal position.

## Models tested
The following models were tested:
1. Multiple Linear Regression
2. XGBoost

Three XGBoost feature variants were compared:
- Model 1: `time_index`, `month`, `day_of_week`, `is_holiday_gb_any`, `hdd`, `cdd`
- Model 2: `year`, `day_of_year`, `month`, `day_of_week`, `is_holiday_gb_any`, `hdd`, `cdd`
- Model 3: `time_index`, `day_of_year`, `month`, `day_of_week`, `is_holiday_gb_any`, `hdd`, `cdd`

## Validation approach
A time-based validation strategy was used.
- Development period: 2010–2023
- Final test period: 2024
- Cross-validation: `TimeSeriesSplit` with ordered folds and yearly-sized validation windows

## Evaluation metrics
The following metrics were used:
- MAE
- RMSE
- R²

Residual summaries and prediction plots were also reviewed.

## Final selected model
The selected XGBoost specification was Model 3:
- `time_index`
- `day_of_year`
- `month`
- `day_of_week`
- `is_holiday_gb_any`
- `hdd`
- `cdd`

This variant achieved the strongest out-of-sample test performance among the tested XGBoost variants.

## Output files
Main output:
- `demand_model_train_ready.parquet`
