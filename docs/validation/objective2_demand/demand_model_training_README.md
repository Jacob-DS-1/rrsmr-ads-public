
# demand_model_training_README

## Purpose
This README documents how `demand_model_training_data.parquet` was created for Objective 2 - Task 1.

This dataset is the final historic daily model input table used to train the climate-sensitive daily demand model.

Primary target: `nd_daily_mwh`
Granularity: `one row per `date`
Coverage: `2010-01-01` to `2024-12-31`
Main output file: `demand_model_training_data.parquet`
Final output shape: `5479 rows, 15 columns`

---

## Input files

### 1. `demand_hist_daily.parquet`: Historic daily demand input

Columns available:
- `date`
- `nd_daily_mwh`
- `nd_mean_mw`
- `tsd_daily_mwh`


### 2. `weather_hist_daily.parquet`: Historic daily weather input in long format

Columns:
- `date`
- `region`
- `tasmin_c`
- `tasmax_c`
- `tmean_c`

Expected regions:
- `eng_wales`
- `scotland`

### 3. `holiday_calendar_2010_2045.csv`: Holiday calendar input

Columns:
- `date`
- `is_holiday_eng_wales`
- `is_holiday_scotland`
- `is_holiday_gb_any`

---

## Initial data exploration and QA checks

### Initial dataset shapes:
- `demand_hist_daily.parquet`: 5479 rows x 4 columns
- `weather_hist_daily.parquet`: 10958 rows x 5 columns
- `holiday_calendar_2010_2045.csv`: 13149 rows x 4 columns


### Missing value results: 0 missing values in all three inputs

### Duplicate check results: 0 full-row duplicates in all three inputs

### Date coverage check results:
- demand date range: 2010-01-01 to 2024-12-31
- weather date range: 2010-01-01 to 2024-12-31
- holiday date range: 2010-01-01 to 2045-12-31


---


## Weather reshaping logic

### Why reshaping was needed
The weather input file is in long format, meaning there is one row for each `date`, `region`

Example structure before reshape:
- 2010-01-01, eng_wales, ...
- 2010-01-01, scotland, ...
- 2010-01-02, eng_wales, ...
- 2010-01-02, scotland, ...

However, the final Task 1 modelling dataset needs: one row per `date`

### Reshaping method used
A pivot was applied using:
- `index = "date"`
- `columns = "region"`
- `values = ["tasmin_c", "tasmax_c", "tmean_c"]`

This produced a wide table where each date has separate weather columns for each region.


---

## Calendar feature creation

- A complete daily calendar table was created using:`pd.date_range("2010-01-01", "2024-12-31", freq="D")`
- This calendar table was used as the merge base to ensure full daily coverage.
- The following calendar features were derived from `date`:`year`,`month`, `day_of_week`, `is_weekend`

Definition of day_of_week: Monday = 0, Tuesday = 1, Wednesday = 2, Thursday = 3, Friday = 4, Saturday = 5, Sunday = 6

---

## Merge logic

A complete daily calendar table was used as the starting base table. The final dataset was built by merging in this order:

### 1. Calendar + demand
Merged on: `date`

Added: `nd_daily_mwh`

### 2. Add holiday flags
Merged on: `date`

Added:`is_holiday_eng_wales`, `is_holiday_scotland`, `is_holiday_gb_any`

### 3. Add reshaped weather table
Merged on: `date`

Added: `tasmin_eng_wales_c`, `tasmax_eng_wales_c`, `tmean_eng_wales_c`, `tasmin_scotland_c`, `tasmax_scotland_c`, `tmean_scotland_c`

### Join type
The joins were done using `how="left"` from the calendar base table.

---

