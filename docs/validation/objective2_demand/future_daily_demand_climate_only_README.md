# future_daily_demand_climate_only_README.md

Purpose:
Generate climate-only future daily GB National Demand before FES annual anchoring.

Inputs:
- weather_future_features_daily.parquet
- ukcp18_member_selection.csv
- holiday_calendar_2010_2045.csv
- demand_model_train_ready.parquet or saved demand_model_xgb_model3.pkl

Model:
Final Task 3 XGBoost Model 3 feature set:
time_index, day_of_year, month, day_of_week, is_holiday_gb_any, hdd, cdd.

Notes:
- Only selected UKCP18 climate members are used.
- Outputs are daily and use date as the key.
- Demand is in MWh.
- Climate-only demand is not the final future demand level; Task 6 anchors annual totals to FES ED1.
