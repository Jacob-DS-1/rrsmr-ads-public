# ukcp18_member_selection_note_README.md

## Purpose

This task identifies a small set of representative UKCP18 climate members to be used in downstream demand modelling. As the full ensemble contains multiple plausible future climate realisations, a reduced subset is selected to capture the range of temperature-driven demand outcomes while maintaining computational efficiency.

---

## Input data

Two input datasets were used:

- `weather_future_features_daily.parquet`  
  Contains daily GB-aggregated future weather variables for each UKCP18 climate member, including temperature (`tmean_gb_c`) and demand-relevant indicators (`hdd`, `cdd`).

- `ukcp18_member_lookup`  
  Provides mapping between `climate_member` identifiers and member metadata.

The analysis period was restricted to **2030–2045**, consistent with the task requirement.

---

## Data preparation and QA

The `date` column was parsed to datetime and filtered to the target period. Quality assurance checks were performed at the climate-member level to ensure:
- consistent row counts across members  
- no duplicate `(date, climate_member)` combinations  
- no missing values in key variables (`tmean_gb_c`, `hdd`, `cdd`)  
- complete temporal coverage for each member  

This ensures that all members are directly comparable.

---

## Feature engineering

Time-based features were derived to support aggregation:

- `year` enables annual grouping for HDD/CDD calculations  
- `month` supports seasonal classification  

Meteorological seasons were used:
- Winter: December–February  
- Summer: June–August  

This approach follows standard climatological practice and ensures consistent seasonal comparisons.

---

## Member-level summarisation

Each climate member was summarised using temperature and demand-relevant metrics:

### Annual temperature
- `mean_tmean_gb_c`: primary metric used for ranking members  
- `median_tmean_gb_c`: included as a robustness check against extreme values  

### Seasonal temperature
- `winter_mean_tmean_gb_c`: captures cold-period conditions relevant to heating demand  
- `summer_mean_tmean_gb_c`: captures warm-period conditions relevant to cooling demand  

Including seasonal metrics ensures that members are not only distinguished by annual averages but also by their seasonal characteristics.

### HDD/CDD aggregation
Daily `hdd` and `cdd` values were:
1. summed within each `(climate_member, year)` to obtain annual totals  
2. averaged across all years to compute `mean_annual_hdd` and `mean_annual_cdd`  

This two-step approach aligns with standard practice, as HDD and CDD are typically interpreted at the annual level.

---

## Ranking and selection logic

Climate members were ranked from cooler to warmer based on `mean_tmean_gb_c`, with `member_number` used as a secondary tie-breaker.

To represent the full range of plausible future climates, three members were selected:

- **Cool member**: lowest mean temperature  
- **Median member**: central member in the ranked distribution  
- **Warm member**: highest mean temperature  

This selection captures lower, central, and upper bounds of temperature-driven demand scenarios, ensuring that downstream modelling reflects uncertainty in future climate conditions.

---

## Outputs

Two output files were generated:

### 1. `ukcp18_member_summary_2030_2045.csv`
Contains one row per climate member with:
- temperature metrics (mean, median, seasonal)  
- demand indicators (mean annual HDD/CDD)  
- temperature rank  
- selection flag  

### 2. `ukcp18_member_selection.csv`
Contains only the selected members with:
- `selection_role` (cool / median / warm)  
- `selection_basis`  
- key summary metrics  

---

## Summary

This approach provides a transparent and statistically grounded method for reducing the UKCP18 ensemble into a manageable subset while preserving the key variation in future temperature and demand-relevant conditions.
