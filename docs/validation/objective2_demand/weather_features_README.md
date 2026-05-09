
# weather_features_README

## Purpose
This README documents the creation of the Task 2 weather feature outputs for Objective 2.

---

## Task 2 objective
Task 2 creates climate features that can later be used in the daily climate-sensitive demand model.

Specifically, this task produces:

- GB-weighted daily minimum temperature
- GB-weighted daily maximum temperature
- GB-weighted daily mean temperature
- Heating Degree Days (`hdd`)
- Cooling Degree Days (`cdd`)

These features are created for both:

- historic weather data
- future UKCP18 weather data

## Final Dataset size

weather_hist_features_daily shape: (5479, 6)
weather_future_features_daily shape: (175320, 7)

---

## Why these weather features are needed

The final modelling target in Objective 2 is a single GB-level daily demand series, rather than separate regional demand series.

However, the weather inputs are available at regional level:
- England and Wales
- Scotland

This creates a mismatch:
- the response variable is GB-level
- the raw weather variables are regional

Therefore, a GB-level weather representation is required so that the climate features are aligned with the demand target.

A single GB-weighted temperature series is also needed so that:
- one consistent national daily weather signal is used in the model
- HDD and CDD can be calculated from a common GB temperature basis
- the same feature engineering logic can be applied to both historic and future climate data
- future climate members can later be compared consistently

---

### Data quality checks
The following checks were carried out:
- no unexpected missing values in weather variables
- no duplicate `date + region` rows in historic weather
- no duplicate `date + climate_member + region` rows in future weather
- expected regional values present
- date coverage checked
- climate member values inspected against lookup file

---

## Modelling assumptions

This task requires explicit modelling assumptions for:

1. GB temperature weighting
2. HDD base temperature
3. CDD base temperature

These assumptions are documented below.

---

## Assumption 1: GB-weighted temperature

### Why weighting is required
The model target is national GB daily electricity demand, but the available weather data is regional.

To create climate features that are aligned with the target variable, the regional temperatures must be combined into a single GB-level temperature representation.

### Weighting approach chosen
A **population-weighted** approach was used to combine England/Wales and Scotland temperature series.

### Why population-weighting was chosen
A demand-weighted approach would be conceptually strongest, because the model predicts aggregate GB electricity demand.

However, the available project datasets contain:
- GB-level demand
- regional weather

and do not contain:
- regional electricity demand shares for England/Wales and Scotland

Therefore, direct demand-weighting could not be estimated from the available data.

Population-weighting was chosen as a transparent and defensible proxy because:
- electricity demand is linked to where people and occupied buildings are concentrated
- it is more appropriate than equal weighting for a national demand model
- it is more relevant than land-area weighting for a demand-driven problem
- it produces a reproducible GB-level exposure measure

### Population-weight formula
Let:

- `pop_eng_wales` = England and Wales population
- `pop_scotland` = Scotland population

Then:

- `w_eng_wales = pop_eng_wales / (pop_eng_wales + pop_scotland)`
- `w_scotland = pop_scotland / (pop_eng_wales + pop_scotland)`

### Temperature aggregation formulas
The same weighting logic was applied to daily minimum, maximum, and mean temperature.

#### GB-weighted daily mean temperature
`tmean_gb_c = w_eng_wales * tmean_eng_wales_c + w_scotland * tmean_scotland_c`

#### GB-weighted daily minimum temperature
`tasmin_gb_c = w_eng_wales * tasmin_eng_wales_c + w_scotland * tasmin_scotland_c`

#### GB-weighted daily maximum temperature
`tasmax_gb_c = w_eng_wales * tasmax_eng_wales_c + w_scotland * tasmax_scotland_c`

### Recommendation for documentation
The exact population values and final calculated weights used in code should be recorded in the final project submission or appendix.

---

## Assumption 2: Heating Degree Days (HDD)

### What HDD represents
Heating Degree Days measure how much colder a day is than a chosen heating threshold.

This is used to capture the idea that colder days are associated with stronger heating-related electricity demand effects.

### Why HDD is needed
A single temperature variable may not fully capture non-linear temperature-demand behaviour.

In practice, electricity demand often becomes more sensitive when temperatures fall below a certain threshold.

HDD is therefore used to represent cold-weather demand pressure in an interpretable way.

### HDD formula
`hdd = max(hdd_base_c - tmean_gb_c, 0)`

### HDD base temperature used
- `hdd_base_c = 15.5`

### Why 15.5°C was used
This was selected as a practical initial modelling threshold for heating sensitivity.

Reasoning:
- it is a defensible energy-modelling threshold
- it allows cold-weather demand effects to be represented without treating all mild days as strong heating days
- it provides a transparent starting point for feature engineering
- it can later be validated during model training in Task 3

This should be interpreted as a modelling assumption, not as a fixed physical truth.

---

## Assumption 3: Cooling Degree Days (CDD)

### What CDD represents
Cooling Degree Days measure how much warmer a day is than a chosen cooling threshold.

This is used to capture the possibility that hotter days may increase cooling-related electricity demand.

### Why CDD is needed
Although cooling sensitivity is generally weaker than heating sensitivity in GB, future climate analysis may make warm-weather effects more relevant.

Including CDD ensures that the feature set can represent both:
- cold-weather demand effects
- hot-weather demand effects

### CDD formula
`cdd = max(tmean_gb_c - cdd_base_c, 0)`

### CDD base temperature used
- `cdd_base_c = 22.0`

### Why 22.0°C was used
This was selected as a conservative initial threshold for cooling sensitivity.

Reasoning:
- it avoids overstating cooling effects on moderately warm days
- it is appropriate for a GB context where widespread cooling demand is weaker than heating demand
- it supports future climate scenario modelling without forcing a strong cooling response on mild temperatures
- it can later be reviewed during model validation in Task 3

This should again be interpreted as an initial modelling assumption.

## Note on `.clip(lower=0)`

In the Python implementation, HDD and CDD were created using `.clip(lower=0)`.

This sets any value below 0 to 0, while leaving positive values unchanged.

### Why it is needed for HDD and CDD
The HDD and CDD formulas are:

- `hdd = max(hdd_base_c - tmean_gb_c, 0)`
- `cdd = max(tmean_gb_c - cdd_base_c, 0)`

This means:
- HDD should never be negative
- CDD should never be negative

If the raw subtraction produces a negative value, that means the threshold has not been exceeded, so the correct degree-day value is 0.


---

## Limitations and interpretation

### 1. Population-weighting is a proxy
Population-weighting is not identical to demand-weighting.

It is used because regional demand-share data was not available.

### 2. HDD and CDD thresholds are modelling assumptions
The chosen base temperatures are initial, theory-informed thresholds rather than final confirmed optima.

Their usefulness should be assessed in Task 3 when the demand model is trained and validated.

### 3. Single national temperature representation
Using one GB-weighted temperature simplifies the national demand modelling problem.

This is appropriate for the current project design, but it does not explicitly model separate regional demand responses.


---


