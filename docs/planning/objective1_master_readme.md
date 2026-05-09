# Objective 1 — Future Supply Modelling

**Geography:** Great Britain (GB)
**Horizon:** Hourly generation, **2030–2045**
**Scenario framework:** NESO **Future Energy Scenarios (FES)** — *Holistic
Transition* and *Electric Engagement*

This is the master technical summary for **Objective 1: Future Supply
Modelling**. It is written in plain English for a mixed audience (modelling,
policy, engineering). Every claim in this document is backed by a script in
`src/` or an artefact in `outputs/objective1_generation/`.

All times are **UTC**. Power is **megawatts (MW)**; annual energy
reconciliation uses **terawatt-hours (TWh)**.

---

## Executive summary

Objective 1 produces a **model-ready hourly GB generation dataset** for the
period **2030–2045**, on a 1-hour grain, with one row per
`(timestamp_utc, fes_scenario, weather_year)`.

The pipeline:

1. **Calibrates** historical generation (NESO genmix) to historical capacity
   (DUKES) and historical weather (ERA5), giving us empirical capacity
   factors for wind and solar by hour.
2. **Trains** a small benchmarked machine-learning model that maps weather
   features (100 m wind speed, surface shortwave radiation) onto observed
   capacity factors.
3. **Builds a 3-year weather scaffold** that repeats three representative
   ERA5 historical years (2010 = low wind, 2014 = average, 2015 = high)
   across the future calendar.
4. **Adjusts** the historic baseline shape with the ML model's
   weather-driven correction factor for wind and solar, and uses the
   historic baseline directly for dispatchables and storage.
5. **Anchors** every FES-anchored technology's annual energy to the matching
   FES TWh target per scenario and weather year.
6. **Exports** a wide MW parquet with explicit, post-review column names and
   `model_role` flags so Objective 3 can read each column unambiguously.

The output is suitable for downstream unit-commitment work, residual demand
calculations, scenario comparisons, and integration with demand and network
models. Objective 1 deliberately does **not** balance the system — that is
Objective 3's responsibility (see Scope & Limitations below).

---

## Methodology

### Step 1 — Historic calibration

We calibrate three historical sources against each other:

- **NESO genmix (hourly generation by source).** This gives us observed
  hourly MW per technology for 2010–2024.
- **DUKES capacity tables (annual installed MW).** This gives us the
  installed wind and solar capacity for each year.
- **ERA5 reanalysis (hourly weather).** This gives us GB-mean wind speed
  at 100 m and surface shortwave radiation hourly.

Joining these on year and `timestamp_utc` produces a training table where
each row carries: an observed MW for wind and solar, the DUKES installed
capacity for that year, and the matching ERA5 weather. The empirical
capacity factor is then `MW / installed MW`.

We also persist a `genmix_profile_library.parquet` of historic
`p10 / p50 / p90` MW shapes per `tech × month × day_type × hour`. This is
the **baseline shape** that the rest of the pipeline rests on.

### Step 2 — Weather-to-capacity-factor model (benchmarked)

We benchmark three gradient-boosted libraries on a temporal 80/20 split:

- `sklearn.HistGradientBoostingRegressor`
- `xgboost.XGBRegressor`
- `lightgbm.LGBMRegressor`

The winner per energy type (wind / solar) is selected by test-set R²
(tie-break: lower RMSE, then faster training). The winner is then refit on
the full training table inside a `ClippedRegressor` wrapper (predictions
clipped to `[0, 1]`) and saved to `era5_renewable_models.joblib` for the
downstream pipeline.

Model inputs are deliberately minimal — wind uses the 100 m wind speed,
solar uses surface shortwave radiation — because the output is going to be
used as an *adjustment factor*, not a raw shape (see step 4).

### Step 3 — Three-year ERA5 weather scaffold

We chose **three** representative ERA5 weather years from the 15-year
record, instead of a single 2018 scaffold. Selection is by GB-mean wind
speed:

| Weather year | Role          | GB-mean wind (m/s) | Notes                          |
|--------------|---------------|--------------------|--------------------------------|
| **2010**     | low_wind      | 7.480              | Lowest wind year on record     |
| **2014**     | average_wind  | 8.213              | Closest to 15-year median      |
| **2015**     | high_wind     | 8.699              | Highest wind year on record    |

All three are non-leap (8760 hours), so future leap-day hours (Feb-29 in
2032 / 2036 / 2040 / 2044) fall back to Feb-28 of the chosen weather year
— a clean, no-NaN mapping across the entire pathway.

The scaffold maps each representative year's hourly weather onto the
2030–2045 calendar. Every downstream output is therefore produced in
**three parallel weather realisations** so any annual or hourly metric can
be reported as a band rather than a single number.

### Step 4 — Baseline + Weather Adjustment for renewables

For each future hour and each weather year, the unscaled wind / solar MW
is computed as:

```
unscaled_MW  =  baseline_p50_MW  ×  ( predicted_CF / typical_CF )
```

- **`baseline_p50_MW`** — historic median MW for that
  `month × day_type × hour`, from `genmix_profile_library.parquet`.
- **`predicted_CF`** — ML model output for the scaffolded ERA5 features.
- **`typical_CF`** — historic median capacity factor for the matching
  `month × hour × is_weekend`, from `typical_cf_library.parquet`.

The ratio `predicted_CF / typical_CF` is clipped to `[0, 5]`. Where the
typical CF is below `1e-3` (e.g. solar at night) the ratio is forced to
`1.0`, but the baseline MW is also ~0 in those hours so the product is ~0.

This is the structural change requested by Priyanshi: the model corrects
the **shape** of weather variability on top of a physically calibrated
baseline rather than emitting an absolute MW directly.

### Step 5 — Profile-based dispatchables, storage, and imports

| Technology family                                      | How its hourly shape is built                                                |
|--------------------------------------------------------|------------------------------------------------------------------------------|
| Nuclear (existing), biomass, hydro, "other", storage   | Historic `p50_mw` profile from `genmix_profile_library.parquet` directly.    |
| Imports (`imports_net_baseline_mw`)                    | DUKES 5.13 2020–2024 mean Total net imports (TWh), held flat across the pathway, with the historic `p50_mw` shape rescaled per year so the annual sum equals the baseline TWh. |
| Coal, gas (`coal_reference_mw`, `gas_reference_mw`)    | Historic `p50_mw` profile carried forward as a diagnostic series.            |

Imports use the explicit DUKES 5.13 baseline cap rather than a
`multiplier = 1` placeholder — this is the change requested by Jacob.

### Step 6 — FES anchoring per technology

Every FES-anchored technology has its annual TWh scaled to the matching FES
target, computed per `(year, fes_scenario, weather_year, model_tech)`. The
multiplier for each combination is written to
`fes_anchoring_multipliers.csv` so a reviewer can audit any anchored value
end-to-end.

| Wide column                  | FES techs summed                              | Anchored? |
|------------------------------|-----------------------------------------------|-----------|
| `wind_mw`                    | Onshore Wind + Offshore Wind                  | ✅        |
| `solar_mw`                   | Solar PV                                      | ✅        |
| `nuclear_existing_mw`        | Nuclear (Obj 3 adds SMRs on top)              | ✅        |
| `biomass_mw`                 | Biomass + CCS Biomass + Waste                 | ✅        |
| `hydro_mw`                   | Hydro                                         | ✅        |
| `other_mw`                   | Other Renewable + Other Thermal + Hydrogen    | ✅        |
| `storage_net_mw`             | Battery + Long Duration Energy Storage        | ✅        |
| `gas_reference_mw`           | Gas + CCS Gas (diagnostic only)               | ⚠️        |
| `coal_reference_mw`          | (no FES Coal target — historic profile)       | ❌        |
| `imports_net_baseline_mw`    | DUKES 5.13 baseline (no FES anchor)           | ❌        |

`gas_reference_mw` is anchored to the FES gas TWh as a diagnostic ceiling
so its magnitude is comparable to FES, but `model_role = reference` flags
that Objective 3 should treat it as the rebalancing source.

---

## Pipeline (sequential tasks)

```
src/
├── task1_prep_and_calibration.py
├── task2_ml_training.py
├── task3_baseline_and_weather_scaffold.py
├── task4_weather_adjustment.py
└── task5_fes_anchoring_and_export.py
```

| Task | Script                                             | Role                                                                                                                                                |
|------|----------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| 1    | `src/task1_prep_and_calibration.py`                | Build the training table and the four foundational deliverables: ERA5 resource for the three weather years, tech-year DUKES calibration, profile library, taxonomy map. |
| 2    | `src/task2_ml_training.py`                         | Benchmark HistGBR / XGBoost / LightGBM on a temporal 80/20 split; refit winners on the full training table inside `ClippedRegressor`; emit `era5_renewable_models.joblib`. |
| 3    | `src/task3_baseline_and_weather_scaffold.py`       | Select the 3 historical ERA5 years, build the typical historic CF library (denominator of the adjustment factor), and scaffold the ERA5 weather onto 2030–2045. |
| 4    | `src/task4_weather_adjustment.py`                  | Apply Priyanshi's `predicted_CF / typical_CF` adjustment factor to wind and solar; carry dispatchables / storage on the historic profile; build the DUKES-5.13 imports baseline. Output: `future_supply_hourly_unscaled.parquet`. |
| 5    | `src/task5_fes_anchoring_and_export.py`            | Apply per-tech FES multipliers; pivot to the wide schema with renamed columns; perform DUKES load-factor QA, calendar / sign / reconciliation gates; emit the final wide parquet, multipliers audit, QA notes, and validation plots. |

Run order:

```bash
python src/task1_prep_and_calibration.py
python src/task2_ml_training.py
python src/task3_baseline_and_weather_scaffold.py
python src/task4_weather_adjustment.py
python src/task5_fes_anchoring_and_export.py
```

Task 5 prints `INTEGRATION CHECK PASS` when calendar coverage,
no-duplicate-keys, sign conventions, and FES-annual reconciliation all
pass.

---

## Deliverables

All deliverables live in `outputs/objective1_generation/`.

### Final modelling output

- `generation_future_hourly_2030_2045` — wide hourly export, one row
  per `(timestamp_utc, fes_scenario, weather_year)` with one MW column per
  technology.

### Foundational deliverables (Task 1 — calibration & taxonomy)

| File                                | Description                                                                                                       |
|-------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| `era5_resource_hourly.parquet`      | GB-mean ERA5 hourly wind speed (m/s) and SSRD (J/m²) for the **three representative weather years**, tagged with `weather_year_role`. |
| `tech_year_calibration.csv`         | DUKES annual capacity, observed genmix-derived generation (MWh / TWh), DUKES load factor (%), and implied LF per `(tech, year)`. |
| `genmix_profile_library.parquet`    | Historic technology profile library: `tech × month × day_type × is_weekend × hour` with `p10 / p50 / p90` MW.    |
| `genmix_taxonomy_map.csv`           | Explicit raw NESO source → internal name → model column map, with `role`, FES anchor target, and is-anchored flag. |

### ML model artefacts (Task 2)

- `era5_renewable_models.joblib` — production wind / solar models.
- `weather_model_metrics.csv` — selected models' R² / RMSE on the held-out
  test window.
- `model_performance_report.csv` — full benchmark table (every library,
  R², RMSE, training time, selected flag).
- `benchmark_plot.png` — bar chart of R² and training time across libraries.

### Baseline & adjustment intermediates (Tasks 3–4)

- `typical_cf_library.parquet` — typical historic CF by
  `tech × month × hour × is_weekend` (the *denominator* of the adjustment
  factor).
- `weather_scaffold_hourly.parquet` — three weather years mapped onto the
  2030–2045 calendar with `wind_speed_100m_ms` and `ssrd_j_m2` per future
  hour.
- `future_supply_hourly_unscaled.parquet` — long-form unscaled MW per
  `(timestamp_utc, weather_year, tech)` after the adjustment factor and
  imports baseline have been applied.

### Anchoring & QA artefacts (Task 5)

- `future_supply_hourly_fes_anchored.parquet` — long-form anchored MW.
- `fes_anchoring_multipliers.csv` — per-`(year, scenario, weather_year,
  tech)` audit of every anchoring multiplier.
- `qa_notes.md` — auto-generated summary of every QA gate.
- `qa_dukes_loadfactor_check.csv` — STRICT renewable CF envelope check
  (ML predicted CF vs DUKES 6.3) and INFO anchored-tech rows.
- `qa_fes_reconciliation.csv` — per-`(year, scenario, weather_year,
  anchored column)` `|model − FES|` TWh.
- `qa_plots/qa_era5_fit.png` — historic vs ERA5-linked CF fit on training
  data.
- `qa_plots/qa_annual_pathway.png` — annual TWh by technology, model band
  across the three weather years overlaid on FES targets per scenario.
- `qa_plots/qa_diurnal_2030.png` — mean diurnal wind / solar MW for 2030 by
  weather year.

---

## Output schema (final wide parquet)

`generation_future_hourly_2030_2045` — one row per
`(timestamp_utc, fes_scenario, weather_year)`.

### Identifier columns

| Column                | Type      | Description                                                  |
|-----------------------|-----------|--------------------------------------------------------------|
| `timestamp_utc`       | datetime  | Hour-beginning timestamp, **timezone-aware UTC**.            |
| `year`                | int32     | Calendar year derived from `timestamp_utc`.                  |
| `fes_scenario`        | string    | `Holistic Transition` or `Electric Engagement`.              |
| `weather_year`        | int32     | One of `2010` (low), `2014` (average), `2015` (high).        |
| `weather_year_role`   | string    | `low_wind` / `average_wind` / `high_wind`.                   |

### Technology columns (MW)

| Column                       | Description                                                                                |
|------------------------------|--------------------------------------------------------------------------------------------|
| `wind_mw`                    | Wind (onshore + offshore combined), FES-anchored.                                          |
| `solar_mw`                   | Solar PV, FES-anchored.                                                                    |
| `nuclear_existing_mw`        | Existing nuclear fleet baseline (renamed from `nuclear_mw`). Obj 3 adds SMRs on top.       |
| `biomass_mw`                 | Biomass + CCS Biomass + Waste, FES-anchored.                                               |
| `hydro_mw`                   | Hydro, FES-anchored.                                                                       |
| `other_mw`                   | Other Renewable + Other Thermal + Hydrogen (per `genmix_taxonomy_map.csv`).                |
| `storage_net_mw`             | Storage net (FES-anchored). `+` = discharge to grid; `−` = pumping / charge.               |
| `gas_reference_mw`           | **Diagnostic only** — Obj 3 rebalances gas (renamed from `gas_mw`).                        |
| `coal_reference_mw`          | **Diagnostic only** — historic profile, unscaled (no FES Coal target).                     |
| `imports_net_baseline_mw`    | DUKES 5.13 baseline cap (renamed from `imports_net_mw`, no FES anchor). `+` = import to GB; `−` = export. |

### Sign conventions and units

- `_mw` columns are **MW** (instantaneous hourly mean power).
- Annual energy from hourly MW summed over hours is **MWh**; divide by `1e6`
  for **TWh**.
- All timestamps are **timezone-aware UTC**.
- Leap years (2032, 2036, 2040, 2044) carry **8784** rows per
  `(scenario, weather_year)`; non-leap years carry **8760**.
- All non-storage / non-imports columns are non-negative (the QA gate
  enforces this).

---

## Scope & Limitations

### What Objective 1 *is*

Objective 1 produces the **exogenous, profile-based hourly supply
pathways** for GB. Each anchored column matches its FES TWh target. Each
non-anchored column is documented and flagged so it can be overridden
downstream.

### What Objective 1 *is not*

- **Objective 1 does not balance the system.** It does not compute residual
  demand, does not allocate dispatch, and does not enforce any
  supply ↔ demand equality. **System balancing is Objective 3.**
- **Objective 1 does not allocate SMR capacity.** `nuclear_existing_mw` is
  the *existing-fleet baseline only*. SMR contribution is added on top of
  this column in **Objective 3**.
- **`gas_reference_mw`, `coal_reference_mw`, and `imports_net_baseline_mw`
  are diagnostic / baseline-only columns.** Obj 3 must rebalance gas (the
  marginal source) and may overwrite imports from an ES2-derived
  projection.
- **FES capacity is not in the supplied tables.** The DUKES load-factor
  envelope check on anchored fleets (nuclear, biomass, hydro) is therefore
  reported as `INFO` only — out-of-envelope rows generally indicate
  projected fleet expansion under FES, not a model error.

### Climate scope (UKCP18 disclaimer)

- **UKCP18 is intentionally kept out of the Objective 1 core modelling.**
  Objective 1 uses **ERA5 historical reanalysis** as the weather scaffold
  (three representative years) because the renewable conversion model and
  the baseline shape library are calibrated against the **same
  observational reference**. Mixing UKCP18 climate projections directly
  into the supply scaffold would break that physical consistency.
- The overall project remains climate-aware: **explicitly
  climate-responsive demand modelling using UKCP18 is performed in
  Objective 2**. Objective 1 (supply, ERA5-based) and Objective 2 (demand,
  UKCP18-based) sit alongside each other; Objective 3 then balances them.

---

## References

- **NESO FES** — Future Energy Scenarios (*Holistic Transition*,
  *Electric Engagement*).
- **ECMWF ERA5** — global reanalysis providing hourly weather drivers.
- **DUKES** — UK government Digest of UK Energy Statistics. Tables used:
  5.7 (capacity), 5.10B (load factors), 5.13 (interconnector flows), 6.2
  (renewable capacity), 6.3 (renewable load factors).

---

*See `CHANGELOG_Peer_Review_Updates.md` for an item-by-item record of how
the Jacob and Priyanshi feedback was incorporated into the codebase and the
output schema.*
