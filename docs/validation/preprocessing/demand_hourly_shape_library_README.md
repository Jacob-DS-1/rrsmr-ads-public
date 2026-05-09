# demand_hourly_shape_library.parquet — README

## Overview
This deliverable is an **hourly demand shape library** used to redistribute **daily demand values into hourly profiles** for Great Britain (GB).

## Data summary
- **Input basis:** cleaned historical hourly demand output derived from NESO Historic Demand data
- **Geographic scope:** Great Britain (England + Wales + Scotland; excludes Northern Ireland)
- **Date range:** 2010–2024
- **Primary shaping variable:** `nd_mw` (National Demand)

**File:** `demand_hourly_shape_library.parquet`

**Columns:**
- `month` — month number (1–12) based on GB local time
- `day_type` — `weekday`, `weekend`, or `holiday`
- `hour` — hour of day (0–23) in GB local time
- `nd_hour_fraction` — average fraction of a local day’s demand occurring in that hour (unitless)

---

## Purpose
Forecasting or scenario modelling is often easier at **daily** resolution.  
This library supports conversion from **daily demand → hourly demand** by providing a typical within-day ND profile conditioned on:
- **month** (captures seasonal variation)
- **day type** (captures weekday, weekend, and GB bank-holiday behaviour)
- **hour** (captures intraday timing patterns)

The output is intended for downstream future hourly demand reconstruction rather than direct reproduction of historical absolute demand levels.

---

## Input basis
The library is built from the final cleaned hourly historical demand output, which contains:
- `timestamp_utc`
- `nd_mw`
- `tsd_mw`
- The shape library also uses holiday_calendar_2010_2045.csv to identify GB bank holidays during the historical calibration period.

Only **ND** is used to construct the shape library, because ND is the primary demand definition used in this project.

---

## How the library is constructed

### Step 1: Start from hourly ND series
The shape library starts from the cleaned hourly ND series (`nd_mw`) indexed by `timestamp_utc`.

### Step 2: Convert UTC timestamps to GB local time
Although the hourly demand file is stored in **UTC**, demand shape behaviour is driven by **local GB time**.  
Therefore, `timestamp_utc` is converted back to `Europe/London` to derive:
- `date_local`
- `month`
- `hour`
- `day_of_week`
- `day_type`

This ensures that hourly shape features align with local calendar behaviour rather than UTC clock time.

### Step 3: Compute local-day ND totals
For each **local GB date**, total daily ND is calculated as:

- `nd_daily_total_local = sum(nd_mw across all hours in that local day)`

Because `nd_mw` is an hourly MW series, summing across the hours of a local day gives the daily total in MWh-equivalent terms for shaping purposes.

### Step 4: Compute hourly fraction within each local day
For each hour within a local day:

- `nd_hour_fraction = nd_mw / nd_daily_total_local`

This produces a **unitless within-day fraction**.  
For example, a value of `0.035` means that approximately **3.5%** of that local day’s total demand occurs in that hour.

### Step 5: Average fractions into the reusable library
Hourly fractions are averaged across the historical period by:
- `month`
- `day_type`
- `hour`

This produces a typical hourly demand shape for each month × day-type × hour combination.

---

## Day type definition

- `holiday` = any date where `is_holiday_gb_any == 1`
- `weekend` = Saturday or Sunday where `is_holiday_gb_any == 0`
- `weekday` = Monday to Friday where `is_holiday_gb_any == 0`

Holiday classification takes priority over weekday/weekend classification. For example, a bank holiday that falls on a Monday is labelled `holiday`, not `weekday`.

Bank holidays are **not** included as a separate category in this version of the library.  

---

## DST and local-time considerations
The hourly source file is stored in **UTC**, but the shape library is intentionally built using **local GB time** because demand patterns follow local clock time.

As a result:
- local-date totals are based on `Europe/London`
- DST effects are handled through the UTC → local conversion step
- the shape library reflects observed local intraday behaviour rather than UTC-hour behaviour


---

## Units
- `nd_hour_fraction` is **unitless**

---

## QA
The library is grouped by `month`, `day_type`, and `hour`. Each `month × day_type` hourly profile is normalised so that `nd_hour_fraction` sums to 1. Some months may not contain a `holiday` profile if there were no historical GB holiday dates in that month.

