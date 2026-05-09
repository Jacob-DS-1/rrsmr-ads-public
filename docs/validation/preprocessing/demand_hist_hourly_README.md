# demand_hist_hourly.parquet — README

## Overview
This deliverable contains an **hourly historical demand time series for Great Britain (GB)** derived from NESO “Historic Demand” half-hourly settlement data (2010-2024).

## Data summary
- **Input data:** NESO Historic Demand Data (half-hourly annual files)
- **Geographic scope:** Great Britain (England + Wales + Scotland; excludes Northern Ireland)
- **Date range:** 2010–2024
- **Primary source fields used:** `SETTLEMENT_DATE`, `SETTLEMENT_PERIOD`, `ND`, `TSD`
- **Missing Values:** 0
- **Total rows in final output:** 131496

**File:** `demand_hist_hourly.parquet`

**Columns:**
- `timestamp_utc` — timezone-aware UTC timestamp at hourly resolution
- `nd_mw` — hourly mean National Demand, in MW
- `tsd_mw` — hourly mean Transmission System Demand, in MW

---

## Source data and granularity
The NESO historic demand source data is indexed by:
- `SETTLEMENT_DATE` — settlement day in GB local time (`Europe/London`)
- `SETTLEMENT_PERIOD` — half-hour period number within the settlement day

Each settlement period represents a **30-minute** interval.

Valid settlement-day lengths are expected to be:
- **48 periods** on standard days
- **46 periods** on spring DST transition days
- **50 periods** on autumn DST transition days

---

## Column definitions

### ND — National Demand (MW)
**National Demand (ND)** is the core GB demand series used in this project and is treated as the primary demand variable for modelling and FES anchoring.

### TSD — Transmission System Demand (MW)
**Transmission System Demand (TSD)** is retained as a secondary GB demand series for comparison and downstream use.

### England & Wales demand
Some NESO historic demand files may include `ENGLAND_WALES_DEMAND`, which is an England & Wales-specific measure rather than a GB-wide series.  
This deliverable focuses on the GB-wide outputs `ND` and `TSD`, so `ENGLAND_WALES_DEMAND` is not included in the final parquet output.

---

## Time handling and timestamp construction

### Why timestamp construction is needed
The NESO source is supplied as settlement date plus settlement period, rather than as an explicit UTC timestamp.

### Settlement-date anchor
To construct timestamps robustly across DST transitions:
1. each `SETTLEMENT_DATE` is anchored at **local midnight** in `Europe/London`
2. that local-midnight anchor is converted to **UTC**
3. each half-hourly timestamp is then created as:

`timestamp_utc = local settlement midnight converted to UTC + (SETTLEMENT_PERIOD - 1) × 30 minutes`

This approach preserves valid **46-, 48-, and 50-period settlement days** without dropping rows and avoids ambiguity from constructing local wall-clock timestamps directly.

### DST handling
DST affects the number of periods in a **local settlement day**, but the final output is stored in **UTC**.

As a result:
- local GB dates may correspond to **23, 24, or 25 hours**
- the final `timestamp_utc` series is expected to remain **continuous and gap-free in UTC**

---

## Half-hourly to hourly aggregation method

### Objective
Convert half-hourly MW demand values into an hourly MW demand time series.

### Method
1. Each half-hourly `timestamp_utc` is assigned to its containing UTC hour:
   - `hour_utc = floor(timestamp_utc to the hour)`
2. Half-hourly values are aggregated within each UTC hour using the **mean**:
   - `nd_mw = mean(ND over the hour)`
   - `tsd_mw = mean(TSD over the hour)`

### Why mean?
ND and TSD are power values measured in **MW**.  
Taking the mean of the two half-hourly MW observations gives the **average MW across the hour**, which is the appropriate hourly representation for this deliverable.


---

## Units
- `ND`, `TSD`, `nd_mw`, and `tsd_mw` are in **megawatts (MW)**

---
