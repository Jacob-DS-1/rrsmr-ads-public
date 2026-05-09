# demand_hist_daily.parquet — README

## Overview
This deliverable contains **daily historical demand summaries for Great Britain (GB)** derived from NESO “Historic Demand” half-hourly settlement data.

## Data summary
- **Input data:** NESO Historic Demand Data (half-hourly annual files)
- **Geographic scope:** Great Britain (England + Wales + Scotland; excludes Northern Ireland)
- **Date range:** 2010–2024
- **Primary source fields used:** `SETTLEMENT_DATE`, `SETTLEMENT_PERIOD`, `ND`, `TSD`
- **Missing Values:** 0
- **Total rows in final output:** 5479

**File:** `demand_hist_daily.parquet`

**Columns:**
- `date` — settlement date (GB local settlement date)
- `nd_daily_mwh` — daily National Demand energy, in MWh
- `nd_mean_mw` — mean half-hourly National Demand over the settlement day, in MW
- `tsd_daily_mwh` — daily Transmission System Demand energy, in MWh

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

---

## Daily calculations

### Daily aggregation basis
The daily file is derived directly from the cleaned half-hourly demand dataset using:
- `SETTLEMENT_DATE`
- `SETTLEMENT_PERIOD`
- `ND`
- `TSD`

The daily output is grouped by **settlement date**, so `date` represents the GB local settlement day rather than a UTC date.
- `the hourly deliverable is indexed by timestamp_utc`
- `the daily deliverable is grouped by SETTLEMENT_DATE`. A daily aggregation should usually respect the natural day boundary of the source system. Here, the source system is settlement-based, not UTC-calendar-based.

### Daily mean demand (MW)
Daily mean demand is calculated from the half-hourly MW values as:

- `nd_mean_mw = mean(ND over all settlement periods in that date)`

This represents the average ND power level across that settlement day.

### Daily energy (MWh) from half-hourly MW
Daily energy is calculated from half-hourly power values using the 30-minute settlement-period duration:

- `nd_daily_mwh = sum(ND over all periods in that date) × 0.5`
- `tsd_daily_mwh = sum(TSD over all periods in that date) × 0.5`

### Why multiply by 0.5?
ND and TSD are measured in **MW**, which is power.  
To convert power to energy:

- energy (MWh) = power (MW) × time (hours)

Because each settlement period covers **0.5 hours**, the summed half-hourly MW values are multiplied by **0.5** to derive daily energy in MWh.

---

## DST considerations
DST affects the number of settlement periods in a **local GB settlement day**:
- standard days contain 48 half-hour periods
- spring transition days contain 46 periods
- autumn transition days contain 50 periods

Here, these valid settlement-day lengths are preserved rather than removed during timestamp construction.  
As a result, daily calculations are based on the full valid set of settlement periods available for each settlement date.

This means:
- daily mean demand is computed across the actual settlement periods present on that date
- daily energy is computed using the full valid half-hourly coverage for that settlement date


---

## Units
- `nd_mean_mw` is in **MW**
- `nd_daily_mwh` is in **MWh**
- `tsd_daily_mwh` is in **MWh**

---


