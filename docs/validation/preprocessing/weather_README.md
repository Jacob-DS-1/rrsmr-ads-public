# Weather Data Preprocessing — README

**Project:** Small Nuclear Reactor Integration into the GB National Grid  
**Person:** Person 4

---

## 1. Input Files

### HadUK-Grid (historical)
| File | Description |
|---|---|
| `tasmax_hadukgrid_uk_country_day_19310101-20241231.nc` | Daily maximum temperature, 8 UK regions, 1931–2024 |
| `tasmin_hadukgrid_uk_country_day_19310101-20241231.nc` | Daily minimum temperature, same structure |

### UKCP18 (future projections)
| File | Region | Variable |
|---|---|---|
| `Eng_Wales_Max_Temp_2010_2050.csv` | England and Wales | `tasmax_c` |
| `Eng_Wales_Min_Temp_2010_2050.csv` | England and Wales | `tasmin_c` |
| `Eng_Wales_Mean_Temp_2010_2050.csv` | England and Wales | `tmean_c` |
| `Scot_Max_Temp_2010_2050.csv` | Scotland | `tasmax_c` |
| `Scot_Min_Temp_2010_2050.csv` | Scotland | `tasmin_c` |
| `Scot_Mean_Temp_2010_2050.csv` | Scotland | `tmean_c` |

---

## 2. Regions Kept and Excluded

The HadUK-Grid NetCDF contains 8 region aggregates. Only two were retained:

| geo_region label | Action | Output slug |
|---|---|---|
| Channel Islands | Excluded | — |
| England | Excluded | — |
| England and Wales | **Kept** | `eng_wales` |
| Isle of Man | Excluded | — |
| Northern Ireland | Excluded | — |
| Scotland | **Kept** | `scotland` |
| United Kingdom | Excluded | — |
| Wales | Excluded | — |

---

## 3. Output Files and Date Ranges

| Deliverable | Format | Date Range | Purpose |
|---|---|---|---|
| `weather_hist_daily.parquet` | Parquet | 2010-01-01 : 2024-12-31 | Historical daily weather for modelling baseline |
| `weather_future_daily_ukcp18_raw360.parquet` | Parquet | 2010-01-01 : 2049-12-30 | Raw UKCP18 future data preserved on original 360-day calendar |
| `weather_future_daily_ukcp18.parquet` | Parquet | 2030-01-01 : 2045-12-31 | Gregorian daily future weather for downstream modelling and joins |
| `ukcp18_member_lookup.csv` | CSV | — | Lookup table for retained UKCP18 ensemble members |
| `weather_README.md` | Markdown | — | Documentation |

### Intended downstream use
- Use `weather_hist_daily.parquet` for historical model fitting and diagnostics.
- Use `weather_future_daily_ukcp18.parquet` for joins to demand / calendar-based modelling.
- Keep `weather_future_daily_ukcp18_raw360.parquet` as an audit trail of the untouched UKCP18 calendar.

---

## 4. Temperature Column Derivation

### HadUK-Grid
| Column | Source |
|---|---|
| `tasmax_c` | Loaded directly from `tasmax` variable in tasmax NetCDF |
| `tasmin_c` | Loaded directly from `tasmin` variable in tasmin NetCDF |
| `tmean_c` | Computed as `(tasmax_c + tasmin_c) / 2` — no separate mean file available |

The HadUK time axis stores a noon timestamp (`12:00:00`) on every date.  
This was stripped using `.normalize()` to give clean daily dates.

### UKCP18
| Column | Source |
|---|---|
| `tasmax_c` | Loaded from `Eng_Wales_Max_Temp` / `Scot_Max_Temp` CSV |
| `tasmin_c` | Loaded from `Eng_Wales_Min_Temp` / `Scot_Min_Temp` CSV |
| `tmean_c` | Loaded from `Eng_Wales_Mean_Temp` / `Scot_Mean_Temp` CSV — **not** overwritten with `(tasmin_c + tasmax_c) / 2` |

This means the future mean temperature series remains the official UKCP18 mean-temperature variable.

---

## 5. UKCP18 Metadata Header Handling

Each UKCP18 CSV begins with a metadata block. The parsing procedure is:

1. Line 1 contains `header length,N`.
2. `skiprows = N - 1` is passed to `pd.read_csv()` so row `N` becomes the column header.
3. Lines 2 to `N-1` are parsed into a metadata dictionary.
4. The metadata fields `Area` and `Variable` are extracted and cross-checked against the expected values implied by the filename.
5. Any mismatch raises an `AssertionError` immediately.

This prevents the wrong region or variable file from being loaded silently.

---

## 6. 360-Day Calendar Issue and Implemented Fix

### Problem
UKCP18 regional projections use a **360-day calendar**. In this calendar:
- every month has exactly 30 days
- dates such as 31 January, 31 March, and 31 December do not exist
- February has 30 days instead of 28 or 29

That is valid within UKCP18, but it causes problems for this project because the demand model and historical weather data operate on a normal Gregorian daily calendar.

### Why this matters for this project
The project objective is to model future UK energy requirements using seasonal past performance and future climate. That requires the future temperature series to align properly with:
- Gregorian daily demand data
- bank holidays / weekday structure
- historical daily weather series

Leaving the future file on a 360-day calendar would make daily joins unreliable and would distort day-based demand features.

### Fix adopted
The notebook now produces **two** future outputs instead of one:

1. **Raw archive output**  
   `weather_future_daily_ukcp18_raw360.parquet`  
   - preserves the original UKCP18 360-day calendar exactly
   - keeps the source structure for traceability and auditability

2. **Modelling output**  
   `weather_future_daily_ukcp18.parquet`  
   - converts each `(region, climate_member, year)` from the 360-day source calendar to a standard Gregorian daily calendar
   - creates a modelling-ready daily file for downstream joins

### Alignment method used
The alignment is performed by **within-year interpolation by relative position through the year**.

For each `(region, climate_member, year)`:
- the 360 source days are mapped onto fractional positions through the year
- the Gregorian target days (365 or 366 depending on leap year) are mapped onto fractional positions through the same year
- `tasmin_c`, `tasmax_c`, and `tmean_c` are interpolated from the 360-day source positions onto the Gregorian target positions

In effect:
- the raw climate trajectory for each year is retained
- the year is stretched from 360 source days to 365/366 target days
- no Gregorian dates are missing in the aligned modelling file

### Why this fix is appropriate
This was chosen because it is the best fit for the project aims:

- It preserves the original UKCP18 data unchanged in a raw archive.
- It creates a clean daily Gregorian series for direct use in the demand model.
- It avoids dropping valid demand dates from the Gregorian side.
- It avoids forcing the whole project to monthly resolution too early.
- It keeps all ensemble members and both retained regions usable in a consistent way.

### Important interpretation note
The aligned future file is **not** a native Gregorian UKCP18 product. It is a derived daily modelling dataset created from the raw 360-day UKCP18 source via interpolation. For transparency, both files are retained.

---

## 7. UKCP18 Ensemble Members

The retained UKCP18 members are:

`member_01, member_04, member_05, member_06, member_07, member_08, member_09, member_10, member_11, member_12, member_13, member_15`

Members `02`, `03`, and `14` are intentionally absent. This is expected and reflects the UKCP18 ensemble design, not a download or preprocessing error.

---

## 8. QA Summary

### A — `weather_hist_daily.parquet`

| Check | Result |
|---|---|
| Regions retained | `eng_wales`, `scotland` |
| Date column type | Daily datetime, no time component |
| Duplicate (`date`, `region`) keys | 0 |
| Null temperature values | 0 |
| Missing dates within retained historical span | 0 per region |

### B — `weather_future_daily_ukcp18_raw360.parquet`

| Check | Result |
|---|---|
| Regions retained | `eng_wales`, `scotland` |
| Members per region | 12 |
| Calendar type | Original UKCP18 360-day calendar |
| Duplicate (`date`, `region`, `climate_member`) keys | 0 |
| Null temperature values | 0 |
| Dates per `(region, member, year)` | 360 |

### C — `weather_future_daily_ukcp18.parquet`

| Check | Result |
|---|---|
| Regions retained | `eng_wales`, `scotland` |
| Members per region | 12 |
| Calendar type | Gregorian daily calendar |
| Duplicate (`date`, `region`, `climate_member`) keys | 0 |
| Null temperature values | 0 |
| Dates per `(region, member, year)` | 365 or 366 as expected |
| Project horizon 2030–2045 present | Yes |

### D — `ukcp18_member_lookup.csv`

| Check | Result |
|---|---|
| Duplicate entries | 0 |
| Members 02, 03, 14 absent | True |

---

## 9. Practical Notes for the Modelling Team

- Use the **aligned Gregorian** future file for any daily merge with demand, calendar, or historical weather data.
- Use the **raw 360-day** future file only for provenance, validation, or reprocessing.
- Do not recompute `tmean_c` from `tasmin_c` and `tasmax_c` for UKCP18, because a separate UKCP18 mean-temperature file was provided and used directly.
- When checking annual row counts:
  - raw future file should have **360** days per year
  - aligned future file should have **365** or **366** days per year depending on leap year

---

## 10. Acceptance Checklist

- Only `eng_wales` and `scotland` included
- Daily `date` column present in all final outputs
- `tasmin_c`, `tasmax_c`, `tmean_c` present in historical and future outputs
- No duplicate keys in any final output
- Historical file uses Gregorian daily dates
- Raw UKCP18 file preserves the original 360-day calendar
- Aligned UKCP18 file provides a Gregorian daily series for modelling
- 360-day calendar issue is resolved for downstream daily joins
- UKCP18 member exclusions (`02`, `03`, `14`) documented and treated as expected
