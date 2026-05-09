# ERA5 GB Hourly Resource Data — 2010–2024

## File
era5_resource_hourly_gb_2010_2024.parquet

## Columns
| Column               | Units  | Notes |
|----------------------|--------|-------|
| timestamp_utc        | —      | Hourly, UTC, 2010-01-01 00:00 - 2024-12-31 23:00 |
| wind_speed_100m_ms   | m/s    | Derived from ERA5 u100/v100: sqrt(u^2 + v^2) |
| ssrd_j_m2            | J/m^2  | ERA5 native units (hourly accumulation). To convert to mean W/m^2: divide by 3600 |

## Spatial aggregation
Capacity-weighted spatial mean with cosine-latitude area correction.
Each grid cell weight = installed capacity (MW) × cos(latitude).
Grid cells with no mapped capacity fall back to cosine-latitude weighting only.

    Wind:  weighted by operational onshore + offshore wind capacity (REPD)
    Solar: weighted by operational solar PV capacity (REPD)
    Domain: Lat 49.0–61.0 | Lon -12.0–4.0 | 0.25° resolution

Source: Renewable Energy Planning Database (REPD), DESNZ.

Note: REPD capacity weights reflect the fleet at time of download (snapshot).
A 2010 timestep receives the same weights as a 2024 timestep. This is a known
and accepted simplification in GB energy system modelling.

## Data cleaning
ssrd_j_m2: small negative values clipped to zero (ERA5 spectral artefact).
    Min value before clip: -1.902 J/m²

## Quality checks
    Duplicate timestamps: 0
    Missing hours 2010–2024: 0
    Missing values (wind): 0
    Missing values (ssrd): 0

## Source data
ERA5 reanalysis, ECMWF via Copernicus CDS.
All timestamps are UTC (encoding: hours since 1900-01-01 00:00:00.0 UTC).