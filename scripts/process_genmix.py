#!/usr/bin/env python3
"""
Person 2: Data cleaning and merging for GB electricity generation mix.
Processes df_fuel_ckan.csv and generates the required deliverables.

Directory structure:
  data/     - input data (df_fuel_ckan.csv)
  scripts/  - this script
  output/   - generated deliverables
"""

import os

import pandas as pd
from pathlib import Path

# Paths: project root is parent of scripts/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = Path(os.environ.get("RRSMR_GENMIX_OUTPUT_DIR", PROJECT_ROOT / "output"))

INPUT_CSV = Path(os.environ.get("RRSMR_GENMIX_INPUT_CSV", DATA_DIR / "df_fuel_ckan.csv"))
OUTPUT_PARQUET_HOURLY = OUTPUT_DIR / "genmix_hist_hourly.parquet"
OUTPUT_PARQUET_LIBRARY = OUTPUT_DIR / "genmix_profile_library.parquet"
OUTPUT_TAXONOMY_CSV = OUTPUT_DIR / "genmix_taxonomy_map.csv"


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------------------
    # 1. Load raw data
    # -------------------------------------------------------------------------
    print("Loading df_fuel_ckan.csv...")
    df = pd.read_csv(INPUT_CSV)

    # -------------------------------------------------------------------------
    # 2. Timezone: Convert DATETIME to UTC
    # -------------------------------------------------------------------------
    print("Converting DATETIME to UTC...")
    df["timestamp_utc"] = pd.to_datetime(df["DATETIME"], utc=True)

    # -------------------------------------------------------------------------
    # 3. Date filtering: 2010-01-01 to 2024-12-31
    # -------------------------------------------------------------------------
    print("Filtering date range 2010-01-01 to 2024-12-31...")
    df = df[
        (df["timestamp_utc"] >= "2010-01-01")
        & (df["timestamp_utc"] <= "2024-12-31 23:59:59")
    ].copy()

    # -------------------------------------------------------------------------
    # 4. Create wind_total_mw = WIND + WIND_EMB
    # -------------------------------------------------------------------------
    df["wind_total_mw"] = df["WIND"].fillna(0) + df["WIND_EMB"].fillna(0)

    # -------------------------------------------------------------------------
    # 5. Column mapping and selection
    # -------------------------------------------------------------------------
    col_map = {
        "GAS": "gas_mw",
        "COAL": "coal_mw",
        "NUCLEAR": "nuclear_mw",
        "SOLAR": "solar_mw",
        "HYDRO": "hydro_mw",
        "BIOMASS": "biomass_mw",
        "STORAGE": "storage_net_mw",
        "IMPORTS": "imports_net_mw",
        "OTHER": "other_mw",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Keep only our target columns (drop all _perc, CARBON_INTENSITY, LOW_CARBON, GENERATION, etc.)
    final_cols = [
        "timestamp_utc",
        "gas_mw",
        "coal_mw",
        "nuclear_mw",
        "wind_total_mw",
        "solar_mw",
        "hydro_mw",
        "biomass_mw",
        "storage_net_mw",
        "imports_net_mw",
    ]
    if "other_mw" in df.columns:
        final_cols.insert(-2, "other_mw")
    df = df[[c for c in final_cols if c in df.columns]].copy()

    # -------------------------------------------------------------------------
    # 6. Aggregate half-hourly to hourly using .mean()
    # -------------------------------------------------------------------------
    print("Aggregating half-hourly to hourly (mean)...")
    df["timestamp_hour"] = df["timestamp_utc"].dt.floor("h")
    df_hourly = (
        df.groupby("timestamp_hour", as_index=False)
        .mean(numeric_only=True)
    )
    df_hourly = df_hourly.rename(columns={"timestamp_hour": "timestamp_utc"})

    # -------------------------------------------------------------------------
    # DELIVERABLE A: genmix_hist_hourly.parquet
    # -------------------------------------------------------------------------
    print("Saving genmix_hist_hourly.parquet...")
    df_hourly.to_parquet(OUTPUT_PARQUET_HOURLY, index=False)

    # -------------------------------------------------------------------------
    # DELIVERABLE B: genmix_profile_library.parquet
    # -------------------------------------------------------------------------
    print("Building genmix_profile_library.parquet...")
    tech_cols = [c for c in df_hourly.columns if c != "timestamp_utc"]
    df_long = df_hourly.melt(
        id_vars=["timestamp_utc"],
        value_vars=tech_cols,
        var_name="tech",
        value_name="mw",
    )
    df_long["month"] = df_long["timestamp_utc"].dt.month
    df_long["hour"] = df_long["timestamp_utc"].dt.hour
    df_long["day_type"] = df_long["timestamp_utc"].dt.dayofweek.apply(
        lambda x: "weekday" if x < 5 else "weekend"
    )

    profile = (
        df_long.groupby(["tech", "month", "day_type", "hour"])["mw"]
        .quantile([0.10, 0.50, 0.90])
        .unstack()
        .reset_index()
    )
    profile.columns = ["tech", "month", "day_type", "hour", "p10_mw", "p50_mw", "p90_mw"]
    profile.to_parquet(OUTPUT_PARQUET_LIBRARY, index=False)

    # -------------------------------------------------------------------------
    # DELIVERABLE C: genmix_taxonomy_map.csv
    # -------------------------------------------------------------------------
    print("Saving genmix_taxonomy_map.csv...")

    taxonomy = [
        {
            "tech": "gas",
            "model_column": "gas_mw",
            "source_columns": "NESO:GAS; FES:Gas",
            "description": "Gas generation",
            "notes": "FES Gas maps to gas."
        },
        {
            "tech": "coal",
            "model_column": "coal_mw",
            "source_columns": "NESO:COAL",
            "description": "Coal generation",
            "notes": "Historic DUKES/NESO category; not necessarily retained in future FES supply."
        },
        {
            "tech": "nuclear",
            "model_column": "nuclear_mw",
            "source_columns": "NESO:NUCLEAR; FES:Nuclear",
            "description": "Existing/future non-SMR nuclear generation",
            "notes": "Future SMR units are added separately in Objective 3."
        },
        {
            "tech": "wind_total",
            "model_column": "wind_total_mw",
            "source_columns": "NESO:WIND + WIND_EMB; FES:Offshore Wind + Onshore Wind",
            "description": "Total wind generation",
            "notes": "Combines transmission-connected and embedded wind historically; combines offshore and onshore wind in FES."
        },
        {
            "tech": "solar",
            "model_column": "solar_mw",
            "source_columns": "NESO:SOLAR; FES:Solar PV",
            "description": "Solar generation",
            "notes": "FES Solar PV maps to solar."
        },
        {
            "tech": "hydro",
            "model_column": "hydro_mw",
            "source_columns": "NESO:HYDRO; FES:Hydro",
            "description": "Hydro generation",
            "notes": "Hydro mapped consistently across sources."
        },
        {
            "tech": "biomass",
            "model_column": "biomass_mw",
            "source_columns": "NESO:BIOMASS; FES:Biomass",
            "description": "Biomass generation",
            "notes": "Biomass excluding CCS biomass unless explicitly included elsewhere."
        },
        {
            "tech": "storage",
            "model_column": "storage_net_mw",
            "source_columns": "NESO:STORAGE; FES:Battery + Long Duration Energy Storage",
            "description": "Electricity storage",
            "notes": "Historic sign convention: positive = discharge/generation, negative = pumping/charging."
        },
        {
            "tech": "imports",
            "model_column": "imports_net_mw",
            "source_columns": "NESO:IMPORTS",
            "description": "Net imports",
            "notes": "Positive = importing to GB, negative = exporting from GB."
        },
        {
            "tech": "other",
            "model_column": "other_mw",
            "source_columns": "NESO:OTHER",
            "description": "Other historic generation",
            "notes": "Historic residual category."
        },
        {
            "tech": "other_thermal",
            "model_column": "other_thermal_mw",
            "source_columns": "FES:Other Thermal",
            "description": "Other thermal generation",
            "notes": "Used in future FES supply mapping."
        },
        {
            "tech": "other_renewable",
            "model_column": "other_renewable_mw",
            "source_columns": "FES:Other Renewable",
            "description": "Other renewable generation",
            "notes": "Used in future FES supply mapping."
        },
        {
            "tech": "hydrogen",
            "model_column": "hydrogen_mw",
            "source_columns": "FES:Hydrogen",
            "description": "Hydrogen generation",
            "notes": "Retained as separate FES future supply category."
        },
        {
            "tech": "gas_ccs",
            "model_column": "gas_ccs_mw",
            "source_columns": "FES:CCS Gas",
            "description": "Gas with carbon capture and storage",
            "notes": "Retained as separate FES future supply category."
        },
        {
            "tech": "biomass_ccs",
            "model_column": "biomass_ccs_mw",
            "source_columns": "FES:CCS Biomass",
            "description": "Biomass with carbon capture and storage",
            "notes": "Retained as separate FES future supply category."
        },
        {
            "tech": "waste",
            "model_column": "waste_mw",
            "source_columns": "FES:Waste",
            "description": "Waste generation",
            "notes": "Retained as separate FES future supply category."
        },
    ]
    
    df_taxonomy = pd.DataFrame(taxonomy)
    
    assert not df_taxonomy.duplicated(["tech"]).any()
    df_taxonomy.to_csv(OUTPUT_TAXONOMY_CSV, index=False)

    print("Done. All deliverables created in output/:")
    print(f"  - {OUTPUT_PARQUET_HOURLY}")
    print(f"  - {OUTPUT_PARQUET_LIBRARY}")
    print(f"  - {OUTPUT_TAXONOMY_CSV}")


if __name__ == "__main__":
    main()
