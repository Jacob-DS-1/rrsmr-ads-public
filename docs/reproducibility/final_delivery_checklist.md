# Final delivery checklist

This checklist is for a marker, client, reviewer, or team member who needs to rerun, audit, and inspect the final Rolls-Royce SMR ADS public-release workflow. The repository should be treated as a reproducible modelling workflow, not as a store of generated evidence files.

Prepared/source inputs are restored from the public Zenodo source-input bundle, generated outputs are rebuilt locally, and generated Parquet/CSV/PNG artifacts should remain ignored unless explicitly documented otherwise.

## 1. Environment check

Run from the repo root:

```bash
python --version
python -m pip check
jupyter kernelspec list | grep rrsmr-ads || true
```

For a fresh public clone, a clean kernel name is recommended:

```bash
python -m ipykernel install --user --name rrsmr-ads-fresh --display-name "Python (rrsmr-ads-fresh)"
```

## 2. Restore public Zenodo source inputs

Restore the external source-input bundle:

```bash
python scripts/prepare_source_inputs.py \
  --archive "https://zenodo.org/records/20073518/files/rrsmr-source-inputs-final-delivery.zip?download=1" \
  --expected-sha256 39666204bed374e6bda724f13a02daac920c86347fd8c7d70d358a9837b8219c \
  --dest external_data/source_inputs
```

Expected restored input roots:

* `external_data/source_inputs/objective1_raw`
* `external_data/source_inputs/ERA5`

The archive and restored `external_data/` contents are not committed to Git.

## 3. Full clean model run

Use the complete runner when rebuilding the full workflow from restored source inputs:

```bash
bash scripts/run_complete_model.sh \
  --clean \
  --kernel-name rrsmr-ads-fresh \
  --source-dir external_data/source_inputs/objective1_raw \
  --era5-source-dir external_data/source_inputs/ERA5
```

The complete runner performs:

1. preprocessing, unless explicitly skipped;
2. Objective 1 generation modelling;
3. Objective 2 demand modelling;
4. Objective 3 SMR fleet modelling;
5. Objective 3 demand/generation/SMR integration;
6. Objective 3 system-impact metrics;
7. Objective 3 final QA/dashboard sensitivity outputs;
8. reproducible output audit.

Generated data are written under ignored `data/processed/` and `outputs/` paths.

## 4. Reproducible output audit

After a full run, audit the generated outputs:

```bash
python scripts/audit_reproducible_outputs.py
```

Expected final status:

```text
Overall status: pass
```

The audit covers the main Objective 1, Objective 2, Objective 3, Owner 5 sensitivity, and figure-output contracts.

## 5. Dashboard data build

Build the dashboard input bundle from repo-generated outputs:

```bash
python dashboard/objective3_smr_integration/scripts/build_dashboard_data.py
```

Expected generated dashboard data are written under:

```text
outputs/dashboard/objective3_smr_integration/data/
```

This folder is ignored and should not be committed.

## 6. Dashboard run

Run the dashboard locally after building its data bundle:

```bash
python -m streamlit run dashboard/objective3_smr_integration/app.py
```

The dashboard should read from:

```text
outputs/dashboard/objective3_smr_integration/data/
```

The dashboard communicates Objective 3 system-impact results without requiring committed generated artifacts.

## 7. Standard validation chain

Run this before publishing the release:

```bash
python -m pytest -q
python scripts/audit_reproducible_outputs.py
git ls-files | grep -Ei '\.(parquet|pkl|joblib|pyc|png|jpe?g)$|__pycache__|\.egg-info|\.DS_Store|\.Rhistory' || true
git diff --check
git status --short
```

Expected final reference results:

* `python -m pytest -q`: `136 passed`
* `python scripts/audit_reproducible_outputs.py`: `Overall status: pass`
* forbidden tracked artifact check: no output
* `git diff --check`: no output
* `git status --short`: only intentional source/doc changes should appear before committing

## 8. Key output contracts

Objective 1 final output:

```text
data/processed/objective1_generation/generation_future_hourly_2030_2045
```

Expected reference shape and coverage:

* rows: 841,536
* date range: 2030-01-01 00:00 UTC to 2045-12-31 23:00 UTC
* FES scenarios: `Electric Engagement`, `Holistic Transition`
* weather years: 2010, 2014, 2015
* weather-year roles: `low_wind`, `average_wind`, `high_wind`
* duplicate required keys: 0
* required/model missing values: 0
* negative MW values: 0

Objective 2 final output:

```text
data/processed/objective2_demand/demand_future_hourly_2030_2045
```

Expected reference shape and coverage:

* rows: 841,536
* date range: 2030-01-01 00:00 UTC to 2045-12-31 23:00 UTC
* FES scenarios: `Electric Engagement`, `Holistic Transition`
* climate members: `member_06`, `member_12`, `member_13`
* duplicate keys: 0
* required missing values: 0

Objective 3 canonical outputs include:

```text
data/processed/objective3_smr_integration/smr_fleet_hourly_2030_2045
data/processed/objective3_smr_integration/grid_master_hourly_2030_2045
data/processed/objective3_smr_integration/system_impact_metrics_hourly_2030_2045.parquet
data/processed/objective3_smr_integration/system_impact_summary_annual_2030_2045.csv
data/processed/objective3_smr_integration/system_impact_summary_period_2030_2045.csv
```

Canonical Objective 3 outputs preserve all three weather roles. Owner 5 sensitivity/dashboard outputs intentionally use the selected average-wind and low-wind subset where documented.

## 9. Objective 3 interpretation caveats

The Objective 3 repo workflow is the improved/current final workflow, not a byte-for-byte preservation of the original local handoff.

The current workflow includes:

* staggered and simultaneous SMR commissioning cases;
* canonical weather-year roles: `average_wind`, `high_wind`, `low_wind`;
* explicit SMR planned-outage and forced-outage assumptions;
* canonical delivered-MW SMR columns;
* rule-based system-impact metrics, not a simple unmanaged merge.

Current SMR assumptions:

* each SMR delivers 470 MW when online;
* `net_delivery_factor = 1.0`;
* planned outage window: 18 days;
* planned outage frequency: every 24 months;
* forced outage rate: 2%;
* maximum three-unit SMR fleet output: 1,410 MW;
* realised post-commissioning capacity factor is approximately 95.3-95.6%.

Recommended report wording:

The final Objective 3 fleet model no longer applies a permanent 0.9 derating to SMR output. Each SMR is modelled as delivering 470 MW when online, with availability losses represented explicitly through an 18-day planned outage every 24 months and a deterministic 2% forced-outage loss. This produces realised post-commissioning capacity factors of approximately 95.3-95.6%.

## 10. Generated-output hygiene

Do not commit generated model or dashboard artifacts.

Use this check before committing:

```bash
git ls-files | grep -Ei '\.(parquet|pkl|joblib|pyc|png|jpe?g)$|__pycache__|\.egg-info|\.DS_Store|\.Rhistory' || true
```

Expected output is empty.

Generated files should remain under ignored locations such as:

* `data/processed/`
* `outputs/`
* `outputs/dashboard/`

Only source code, configuration, documentation, tests, notebooks, and scripts should normally be committed.

## 11. Public source-input bundle handover

Before publication, confirm:

* the public Zenodo bundle DOI is documented;
* the archive SHA-256 checksum is documented;
* the archive restores with `scripts/prepare_source_inputs.py`;
* the complete model runner works from `external_data/source_inputs/objective1_raw` and `external_data/source_inputs/ERA5`;
* `external_data/` and the source archive are not tracked by Git.

The repository should be described as reproducible from committed code plus the external Zenodo source-input bundle, not from clone alone.

## 12. Final external source-input archive

* Final Zenodo DOI: 10.5281/zenodo.20073518
* Final archive SHA-256: 39666204bed374e6bda724f13a02daac920c86347fd8c7d70d358a9837b8219c
* Final archive filename: `rrsmr-source-inputs-final-delivery.zip`
* Direct archive URL: https://zenodo.org/records/20073518/files/rrsmr-source-inputs-final-delivery.zip?download=1
* The archive is an external public-source reproducibility bundle, not Rolls-Royce proprietary data.
* Generated model/dashboard outputs remain excluded and are recreated by the workflow.
