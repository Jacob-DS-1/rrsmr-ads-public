# SMRs on the National Grid

Public, sanitised release repository for the University of Manchester Applied Data Science project with Rolls-Royce SMR.

This repository contains the reproducible workflow for modelling the potential contribution of three Rolls-Royce small modular reactors at Wylfa to the Great Britain electricity system from 2030 to 2045.

## Project overview

The project models how future GB electricity demand, future GB generation, and a three-unit SMR fleet interact under selected Future Energy Scenarios and weather/climate sensitivities.

The workflow is organised into four main layers:

1. Shared preprocessing creates cleaned inputs used across the project.
2. Objective 1 models future GB generation.
3. Objective 2 models future GB electricity demand.
4. Objective 3 integrates demand, generation, and SMR fleet output into grid/system-impact metrics.

The repository is intended to be auditable, rerunnable from public source inputs, clear about modelling assumptions, and free of committed generated data artifacts. It is not a store of generated Parquet, CSV, or PNG outputs.

## Environment setup

Reproduction starts from a fresh clone of this public sanitised repository, followed by restoration of the external public Zenodo source-input bundle.

Create and activate a Python virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

Install project dependencies:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Register the Jupyter kernel used by the workflow:

```bash
python -m ipykernel install --user --name rrsmr-ads-fresh --display-name "Python (rrsmr-ads-fresh)"
```

Check the environment before restoring data or running the workflow:

```bash
python --version
python -m pytest -q tests/test_prepare_source_inputs.py
```

## Reproducing the workflow

The repository is reproducible from committed code plus the external public Zenodo source-input bundle. It is not reproducible from `git clone` alone because large third-party source inputs are intentionally stored outside Git.

Final public source-input bundle:

* DOI: 10.5281/zenodo.20073518
* Landing page: https://doi.org/10.5281/zenodo.20073518
* Direct archive URL: https://zenodo.org/records/20073518/files/rrsmr-source-inputs-final-delivery.zip?download=1
* SHA-256: 39666204bed374e6bda724f13a02daac920c86347fd8c7d70d358a9837b8219c

Restore the source inputs:

```bash
python scripts/prepare_source_inputs.py \
  --archive "https://zenodo.org/records/20073518/files/rrsmr-source-inputs-final-delivery.zip?download=1" \
  --expected-sha256 39666204bed374e6bda724f13a02daac920c86347fd8c7d70d358a9837b8219c \
  --dest external_data/source_inputs
```

Run the complete workflow:

```bash
bash scripts/run_complete_model.sh \
  --clean \
  --kernel-name rrsmr-ads-fresh \
  --source-dir external_data/source_inputs/objective1_raw \
  --era5-source-dir external_data/source_inputs/ERA5
```

Audit the generated outputs:

```bash
python scripts/audit_reproducible_outputs.py
python -m pytest -q
```

Expected reference status:

* `python scripts/audit_reproducible_outputs.py`: `Overall status: pass`
* `python -m pytest -q`: `136 passed`

## Repository map

* `config/` model configuration and SMR assumptions
* `dashboard/` Streamlit dashboard for Objective 3 results
* `data/` ignored generated and prepared data locations, with placeholders only committed
* `docs/` reproducibility notes, validation evidence, project documentation, and delivery checklist
* `notebooks/` migrated notebook workflows, mainly Objective 2 demand modelling
* `outputs/` ignored generated figures, tables, dashboard extracts, and presentation assets
* `scripts/` reproducible command-line runners and audit helpers
* `src/` reusable project code
* `tests/` automated tests for runners, schemas, and path behaviour

Generated model and dashboard outputs under `data/processed/`, `outputs/`, and `outputs/dashboard/` are intentionally ignored and should not be committed.

Key generated output locations include:

* `data/processed/objective1_generation/generation_future_hourly_2030_2045`
* `data/processed/objective2_demand/demand_future_hourly_2030_2045`
* `data/processed/objective3_smr_integration/grid_master_hourly_2030_2045`
* `outputs/dashboard/objective3_smr_integration/data/`

## Validation

Useful validation commands from the repository root:

```bash
python -m pytest -q
python scripts/audit_reproducible_outputs.py
git ls-files | grep -Ei '\.(parquet|pkl|joblib|pyc|png|jpe?g)$|__pycache__|\.egg-info|\.DS_Store|\.Rhistory' || true
git diff --check
```

Expected final reference status:

* tests: `136 passed`
* reproducible output audit: `Overall status: pass`
* forbidden tracked artifact check: no output
* whitespace check: no output

## Dashboard

Build dashboard data from repo-generated Objective 3 outputs:

```bash
python dashboard/objective3_smr_integration/scripts/build_dashboard_data.py
```

Run the dashboard locally:

```bash
python -m streamlit run dashboard/objective3_smr_integration/app.py
```

Dashboard documentation is in `dashboard/objective3_smr_integration/README.md`.

Generated dashboard data are written under `outputs/dashboard/objective3_smr_integration/data/`. This folder is ignored and should not be committed.
