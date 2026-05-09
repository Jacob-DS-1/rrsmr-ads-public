# Source-input bundle

This public repository is designed to reproduce the project from a separate source-input bundle deposited on Zenodo.

## Zenodo record

DOI:

    10.5281/zenodo.20073518

Expected SHA-256 checksum for the source-input archive:

    39666204bed374e6bda724f13a02daac920c86347fd8c7d70d358a9837b8219c

## Expected local location

After downloading and extracting the archive, the repository should contain:

    external_data/source_inputs

The complete workflow expects these source directories:

    external_data/source_inputs/objective1_raw
    external_data/source_inputs/ERA5

## Why the bundle is separate

The Git repository contains code, documentation, tests, dashboard source, and small validation evidence files.

The source-input bundle is kept separate because it contains larger input files used to reproduce the modelling workflow.

## Checksum verification

On macOS, verify the downloaded archive with:

    shasum -a 256 path/to/source-input-archive.zip

The resulting checksum should match:

    39666204bed374e6bda724f13a02daac920c86347fd8c7d70d358a9837b8219c

## Restore location

Create the expected directory and extract the source-input archive so that:

    external_data/source_inputs/objective1_raw
    external_data/source_inputs/ERA5

exist before running the complete workflow.

## Complete workflow

From the repository root, run:

    bash scripts/run_complete_model.sh \
      --clean \
      --kernel-name rrsmr-ads-public \
      --source-dir external_data/source_inputs/objective1_raw \
      --era5-source-dir external_data/source_inputs/ERA5

## Audit

After the workflow finishes, run:

    python scripts/audit_reproducible_outputs.py

Expected result:

    Overall status: pass
