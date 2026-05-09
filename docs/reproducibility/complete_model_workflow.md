# Complete model workflow

This page documents the top-level reproducible workflow runner for the Rolls-Royce SMR ADS public release. The complete runner orchestrates the validated stage runners. Its purpose is to make the current repository workflow easier to rerun and audit from restored public source inputs.

## Source-input restore

Fresh-clone reproduction requires the external public Zenodo source-input bundle:

* DOI: 10.5281/zenodo.20073518
* Direct archive URL: https://zenodo.org/records/20073518/files/rrsmr-source-inputs-final-delivery.zip?download=1
* SHA-256: 39666204bed374e6bda724f13a02daac920c86347fd8c7d70d358a9837b8219c

Restore it from the repository root:

```bash
python scripts/prepare_source_inputs.py \
  --archive "https://zenodo.org/records/20073518/files/rrsmr-source-inputs-final-delivery.zip?download=1" \
  --expected-sha256 39666204bed374e6bda724f13a02daac920c86347fd8c7d70d358a9837b8219c \
  --dest external_data/source_inputs
```

The restored files are written to `external_data/source_inputs/`. The archive and restored external data are not committed to Git.

## Runner

Run from the repository root:

```bash
bash scripts/run_complete_model.sh --help
```

Clean full run from the restored Zenodo source inputs:

```bash
bash scripts/run_complete_model.sh \
  --clean \
  --kernel-name rrsmr-ads-fresh \
  --source-dir external_data/source_inputs/objective1_raw \
  --era5-source-dir external_data/source_inputs/ERA5
```

Run when shared preprocessing outputs already exist:

```bash
bash scripts/run_complete_model.sh --skip-preprocessing
```

Clean rerun when shared preprocessing outputs already exist:

```bash
bash scripts/run_complete_model.sh --skip-preprocessing --clean
```

The runner exports `KERNEL_NAME` for notebook-based stages and sets `PYTHONHASHSEED` to `0` unless it is already set.

## Execution order

The complete runner executes these stages:

1. Preprocessing, unless `--skip-preprocessing` is supplied.
2. Objective 1 generation.
3. Objective 2 demand.
4. Objective 3 SMR fleet.
5. Objective 3 Owner 3 grid integration.
6. Objective 3 Owner 4 system impact metrics.
7. Objective 3 Owner 5 final visualisation and QA package.
8. Reproducible output audit helper.

The Objective 3 integration runner normally rebuilds the SMR fleet itself. The complete runner calls the SMR fleet step explicitly, then calls integration with `--skip-smr-fleet` to avoid running the same step twice.

## Clean behaviour

When `--clean` is supplied:

* preprocessing receives `--clean`;
* Objective 1 receives `--clean`;
* Objective 2 receives `--clean`;
* known Objective 3 generated data and figure/output folders are removed by the complete runner;
* Objective 3 SMR fleet receives `--clean`;
* Objective 3 system impact receives `--clean`;
* Objective 3 final QA receives `--clean`.

The complete runner only removes generated paths under ignored generated-data/output locations. Generated artifacts remain ignored by Git.

## Audit scope

The final audit command is:

```bash
python scripts/audit_reproducible_outputs.py
```

Expected final status:

```text
Overall status: pass
```

The audit helper checks the main Objective 1, Objective 2, Objective 3, Owner 5 sensitivity, and figure-output contracts.

## Important interpretation notes

Objective 3 outputs should not be described as byte-for-byte unchanged from the original local handoff. The repository workflow is the improved current implementation.

The final Objective 3 fleet model no longer applies a permanent 0.9 derating to SMR output. Each SMR is modelled as delivering 470 MW when online, with availability losses represented explicitly through an 18-day planned outage every 24 months and a deterministic 2% forced-outage loss.

This produces realised post-commissioning capacity factors of approximately 95.3-95.6%.
