# Repository Conventions

## Branch naming
Use short descriptive branch names:
- `data-cleaning-era5`
- `demand-model-gam`
- `presentation-updates`
- `repo-setup`

## Commit messages
Use simple present-tense messages:
- `Add initial repository structure`
- `Document data source register`
- `Move cleaning logic into src`

## Notebook rules
- notebooks are for exploration and reporting only
- reusable logic should eventually move into `src/`
- notebook names should be ordered and descriptive, e.g. `01_initial_checks.ipynb`
- clear noisy outputs before major commits unless the output is intentionally being preserved

## Data rules
- do not commit bulk raw data
- do not commit secrets or credentials
- record source, date accessed, and notes for each dataset
- keep only lightweight samples, schemas, or metadata in Git where needed

## Output rules
- only commit outputs that are intentionally worth preserving
- avoid versioning large temporary exports

## Documentation rules
- log major methodological or scope decisions in `docs/decisions/decision-log.md`