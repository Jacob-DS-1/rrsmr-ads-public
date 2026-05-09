from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable


@dataclass(frozen=True)
class InputSpec:
    label: str
    patterns: tuple[str, ...]
    min_count: int = 1

    def matches(self, source_dir: Path) -> list[Path]:
        found: dict[Path, None] = {}
        for pattern in self.patterns:
            for path in source_dir.glob(pattern):
                if path.is_file():
                    found[path] = None
        return sorted(found)


@dataclass(frozen=True)
class OutputSpec:
    source: str
    dest: str


@dataclass(frozen=True)
class Stage:
    name: str
    description: str
    inputs: tuple[InputSpec, ...]
    outputs: tuple[OutputSpec, ...]
    prepare: str
    notebooks: tuple[str, ...] = ()
    script: str | None = None


CANONICAL_OUTPUTS = (
    "demand_hist_hourly.parquet",
    "demand_hist_daily.parquet",
    "demand_hourly_shape_library.parquet",
    "genmix_hist_hourly.parquet",
    "genmix_profile_library.parquet",
    "fes_demand_annual_2030_2045.parquet",
    "fes_supply_annual_2030_2045.parquet",
    "weather_hist_daily.parquet",
    "weather_future_daily_ukcp18.parquet",
    "ukcp18_member_lookup.csv",
    "era5_resource_hourly_gb_2010_2024.parquet",
    "calendar_hourly_2010_2045.parquet",
    "dukes_capacity_hist_2010_2024.parquet",
    "dukes_loadfactor_hist_2010_2024.parquet",
    "interconnector_annual_hist_2010_2024.parquet",
    "holiday_calendar_2010_2045.csv",
)

STAGES: tuple[Stage, ...] = (
    Stage(
        name="holiday_calendar",
        description="Build GB holiday calendar from GOV.UK bank-holidays JSON.",
        inputs=(
            InputSpec(
                "GOV.UK bank-holidays JSON",
                ("neso/holidays/bank-holidays.json", "**/bank-holidays.json"),
            ),
        ),
        outputs=(OutputSpec("holiday_calendar_2010_2045.csv", "holiday_calendar_2010_2045.csv"),),
        prepare="holiday_calendar",
        notebooks=("notebooks/preprocessing/person5/person5_holiday_calendar.ipynb",),
    ),
    Stage(
        name="hourly_calendar",
        description="Build hourly UTC calendar table from the holiday calendar.",
        inputs=(),
        outputs=(OutputSpec("calendar_hourly_2010_2045.parquet", "calendar_hourly_2010_2045.parquet"),),
        prepare="hourly_calendar",
        notebooks=("notebooks/preprocessing/person5/person5_hourly_calendar_table.ipynb",),
    ),
    Stage(
        name="historic_demand",
        description="Build hourly/daily historic demand and demand shape library.",
        inputs=(
            InputSpec(
                "NESO historic demand annual CSVs",
                ("neso/historic_demand/demanddata_*.csv", "**/demanddata_*.csv"),
                min_count=15,
            ),
        ),
        outputs=(
            OutputSpec("demand_hist_hourly.parquet", "demand_hist_hourly.parquet"),
            OutputSpec("demand_hist_daily.parquet", "demand_hist_daily.parquet"),
            OutputSpec("demand_hourly_shape_library.parquet", "demand_hourly_shape_library.parquet"),
        ),
        prepare="historic_demand",
        notebooks=("notebooks/preprocessing/person1/Historic Demand Pre-processing Final.ipynb",),
    ),
    Stage(
        name="generation_mix",
        description="Build historic generation mix and generation profile library.",
        inputs=(
            InputSpec(
                "NESO historic generation mix CSV",
                ("neso/historic_generation_mix/df_fuel_ckan.csv", "**/df_fuel_ckan.csv"),
            ),
        ),
        outputs=(
            OutputSpec("output/genmix_hist_hourly.parquet", "genmix_hist_hourly.parquet"),
            OutputSpec("output/genmix_profile_library.parquet", "genmix_profile_library.parquet"),
        ),
        prepare="generation_mix",
        script="process_genmix",
    ),
    Stage(
        name="dukes_reference_tables",
        description="Build DUKES capacity, load-factor, and interconnector reference tables.",
        inputs=(
            InputSpec("DUKES 5.7 workbook", ("dukes/**/DUKES_5.7*.xlsx", "**/DUKES_5.7*.xlsx")),
            InputSpec("DUKES 5.10 workbook", ("dukes/**/DUKES_5.10*.xlsx", "**/DUKES_5.10*.xlsx")),
            InputSpec("DUKES 5.13 workbook", ("dukes/**/DUKES_5.13*.xlsx", "**/DUKES_5.13*.xlsx")),
            InputSpec("DUKES 6.2 workbook", ("dukes/**/DUKES_6.2*.xlsx", "**/DUKES_6.2*.xlsx")),
            InputSpec("DUKES 6.3 workbook", ("dukes/**/DUKES_6.3*.xlsx", "**/DUKES_6.3*.xlsx")),
        ),
        outputs=(
            OutputSpec("output/dukes_capacity_hist_2010_2024.parquet", "dukes_capacity_hist_2010_2024.parquet"),
            OutputSpec("output/dukes_loadfactor_hist_2010_2024.parquet", "dukes_loadfactor_hist_2010_2024.parquet"),
            OutputSpec("output/interconnector_annual_hist_2010_2024.parquet", "interconnector_annual_hist_2010_2024.parquet"),
        ),
        prepare="dukes_reference_tables",
        notebooks=(
            "notebooks/preprocessing/person3/dukes_5_7.ipynb",
            "notebooks/preprocessing/person3/dukes_5_10.ipynb",
            "notebooks/preprocessing/person3/dukes_5_13.ipynb",
            "notebooks/preprocessing/person3/6_2__and_6_3_input.ipynb",
        ),
    ),
    Stage(
        name="fes_annual_tables",
        description="Build FES annual demand and supply anchor tables.",
        inputs=(
            InputSpec(
                "FES ES1 Supply CSV",
                ("fes/**/fes2025_es1*.csv", "fes/**/ES1 Supply 2025.csv", "**/fes2025_es1*.csv"),
            ),
            InputSpec(
                "FES ES1 Demand CSV",
                ("fes/**/fes2025_ed1*.csv", "fes/**/ES1 Demand 2025.csv", "**/fes2025_ed1*.csv"),
            ),
        ),
        outputs=(
            OutputSpec("output/fes_supply_annual_2030_2045.parquet", "fes_supply_annual_2030_2045.parquet"),
            OutputSpec("output/fes_demand_annual_2030_2045.parquet", "fes_demand_annual_2030_2045.parquet"),
        ),
        prepare="fes_annual_tables",
        notebooks=("notebooks/preprocessing/person3/fes.ipynb",),
    ),
    Stage(
        name="weather_tables",
        description="Build HadUK historic weather and UKCP18 future climate tables.",
        inputs=(
            InputSpec("HadUK tasmax NetCDF", ("haduk/**/tasmax_hadukgrid_uk_country_day_19310101-20241231.nc", "**/tasmax_hadukgrid_uk_country_day_19310101-20241231.nc")),
            InputSpec("HadUK tasmin NetCDF", ("haduk/**/tasmin_hadukgrid_uk_country_day_19310101-20241231.nc", "**/tasmin_hadukgrid_uk_country_day_19310101-20241231.nc")),
            InputSpec("UKCP18 England/Wales max temperature CSV", ("ukcp18/**/Eng_Wales_Max_Temp_2010_2050/subset_*.csv",)),
            InputSpec("UKCP18 England/Wales mean temperature CSV", ("ukcp18/**/Eng_Wales_Mean_Temp_2010_2050/subset_*.csv",)),
            InputSpec("UKCP18 England/Wales min temperature CSV", ("ukcp18/**/Eng_Wales_Min_Temp_2010_2050/subset_*.csv",)),
            InputSpec("UKCP18 Scotland max temperature CSV", ("ukcp18/**/Scot_Max_Temp_2010_2050/subset_*.csv",)),
            InputSpec("UKCP18 Scotland mean temperature CSV", ("ukcp18/**/Scot_Mean_Temp_2010_2050/subset_*.csv",)),
            InputSpec("UKCP18 Scotland min temperature CSV", ("ukcp18/**/Scot_Min_Temp_2010_2050/subset_*.csv",)),
        ),
        outputs=(
            OutputSpec("weather_hist_daily.parquet", "weather_hist_daily.parquet"),
            OutputSpec("weather_future_daily_ukcp18.parquet", "weather_future_daily_ukcp18.parquet"),
            OutputSpec("ukcp18_member_lookup.csv", "ukcp18_member_lookup.csv"),
        ),
        prepare="weather_tables",
        notebooks=("notebooks/preprocessing/person4/person4_weather_preprocessing.ipynb",),
    ),
    Stage(
        name="era5_resource",
        description="Build ERA5 wind/solar resource table.",
        inputs=(
            InputSpec("ERA5 100m u-component wind NetCDFs", ("era5/**/*u-component of wind*.nc", "era5/**/*u_wind*.nc", "era5/**/*u100*.nc", "*100m_u_wind*.nc", "**/*100m_u_wind*.nc"), min_count=8),
            InputSpec("ERA5 100m v-component wind NetCDFs", ("era5/**/*v-component of wind*.nc", "era5/**/*v_wind*.nc", "era5/**/*v100*.nc", "*100m_v_wind*.nc", "**/*100m_v_wind*.nc"), min_count=8),
            InputSpec("ERA5 surface solar radiation NetCDFs", ("era5/**/*ssrd*.nc", "era5/**/*solar*.nc", "*ssrd*.nc", "**/*ssrd*.nc"), min_count=1),
            InputSpec("REPD workbook", ("era5/**/repd.xlsx", "repd.xlsx", "**/repd.xlsx"), min_count=1),
        ),
        outputs=(OutputSpec("era5_resource_hourly_gb_2010_2024.parquet", "era5_resource_hourly_gb_2010_2024.parquet"),),
        prepare="era5_resource",
        notebooks=("notebooks/preprocessing/person5/person5_era5_cleaning.ipynb",),
    ),
)


def find_repo_root(start: Path | None = None) -> Path:
    current = (start or Path.cwd()).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "config" / "paths.yaml").is_file():
            return candidate
    raise FileNotFoundError("Could not locate repo root containing config/paths.yaml")


def _link_or_copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() or dst.is_symlink():
        dst.unlink()
    try:
        os.symlink(src.resolve(), dst)
    except OSError:
        shutil.copy2(src, dst)


def _first(source_dir: Path, spec: InputSpec) -> Path:
    matches = spec.matches(source_dir)
    if len(matches) < spec.min_count:
        raise FileNotFoundError(spec.label)
    return matches[0]


def _all(source_dir: Path, spec: InputSpec) -> list[Path]:
    matches = spec.matches(source_dir)
    if len(matches) < spec.min_count:
        raise FileNotFoundError(spec.label)
    return matches


def source_dir_for_stage(stage: Stage, source_dir: Path, era5_source_dir: Path | None = None) -> Path:
    if stage.name == "era5_resource" and era5_source_dir is not None:
        return era5_source_dir
    return source_dir


def collect_missing_inputs(source_dir: Path, era5_source_dir: Path | None = None) -> dict[str, list[str]]:
    missing: dict[str, list[str]] = {}
    for stage in STAGES:
        stage_source_dir = source_dir_for_stage(stage, source_dir, era5_source_dir)
        stage_missing: list[str] = []
        for spec in stage.inputs:
            if len(spec.matches(stage_source_dir)) < spec.min_count:
                patterns = ", ".join(spec.patterns)
                stage_missing.append(f"{spec.label} needs at least {spec.min_count} match(es): {patterns}")
        if stage_missing:
            missing[stage.name] = stage_missing
    return missing


def format_missing_inputs(source_dir: Path, missing: dict[str, list[str]]) -> str:
    lines = [
        "Missing preprocessing source inputs.",
        f"Checked source directory: {source_dir}",
        "Pass the local raw bundle with --source-dir, for example:",
        "  scripts/run_preprocessing.sh --source-dir /path/to/source/raw",
        "",
        "Missing by stage:",
    ]
    for stage_name, items in missing.items():
        lines.append(f"- {stage_name}")
        for item in items:
            lines.append(f"  - {item}")
    return "\n".join(lines)


def canonical_output_paths(output_dir: Path) -> list[Path]:
    return [output_dir / name for name in CANONICAL_OUTPUTS]


def missing_canonical_outputs(output_dir: Path) -> list[Path]:
    return [path for path in canonical_output_paths(output_dir) if not path.exists()]


def _stage_outputs_exist(output_dir: Path, stage: Stage) -> bool:
    return all((output_dir / output.dest).exists() for output in stage.outputs)


def _clear_path(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)


def _clean_outputs(output_dir: Path) -> None:
    for path in canonical_output_paths(output_dir):
        if path.exists() or path.is_symlink():
            _clear_path(path)


def prepare_stage(source_dir: Path, output_dir: Path, stage: Stage, work_dir: Path) -> None:
    work_dir.mkdir(parents=True, exist_ok=True)

    if stage.prepare == "holiday_calendar":
        src = _first(source_dir, stage.inputs[0])
        _link_or_copy(src, work_dir / "bank-holidays.json")
        return

    if stage.prepare == "hourly_calendar":
        _link_or_copy(output_dir / "holiday_calendar_2010_2045.csv", work_dir / "holiday_calendar_2010_2045.csv")
        return

    if stage.prepare == "historic_demand":
        demand_files = _all(source_dir, stage.inputs[0])
        for src in demand_files:
            match = re.search(r"demanddata_(\d{4})\.csv$", src.name)
            if not match:
                continue
            year = int(match.group(1))
            if 2010 <= year <= 2024:
                _link_or_copy(src, work_dir / src.name)
        _link_or_copy(output_dir / "holiday_calendar_2010_2045.csv", work_dir / "holiday_calendar_2010_2045.csv")
        return

    if stage.prepare == "generation_mix":
        src = _first(source_dir, stage.inputs[0])
        _link_or_copy(src, work_dir / "df_fuel_ckan.csv")
        return

    if stage.prepare == "dukes_reference_tables":
        expected = {
            "DUKES_5.7_Plant capacity, United Kingdom.xlsx": stage.inputs[0],
            "DUKES_5.10_Plant loads, demand and efficiency.xlsx": stage.inputs[1],
            "DUKES_5.13_Capacity, net imports and utilisation of interconnectors.xlsx": stage.inputs[2],
            "DUKES_6.2_Capacity of, generation from renewable sources and shares of total generation.xlsx": stage.inputs[3],
            "DUKES_6.3_Load factors for renewable electricity generation.xlsx": stage.inputs[4],
        }
        for dest_name, spec in expected.items():
            _link_or_copy(_first(source_dir, spec), work_dir / "dukes_raw" / dest_name)
        return

    if stage.prepare == "fes_annual_tables":
        _link_or_copy(_first(source_dir, stage.inputs[0]), work_dir / "fes_raw" / "ES1 Supply 2025.csv")
        _link_or_copy(_first(source_dir, stage.inputs[1]), work_dir / "fes_raw" / "ES1 Demand 2025.csv")
        return

    if stage.prepare == "weather_tables":
        names = {
            "tasmax_hadukgrid_uk_country_day_19310101-20241231.nc": stage.inputs[0],
            "tasmin_hadukgrid_uk_country_day_19310101-20241231.nc": stage.inputs[1],
            "Eng_Wales_Max_Temp_2010_2050.csv": stage.inputs[2],
            "Eng_Wales_Mean_Temp_2010_2050.csv": stage.inputs[3],
            "Eng_Wales_Min_Temp_2010_2050.csv": stage.inputs[4],
            "Scot_Max_Temp_2010_2050.csv": stage.inputs[5],
            "Scot_Mean_Temp_2010_2050.csv": stage.inputs[6],
            "Scot_Min_Temp_2010_2050.csv": stage.inputs[7],
        }
        for dest_name, spec in names.items():
            _link_or_copy(_first(source_dir, spec), work_dir / dest_name)
        return

    if stage.prepare == "era5_resource":
        for src in _all(source_dir, stage.inputs[0]):
            suffix = src.name.split("_")[-1]
            _link_or_copy(src, work_dir / f"100m_u_wind_{suffix}")
        for src in _all(source_dir, stage.inputs[1]):
            suffix = src.name.split("_")[-1]
            _link_or_copy(src, work_dir / f"100m_v_wind_{suffix}")
        for idx, src in enumerate(_all(source_dir, stage.inputs[2]), start=1):
            _link_or_copy(src, work_dir / f"ssrd_{idx:03d}.nc")
        _link_or_copy(_first(source_dir, stage.inputs[3]), work_dir / "repd.xlsx")
        return

    raise ValueError(f"Unknown stage prepare mode: {stage.prepare}")


def _patch_notebook_for_stage(notebook, stage: Stage):
    for cell in notebook.cells:
        if cell.get("cell_type") != "code":
            continue

        lines = cell.get("source", "").splitlines()
        patched_lines = []
        for line in lines:
            if re.match(r"^\s*import\s+seaborn\s+as\s+sns\s*$", line):
                patched_lines.append("# Optional plotting dependency removed by preprocessing runner: seaborn")
            elif re.match(r"^\s*%load_ext\s+watermark\s*$", line):
                patched_lines.append("# Optional notebook metadata dependency removed by preprocessing runner: watermark")
            elif re.match(r"^\s*%watermark\b", line):
                patched_lines.append("# Optional notebook metadata command removed by preprocessing runner: watermark")
            elif re.match(r"^\s*sns\.", line):
                patched_lines.append("# Optional seaborn plotting call removed by preprocessing runner")
            else:
                line = line.replace('.dt.floor("H")', '.dt.floor("h")')
                line = line.replace(".dt.floor('H')", ".dt.floor('h')")
                line = line.replace('freq="H"', 'freq="h"')
                line = line.replace("freq='H'", "freq='h'")
                if ".apply(align_360day_year)" in line:
                    indent = line[: len(line) - len(line.lstrip())]
                    line = (
                        indent
                        + ".apply(lambda group: align_360day_year(group.assign("
                        + "region=group.name[0], climate_member=group.name[1], year=group.name[2]"
                        + ")))"
                    )
                patched_lines.append(line)
        cell["source"] = "\n".join(patched_lines)

    if stage.name != "historic_demand":
        return notebook

    replaced = False
    for cell in notebook.cells:
        if cell.get("cell_type") != "code":
            continue
        lines = cell.get("source", "").splitlines()
        new_lines = []
        for line in lines:
            if re.match(r"^\s*folder\s*=", line):
                new_lines.append('folder = "."')
                replaced = True
            else:
                new_lines.append(line)
        cell["source"] = "\n".join(new_lines)

    if not replaced:
        import nbformat

        notebook.cells.insert(0, nbformat.v4.new_code_cell('folder = "."'))
    return notebook


def execute_notebook(repo_root: Path, notebook_rel: str, work_dir: Path, executed_dir: Path, kernel_name: str, stage: Stage) -> None:
    import nbformat
    from nbclient import NotebookClient

    notebook_path = repo_root / notebook_rel
    executed_dir.mkdir(parents=True, exist_ok=True)

    notebook = nbformat.read(notebook_path, as_version=4)
    notebook = _patch_notebook_for_stage(notebook, stage)

    client = NotebookClient(
        notebook,
        timeout=-1,
        kernel_name=kernel_name,
        resources={"metadata": {"path": str(work_dir)}},
    )
    old_cwd = Path.cwd()
    try:
        os.chdir(work_dir)
        client.execute()
    finally:
        os.chdir(old_cwd)

    safe_name = stage.name + "__" + Path(notebook_rel).stem.replace(" ", "_") + ".ipynb"
    nbformat.write(notebook, executed_dir / safe_name)


def run_genmix_script(repo_root: Path, work_dir: Path) -> None:
    env = os.environ.copy()
    env["RRSMR_GENMIX_INPUT_CSV"] = str(work_dir / "df_fuel_ckan.csv")
    env["RRSMR_GENMIX_OUTPUT_DIR"] = str(work_dir / "output")
    subprocess.run(
        [sys.executable, str(repo_root / "scripts" / "process_genmix.py")],
        check=True,
        cwd=repo_root,
        env=env,
    )


def copy_stage_outputs(stage: Stage, work_dir: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for output in stage.outputs:
        src = work_dir / output.source
        dst = output_dir / output.dest
        if not src.exists():
            raise FileNotFoundError(f"Stage {stage.name} did not create expected output: {src}")
        shutil.copy2(src, dst)


def run(args: argparse.Namespace) -> int:
    repo_root = find_repo_root()
    source_dir = args.source_dir.resolve()
    era5_source_dir = args.era5_source_dir.resolve() if args.era5_source_dir is not None else None
    output_dir = (repo_root / args.output_dir).resolve() if not args.output_dir.is_absolute() else args.output_dir.resolve()
    work_root = args.work_dir.resolve()
    executed_dir = args.executed_notebooks_dir.resolve()

    if args.clean:
        if work_root.exists():
            shutil.rmtree(work_root)
        if executed_dir.exists():
            shutil.rmtree(executed_dir)
        _clean_outputs(output_dir)

    missing = collect_missing_inputs(source_dir, era5_source_dir)
    if args.check_only:
        if missing:
            print(format_missing_inputs(source_dir, missing))
            return 1
        print("All declared preprocessing source inputs are present.")
        return 0

    output_dir.mkdir(parents=True, exist_ok=True)
    work_root.mkdir(parents=True, exist_ok=True)
    executed_dir.mkdir(parents=True, exist_ok=True)

    for stage in STAGES:
        stage_missing = {stage.name: missing[stage.name]} if stage.name in missing else {}
        if stage_missing:
            if args.allow_existing_outputs and _stage_outputs_exist(output_dir, stage):
                print(f"Skipping {stage.name}: source inputs missing, but canonical output(s) already exist.")
                continue
            print(format_missing_inputs(source_dir, stage_missing), file=sys.stderr)
            return 1

        print(f"Running preprocessing stage: {stage.name}")
        stage_work_dir = work_root / stage.name
        if stage_work_dir.exists():
            shutil.rmtree(stage_work_dir)
        stage_work_dir.mkdir(parents=True, exist_ok=True)

        stage_source_dir = source_dir_for_stage(stage, source_dir, era5_source_dir)
        prepare_stage(stage_source_dir, output_dir, stage, stage_work_dir)

        if stage.script == "process_genmix":
            run_genmix_script(repo_root, stage_work_dir)

        for notebook_rel in stage.notebooks:
            execute_notebook(repo_root, notebook_rel, stage_work_dir, executed_dir, args.kernel_name, stage)

        copy_stage_outputs(stage, stage_work_dir, output_dir)

    missing_outputs = missing_canonical_outputs(output_dir)
    if missing_outputs:
        print("Preprocessing run finished but required canonical outputs are missing:", file=sys.stderr)
        for path in missing_outputs:
            print(f"  {path}", file=sys.stderr)
        return 1

    print("Preprocessing outputs verified under:")
    print(f"  {output_dir}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run or verify the shared preprocessing workflow.")
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("data/raw/preprocessing"),
        help="Directory containing raw preprocessing inputs, usually the source-population data/raw bundle.",
    )
    parser.add_argument(
        "--era5-source-dir",
        type=Path,
        default=None,
        help="Optional separate directory containing ERA5 NetCDFs and repd.xlsx.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/processed/preprocessing"),
        help="Canonical preprocessing output directory.",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        default=Path("/tmp/rrsmr-ads-preprocessing-work"),
        help="Temporary working directory used to stage notebook-compatible inputs.",
    )
    parser.add_argument(
        "--executed-notebooks-dir",
        type=Path,
        default=Path("/tmp/rrsmr-ads-preprocessing-executed-notebooks"),
        help="Directory for executed notebook copies.",
    )
    parser.add_argument("--kernel-name", default=os.environ.get("KERNEL_NAME", "python3"))
    parser.add_argument("--check-only", action="store_true", help="Only check declared source inputs; do not execute stages.")
    parser.add_argument("--allow-existing-outputs", action="store_true", help="Allow stages with missing raw inputs to use existing ignored canonical outputs.")
    parser.add_argument("--clean", action="store_true", help="Remove staged work, executed notebooks, and canonical preprocessing outputs before running.")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
