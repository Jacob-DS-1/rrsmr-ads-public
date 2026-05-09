import json
import subprocess
import sys
import zipfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "scripts" / "prepare_source_inputs.py"


def write_manifest(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "archive_sha256": "",
                "required_directories": ["objective1_raw", "ERA5"],
            }
        )
    )


def make_zip_from_tree(source_root: Path, archive_path: Path) -> None:
    with zipfile.ZipFile(archive_path, "w") as archive:
        for file_path in source_root.rglob("*"):
            if file_path.is_file():
                archive.write(file_path, file_path.relative_to(source_root.parent))


def run_script(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        text=True,
        capture_output=True,
        check=False,
    )


def test_prepare_source_inputs_restores_required_directories(tmp_path: Path) -> None:
    bundle_root = tmp_path / "rrsmr-source-inputs-final-delivery"
    objective1_raw = bundle_root / "objective1_raw"
    era5 = bundle_root / "ERA5"
    objective1_raw.mkdir(parents=True)
    era5.mkdir(parents=True)
    (objective1_raw / "raw-file.csv").write_text("a,b\n1,2\n")
    (era5 / "era5-file.nc").write_text("placeholder")

    archive_path = tmp_path / "rrsmr-source-inputs-final-delivery.zip"
    make_zip_from_tree(bundle_root, archive_path)

    manifest_path = tmp_path / "source_data_bundle.json"
    write_manifest(manifest_path)

    dest = tmp_path / "external_data" / "source_inputs"

    result = run_script(
        "--archive",
        str(archive_path),
        "--dest",
        str(dest),
        "--manifest",
        str(manifest_path),
    )

    assert result.returncode == 0, result.stderr
    assert (dest / "objective1_raw" / "raw-file.csv").exists()
    assert (dest / "ERA5" / "era5-file.nc").exists()
    assert "Prepared source inputs" in result.stdout


def test_prepare_source_inputs_rejects_checksum_mismatch(tmp_path: Path) -> None:
    bundle_root = tmp_path / "bundle"
    (bundle_root / "objective1_raw").mkdir(parents=True)
    (bundle_root / "ERA5").mkdir(parents=True)
    (bundle_root / "objective1_raw" / "raw-file.csv").write_text("x")
    (bundle_root / "ERA5" / "era5-file.nc").write_text("y")

    archive_path = tmp_path / "bundle.zip"
    make_zip_from_tree(bundle_root, archive_path)

    manifest_path = tmp_path / "source_data_bundle.json"
    write_manifest(manifest_path)

    result = run_script(
        "--archive",
        str(archive_path),
        "--dest",
        str(tmp_path / "out"),
        "--manifest",
        str(manifest_path),
        "--expected-sha256",
        "0" * 64,
    )

    assert result.returncode != 0
    assert "checksum mismatch" in result.stderr


def test_prepare_source_inputs_fails_when_required_folder_missing(tmp_path: Path) -> None:
    bundle_root = tmp_path / "bundle"
    (bundle_root / "objective1_raw").mkdir(parents=True)
    (bundle_root / "objective1_raw" / "raw-file.csv").write_text("x")

    archive_path = tmp_path / "bundle.zip"
    make_zip_from_tree(bundle_root, archive_path)

    manifest_path = tmp_path / "source_data_bundle.json"
    write_manifest(manifest_path)

    result = run_script(
        "--archive",
        str(archive_path),
        "--dest",
        str(tmp_path / "out"),
        "--manifest",
        str(manifest_path),
    )

    assert result.returncode != 0
    assert "required directories" in result.stderr


def test_prepare_source_inputs_rejects_unsafe_zip_member(tmp_path: Path) -> None:
    archive_path = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("../evil.txt", "nope")
        archive.writestr("objective1_raw/raw-file.csv", "x")
        archive.writestr("ERA5/era5-file.nc", "y")

    manifest_path = tmp_path / "source_data_bundle.json"
    write_manifest(manifest_path)

    result = run_script(
        "--archive",
        str(archive_path),
        "--dest",
        str(tmp_path / "out"),
        "--manifest",
        str(manifest_path),
    )

    assert result.returncode != 0
    assert "unsafe path" in result.stderr
