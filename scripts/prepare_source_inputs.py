#!/usr/bin/env python3
"""Restore external RR SMR ADS source inputs from a local or remote archive."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import stat
import sys
import tarfile
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from urllib.parse import urlparse


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = REPO_ROOT / "config" / "source_data_bundle.json"
DEFAULT_REQUIRED_DIRS = ["objective1_raw", "ERA5"]


class BundleError(RuntimeError):
    """Raised when the source-data bundle cannot be restored safely."""


def is_url(value: str) -> bool:
    return urlparse(value).scheme.lower() in {"http", "https"}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_manifest(path: Path) -> dict:
    if not path.exists():
        raise BundleError(f"manifest not found: {path}")
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise BundleError(f"manifest is not valid JSON: {path}: {exc}") from exc


def required_dirs_from_manifest(manifest: dict) -> list[str]:
    required_dirs = manifest.get("required_directories") or DEFAULT_REQUIRED_DIRS
    if not isinstance(required_dirs, list) or not required_dirs:
        raise BundleError("manifest required_directories must be a non-empty list")

    out = []
    for name in required_dirs:
        if not isinstance(name, str) or not name.strip():
            raise BundleError("required directory names must be non-empty strings")
        clean = name.strip().replace("\\", "/")
        path = Path(clean)
        if path.is_absolute() or ".." in path.parts:
            raise BundleError(f"unsafe required directory name: {name}")
        out.append(clean)
    return out


def assert_safe_member(base: Path, member_name: str) -> None:
    member_path = Path(member_name)
    if member_path.is_absolute() or ".." in member_path.parts:
        raise BundleError(f"archive contains unsafe path: {member_name}")

    target = (base / member_path).resolve()
    try:
        target.relative_to(base.resolve())
    except ValueError as exc:
        raise BundleError(f"archive member would extract outside destination: {member_name}") from exc


def extract_archive(archive_path: Path, extract_root: Path) -> None:
    extract_root.mkdir(parents=True, exist_ok=True)

    if zipfile.is_zipfile(archive_path):
        with zipfile.ZipFile(archive_path) as archive:
            for info in archive.infolist():
                assert_safe_member(extract_root, info.filename)
                mode = info.external_attr >> 16
                if stat.S_ISLNK(mode):
                    raise BundleError(f"zip archive contains unsupported symlink: {info.filename}")
            archive.extractall(extract_root)
        return

    if tarfile.is_tarfile(archive_path):
        with tarfile.open(archive_path) as archive:
            for member in archive.getmembers():
                assert_safe_member(extract_root, member.name)
                if member.issym() or member.islnk() or member.isdev():
                    raise BundleError(f"tar archive contains unsupported special member: {member.name}")
                if not (member.isdir() or member.isfile()):
                    raise BundleError(f"tar archive contains unsupported member type: {member.name}")
            archive.extractall(extract_root)
        return

    raise BundleError(f"unsupported archive format: {archive_path}")


def download_archive(url: str, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading source-data archive from {url}")
    try:
        with urllib.request.urlopen(url) as response, destination.open("wb") as handle:
            shutil.copyfileobj(response, handle)
    except Exception as exc:
        raise BundleError(f"failed to download archive: {exc}") from exc
    return destination


def find_bundle_root(extract_root: Path, required_dirs: list[str]) -> Path:
    candidates = [extract_root]
    candidates.extend(path for path in extract_root.iterdir() if path.is_dir())

    for candidate in candidates:
        if all((candidate / directory).is_dir() for directory in required_dirs):
            return candidate

    raise BundleError(
        "archive does not contain the required directories at its root or under one "
        f"enclosing directory: {', '.join(required_dirs)}"
    )


def is_dangerous_destination(dest: Path) -> bool:
    resolved = dest.resolve()
    return resolved in {Path("/").resolve(), Path.cwd().resolve(), Path.home().resolve()}


def prepare_destination(dest: Path, force: bool) -> None:
    if is_dangerous_destination(dest):
        raise BundleError(f"refusing to use dangerous destination: {dest}")

    if dest.exists() and not dest.is_dir():
        raise BundleError(f"destination exists and is not a directory: {dest}")

    if dest.exists() and any(dest.iterdir()):
        if not force:
            raise BundleError(f"destination is not empty: {dest}. Use --force to replace it.")
        shutil.rmtree(dest)

    dest.mkdir(parents=True, exist_ok=True)


def copy_required_dirs(bundle_root: Path, dest: Path, required_dirs: list[str]) -> None:
    for directory in required_dirs:
        source = bundle_root / directory
        target = dest / directory
        shutil.copytree(source, target)


def count_files(path: Path) -> int:
    return sum(1 for item in path.rglob("*") if item.is_file())


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Restore RR SMR ADS source inputs.")
    parser.add_argument("--archive", required=True, help="Local archive path or direct http(s) URL.")
    parser.add_argument("--dest", required=True, help="Destination directory, usually external_data/source_inputs.")
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST), help="Source-data bundle manifest JSON.")
    parser.add_argument("--expected-sha256", default=None, help="Expected archive SHA-256.")
    parser.add_argument("--force", action="store_true", help="Replace destination if it already contains files.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    try:
        manifest = load_manifest(Path(args.manifest).expanduser().resolve())
        required_dirs = required_dirs_from_manifest(manifest)
        dest = Path(args.dest).expanduser().resolve()

        with tempfile.TemporaryDirectory(prefix="rrsmr_source_bundle_") as temp_raw:
            temp_dir = Path(temp_raw)

            if is_url(args.archive):
                archive_path = download_archive(args.archive, temp_dir / "downloaded_archive")
            else:
                archive_path = Path(args.archive).expanduser().resolve()
                if not archive_path.exists():
                    raise BundleError(f"archive not found: {archive_path}")

            actual_sha256 = sha256_file(archive_path)
            expected_sha256 = args.expected_sha256 or str(manifest.get("archive_sha256") or "").strip()

            if expected_sha256:
                if actual_sha256.lower() != expected_sha256.lower():
                    raise BundleError(
                        f"archive checksum mismatch: expected {expected_sha256}, got {actual_sha256}"
                    )
                print(f"Checksum verification: pass ({actual_sha256})")
            else:
                print(f"Checksum verification: skipped (archive SHA-256 is {actual_sha256})")

            extract_root = temp_dir / "extracted"
            extract_archive(archive_path, extract_root)
            bundle_root = find_bundle_root(extract_root, required_dirs)

            prepare_destination(dest, force=args.force)
            copy_required_dirs(bundle_root, dest, required_dirs)

        print(f"Prepared source inputs at {dest}")
        for directory in required_dirs:
            print(f"  {directory}: {count_files(dest / directory)} files")

        return 0

    except BundleError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
