"""Download and verify the AcousticBrainz high-level sample archive."""

from __future__ import annotations

import argparse
import hashlib
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path


BASE_URL = (
    "https://data.metabrainz.org/pub/musicbrainz/acousticbrainz/"
    "dumps/acousticbrainz-sample-json-20220623"
)
CHUNK_SIZE = 1024 * 1024
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "archives"


@dataclass(frozen=True)
class Dataset:
    filename: str
    sha256: str

    @property
    def url(self) -> str:
        return f"{BASE_URL}/{self.filename}"


DATASET = Dataset(
    filename="acousticbrainz-highlevel-sample-json-20220623-0.tar.zst",
    sha256="952bdbc23a0684ebd931c59c648ca847b5a8281030f5b305adf449e563955c9f",
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def format_bytes(size: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}"
        value /= 1024
    raise AssertionError("unreachable")


def print_progress(downloaded: int, total: int | None) -> None:
    if total:
        percent = downloaded / total * 100
        message = (
            f"\rDownloaded {format_bytes(downloaded)} / "
            f"{format_bytes(total)} ({percent:.1f}%)"
        )
    else:
        message = f"\rDownloaded {format_bytes(downloaded)}"
    print(message, end="", file=sys.stderr, flush=True)


def download_dataset(
    dataset: Dataset,
    output_dir: Path,
    *,
    overwrite: bool = False,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / dataset.filename
    temporary = destination.with_suffix(destination.suffix + ".part")

    if destination.exists() and not overwrite:
        print(f"Checking existing file: {destination}")
        if file_sha256(destination) == dataset.sha256:
            print("Already downloaded and checksum is valid.")
            return destination
        raise RuntimeError(
            f"{destination} exists but has an invalid checksum; "
            "use --overwrite to replace it"
        )

    request = urllib.request.Request(
        dataset.url,
        headers={"User-Agent": "music-analytics-pipeline/0.1"},
    )
    digest = hashlib.sha256()
    downloaded = 0

    print(f"Downloading {dataset.url}")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            content_length = response.headers.get("Content-Length")
            total = int(content_length) if content_length else None

            with temporary.open("wb") as target:
                while chunk := response.read(CHUNK_SIZE):
                    target.write(chunk)
                    digest.update(chunk)
                    downloaded += len(chunk)
                    print_progress(downloaded, total)
    except (OSError, urllib.error.URLError):
        temporary.unlink(missing_ok=True)
        raise
    finally:
        if downloaded:
            print(file=sys.stderr)

    actual_sha256 = digest.hexdigest()
    if actual_sha256 != dataset.sha256:
        temporary.unlink(missing_ok=True)
        raise RuntimeError(
            f"checksum mismatch for {dataset.filename}: "
            f"expected {dataset.sha256}, got {actual_sha256}"
        )

    temporary.replace(destination)
    print(f"Saved and verified: {destination}")
    return destination


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download the verified AcousticBrainz high-level sample."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="destination directory (default: <project>/data/raw/archives)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="replace an existing archive if its checksum is invalid",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    try:
        download_dataset(
            DATASET,
            args.output_dir,
            overwrite=args.overwrite,
        )
    except (OSError, RuntimeError, urllib.error.URLError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
