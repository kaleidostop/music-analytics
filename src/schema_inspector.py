"""Inspect field paths and JSON types in AcousticBrainz documents."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable


def json_type(value: Any) -> str:
    """Return a JSON-oriented type name (bool must be checked before number)."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def collect_paths(value: Any, prefix: str = "") -> dict[str, set[str]]:
    """Collect the types observed at every dotted field path in one document."""
    paths: dict[str, set[str]] = defaultdict(set)

    if prefix:
        paths[prefix].add(json_type(value))

    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{prefix}.{key}" if prefix else str(key)
            child_paths = collect_paths(child, child_path)
            for path, types in child_paths.items():
                paths[path].update(types)
    elif isinstance(value, list) and value:
        item_path = f"{prefix}[]"
        for child in value:
            child_paths = collect_paths(child, item_path)
            for path, types in child_paths.items():
                paths[path].update(types)

    return paths


def discover_json_files(inputs: Iterable[Path]) -> list[Path]:
    """Resolve files and directories to a stable, deduplicated JSON file list."""
    files: set[Path] = set()
    for input_path in inputs:
        if input_path.is_file() and input_path.suffix.lower() == ".json":
            files.add(input_path.resolve())
        elif input_path.is_dir():
            files.update(path.resolve() for path in input_path.rglob("*.json"))
    return sorted(files, key=str)


def inspect_files(
    files: Iterable[Path],
) -> tuple[int, dict[str, Counter[str]], Counter[str], list[str]]:
    """Inspect documents and return counts plus readable parsing errors."""
    type_counts: dict[str, Counter[str]] = defaultdict(Counter)
    presence_counts: Counter[str] = Counter()
    errors: list[str] = []
    documents = 0

    for path in files:
        try:
            with path.open("r", encoding="utf-8") as source:
                document = json.load(source)
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            errors.append(f"{path}: {error}")
            continue

        if not isinstance(document, dict):
            errors.append(f"{path}: root value must be an object")
            continue

        documents += 1
        document_paths = collect_paths(document)
        for field_path, observed_types in document_paths.items():
            presence_counts[field_path] += 1
            for observed_type in observed_types:
                type_counts[field_path][observed_type] += 1

    return documents, type_counts, presence_counts, errors


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect dotted field paths, types and presence in JSON files."
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        type=Path,
        help="JSON files or directories searched recursively",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="maximum number of files to inspect (default: 100, 0 means all)",
    )
    parser.add_argument(
        "--required",
        action="append",
        default=[],
        metavar="FIELD.PATH",
        help="field that must occur in every valid document; repeatable",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.limit < 0:
        print("error: --limit cannot be negative", file=sys.stderr)
        return 2

    missing_inputs = [str(path) for path in args.inputs if not path.exists()]
    if missing_inputs:
        print(f"error: input does not exist: {', '.join(missing_inputs)}", file=sys.stderr)
        return 2

    files = discover_json_files(args.inputs)
    if args.limit:
        files = files[: args.limit]
    if not files:
        print("error: no JSON files found", file=sys.stderr)
        return 2

    documents, type_counts, presence_counts, errors = inspect_files(files)
    print(f"Files selected: {len(files)}")
    print(f"Valid documents: {documents}")
    print(f"Invalid documents: {len(errors)}")
    print()
    print(f"{'FIELD':<64} {'TYPES':<24} {'PRESENT'}")
    print("-" * 104)

    for field_path in sorted(type_counts):
        types = ", ".join(sorted(type_counts[field_path]))
        present = presence_counts[field_path]
        percentage = (present / documents * 100) if documents else 0
        print(f"{field_path:<64} {types:<24} {present}/{documents} ({percentage:.1f}%)")

    if errors:
        print("\nInvalid documents:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)

    missing_required = [
        field_path
        for field_path in args.required
        if presence_counts[field_path] != documents
    ]
    if missing_required:
        print("\nRequired fields missing from one or more documents:", file=sys.stderr)
        for field_path in missing_required:
            present = presence_counts[field_path]
            print(f"- {field_path}: {present}/{documents}", file=sys.stderr)
        return 1

    return 1 if documents == 0 or errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
