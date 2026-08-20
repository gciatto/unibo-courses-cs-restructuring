#!/usr/bin/env python3
"""Group non-DISI-taught courses by target department and programme."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from group_disi_service_courses_by_department import (
    DEFAULT_PROGRAMMES_DIR,
    group_courses,
    load_programme_names,
)


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = SCRIPT_DIR / "courses_non_disi.yaml"
DEFAULT_OUTPUT = SCRIPT_DIR / "grouped_non_disi_courses_by_department.yaml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", nargs="?", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("output", nargs="?", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--programmes-dir",
        type=Path,
        default=DEFAULT_PROGRAMMES_DIR,
        help=f"programme metadata directory (default: {DEFAULT_PROGRAMMES_DIR})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    with args.input.open(encoding="utf-8") as fh:
        teachers = yaml.safe_load(fh) or []
    if not isinstance(teachers, list):
        raise ValueError(f"Expected a YAML list in {args.input}")

    programme_names = load_programme_names(args.programmes_dir)
    output = group_courses(teachers, programme_names)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(
            output,
            fh,
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
        )

    programme_groups = sum(len(dept["programmes"]) for dept in output)
    programme_codes = sum(
        len(programme["codes"])
        for dept in output
        for programme in dept["programmes"]
    )
    course_assignments = sum(
        len(programme["courses"])
        for dept in output
        for programme in dept["programmes"]
    )
    print(
        f"Wrote {len(output)} departments, {programme_groups} programme groups "
        f"covering {programme_codes} codes, and {course_assignments} unique "
        f"course assignments to {args.output}"
    )


if __name__ == "__main__":
    main()
