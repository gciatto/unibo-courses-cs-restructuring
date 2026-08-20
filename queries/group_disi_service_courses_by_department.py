"""Reorganize DISI service courses by department and teaching programme."""

from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_PROGRAMMES_DIR = SCRIPT_DIR.parent / "data" / "programmes" / "2025"
DEFAULT_INPUT = SCRIPT_DIR / "disi_service_courses.yaml"
DEFAULT_OUTPUT = SCRIPT_DIR / "grouped_disi_service_courses_by_department.yaml"


def load_programme_names(
    programmes_dir: Path,
) -> dict[tuple[str, str], dict[str, str]]:
    """Index programme names by ``(department, code)``."""
    names = {}

    for path in sorted(programmes_dir.glob("*/programme-*.yml")):
        with path.open(encoding="utf-8") as fh:
            programme = yaml.safe_load(fh) or {}

        dept = str(programme.get("department") or "").strip()
        code = str(programme.get("code") or "").strip()
        name = programme.get("name") or {}
        if not dept or not code or not isinstance(name, dict):
            continue

        key = (dept, code)
        normalized_name = {
            str(language): str(title).strip()
            for language, title in name.items()
            if str(title).strip()
        }
        if key in names and names[key] != normalized_name:
            raise ValueError(f"Conflicting programme metadata for {dept}/{code}")
        names[key] = normalized_name

    return names


def group_courses(
    teachers: list[dict[str, Any]],
    programme_names: dict[tuple[str, str], dict[str, str]],
) -> list[dict[str, Any]]:
    """Group courses by department and merge programme editions by name."""
    grouped: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )

    for teacher in teachers:
        teacher_name = str(teacher.get("name") or "").strip()
        teacher_uid = str(teacher.get("uid") or "").strip()

        for course in teacher.get("courses") or []:
            course_entry = {
                "teacher_name": teacher_name,
                "teacher_uid": teacher_uid,
                "course_id": str(course.get("id") or "").strip(),
                "course_name": str(course.get("name") or "").strip(),
                "credits": course.get("credits"),
            }

            # A course is copied into every programme to which it belongs.
            for programme in course.get("programmes") or []:
                dept = str(programme.get("dept") or "").strip()
                code = str(programme.get("code") or "").strip()
                if not dept or not code:
                    continue
                grouped[dept][code].append(course_entry.copy())

    result = []
    for dept in sorted(grouped):
        merged_programmes: dict[
            tuple[str, str], dict[str, Any]
        ] = {}

        for code in sorted(grouped[dept]):
            key = (dept, code)
            if key not in programme_names:
                raise KeyError(f"Programme metadata not found for {dept}/{code}")

            name = programme_names[key]
            name_key = (name.get("it", ""), name.get("en", ""))
            if not all(name_key):
                raise ValueError(
                    f"Italian and English names are required for {dept}/{code}"
                )

            aggregate = merged_programmes.setdefault(
                name_key,
                {"codes": [], "name": name, "courses": []},
            )
            aggregate["codes"].append(code)
            aggregate["courses"].extend(grouped[dept][code])

        programmes = []
        for aggregate in merged_programmes.values():
            # The same assignment can occur in several editions. Keep it once.
            unique_courses = {
                (
                    course["teacher_name"],
                    course["teacher_uid"],
                    course["course_id"],
                    course["course_name"],
                    course["credits"],
                ): course
                for course in aggregate["courses"]
            }
            courses = sorted(
                unique_courses.values(),
                key=lambda item: (
                    item["course_name"],
                    item["course_id"],
                    item["teacher_name"],
                    item["teacher_uid"],
                ),
            )
            programmes.append(
                {
                    "codes": aggregate["codes"],
                    "merged": len(aggregate["codes"]) > 1,
                    "name": aggregate["name"],
                    "courses": courses,
                }
            )

        programmes.sort(key=lambda programme: programme["codes"])
        result.append({"dept": dept, "programmes": programmes})

    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Group disi_service_courses.yaml by department and teaching programme."
        )
    )
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"source YAML file (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"destination YAML file (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--programmes-dir",
        type=Path,
        default=DEFAULT_PROGRAMMES_DIR,
        help=(
            "directory containing programme metadata grouped by department "
            f"(default: {DEFAULT_PROGRAMMES_DIR})"
        ),
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

    programme_count = sum(len(item["programmes"]) for item in output)
    programme_code_count = sum(
        len(programme["codes"])
        for item in output
        for programme in item["programmes"]
    )
    merged_count = sum(
        programme["merged"]
        for item in output
        for programme in item["programmes"]
    )
    course_count = sum(
        len(programme["courses"])
        for item in output
        for programme in item["programmes"]
    )
    print(
        f"Wrote {len(output)} departments and {programme_count} programme groups "
        f"({programme_code_count} codes, {merged_count} merged groups), containing "
        f"{course_count} unique course assignments, to {args.output}"
    )


if __name__ == "__main__":
    main()
