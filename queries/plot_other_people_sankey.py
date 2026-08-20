"""Draw grouped non-DISI courses as course -> programme -> department flows."""

from __future__ import annotations

import argparse
from pathlib import Path

from plot_grouped_disi_service_courses_sankey import draw_sankey, load_flows


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = SCRIPT_DIR / "grouped_non_disi_courses_by_department.yaml"
DEFAULT_OUTPUT = SCRIPT_DIR / "_other_people_sankey.pdf"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", nargs="?", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("-o", "--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--language",
        choices=("en", "it"),
        default="en",
        help="language used for programme names (default: en)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    flows = load_flows(args.input, args.language)
    if not flows["course_programme"]:
        raise SystemExit("No course-to-programme data found to plot.")
    draw_sankey(flows, args.output)
    print(
        f"Wrote {len(flows['course_order'])} courses, "
        f"{len(flows['programme_order'])} programmes, and "
        f"{len(flows['department_order'])} departments to {args.output}"
    )


if __name__ == "__main__":
    main()
