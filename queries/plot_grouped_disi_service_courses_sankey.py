"""Draw a credit-weighted course -> programme -> department Sankey diagram."""

from __future__ import annotations

import argparse
import textwrap
from collections import defaultdict
from pathlib import Path
from typing import Any, Hashable

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib import colormaps
from matplotlib.patches import PathPatch, Rectangle
from matplotlib.path import Path as MplPath
import yaml

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT = SCRIPT_DIR / "grouped_disi_service_courses_by_department.yaml"
DEFAULT_OUTPUT = SCRIPT_DIR / "_grouped_disi_service_courses_sankey.pdf"


def numeric_credits(value: Any, course_id: str) -> float:
    """Parse and validate the credits used as a Sankey flow value."""
    try:
        credits = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid credits for course {course_id}: {value!r}") from exc
    if credits <= 0:
        raise ValueError(f"Credits must be positive for course {course_id}: {value!r}")
    return credits


def load_flows(path: Path, language: str) -> dict[str, Any]:
    """Load YAML and build unique credit-weighted links for the three columns."""
    with path.open(encoding="utf-8") as fh:
        departments = yaml.safe_load(fh) or []
    if not isinstance(departments, list):
        raise ValueError(f"Expected a YAML list in {path}")

    course_labels: dict[tuple[str, str, float], str] = {}
    programme_labels: dict[tuple[str, tuple[str, ...]], str] = {}
    programme_departments: dict[tuple[str, tuple[str, ...]], str] = {}
    course_programme: dict[
        tuple[tuple[str, str, float], tuple[str, tuple[str, ...]]], float
    ] = {}

    department_order = []
    programme_order = []
    for department in departments:
        dept = str(department.get("dept") or "").strip()
        if not dept:
            continue
        department_order.append(dept)

        for programme in department.get("programmes") or []:
            codes = tuple(str(code).strip() for code in programme.get("codes") or [])
            if not codes:
                raise ValueError(f"Programme without codes in department {dept}")
            programme_key = (dept, codes)
            names = programme.get("name") or {}
            programme_name = str(
                names.get(language) or names.get("en") or names.get("it") or ""
            ).strip()
            if not programme_name:
                raise ValueError(f"Programme without a name: {dept}/{', '.join(codes)}")

            programme_order.append(programme_key)
            programme_departments[programme_key] = dept
            programme_labels[programme_key] = (
                f"{programme_name}\n[{', '.join(codes)}]"
            )

            # Co-teachers create repeated YAML records. A course contributes only
            # once to a programme, identified by ID, name, and credit value.
            for course in programme.get("courses") or []:
                course_id = str(course.get("course_id") or "").strip()
                course_name = str(course.get("course_name") or "").strip()
                credits = numeric_credits(course.get("credits"), course_id)
                course_key = (course_id, course_name, credits)
                course_labels[course_key] = f"{course_name} ({credits:g} CFU)"
                course_programme[(course_key, programme_key)] = credits

    programme_department = defaultdict(float)
    for (_course, programme), credits in course_programme.items():
        programme_department[(programme, programme_departments[programme])] += credits

    # Put courses near the average vertical position of their target programmes.
    programme_rank = {key: rank for rank, key in enumerate(programme_order)}
    course_targets = defaultdict(list)
    for course, programme in course_programme:
        course_targets[course].append(programme_rank[programme])
    course_order = sorted(
        course_labels,
        key=lambda key: (
            sum(course_targets[key]) / len(course_targets[key]),
            course_labels[key],
        ),
    )

    return {
        "course_labels": course_labels,
        "programme_labels": programme_labels,
        "course_programme": course_programme,
        "programme_department": dict(programme_department),
        "course_order": course_order,
        "programme_order": programme_order,
        "department_order": department_order,
    }


def column_gaps(keys: list[Hashable], group_of=None) -> list[float]:
    """Return gaps after nodes, with larger gaps between department groups."""
    gaps = []
    for index in range(len(keys) - 1):
        if group_of and group_of(keys[index]) != group_of(keys[index + 1]):
            gaps.append(0.016)
        else:
            gaps.append(0.0045)
    return gaps


def layout_column(
    keys: list[Hashable],
    totals: dict[Hashable, float],
    gaps: list[float],
    scale: float,
) -> dict[Hashable, tuple[float, float]]:
    """Lay out one vertical column, centred in the available height."""
    occupied = sum(totals[key] * scale for key in keys) + sum(gaps)
    cursor = (1.0 + occupied) / 2.0
    positions = {}
    for index, key in enumerate(keys):
        height = totals[key] * scale
        positions[key] = (cursor - height, cursor)
        cursor -= height
        if index < len(gaps):
            cursor -= gaps[index]
    return positions


def add_flow(
    ax,
    source_x: float,
    target_x: float,
    source_interval: tuple[float, float],
    target_interval: tuple[float, float],
    color,
    alpha: float,
) -> None:
    """Add one smooth Sankey ribbon between allocated node intervals."""
    sy0, sy1 = source_interval
    ty0, ty1 = target_interval
    bend = (target_x - source_x) * 0.48
    vertices = [
        (source_x, sy0),
        (source_x + bend, sy0),
        (target_x - bend, ty0),
        (target_x, ty0),
        (target_x, ty1),
        (target_x - bend, ty1),
        (source_x + bend, sy1),
        (source_x, sy1),
        (source_x, sy0),
    ]
    codes = [
        MplPath.MOVETO,
        MplPath.CURVE4,
        MplPath.CURVE4,
        MplPath.CURVE4,
        MplPath.LINETO,
        MplPath.CURVE4,
        MplPath.CURVE4,
        MplPath.CURVE4,
        MplPath.CLOSEPOLY,
    ]
    ax.add_patch(
        PathPatch(MplPath(vertices, codes), facecolor=color, edgecolor="none", alpha=alpha)
    )


def draw_sankey(flows: dict[str, Any], output: Path) -> None:
    """Render the Sankey diagram as a vector PDF or SVG."""
    course_programme = flows["course_programme"]
    programme_department = flows["programme_department"]
    course_order = flows["course_order"]
    programme_order = flows["programme_order"]
    department_order = flows["department_order"]

    course_totals = defaultdict(float)
    programme_totals = defaultdict(float)
    department_totals = defaultdict(float)
    for (course, programme), value in course_programme.items():
        course_totals[course] += value
        programme_totals[programme] += value
    for (programme, department), value in programme_department.items():
        department_totals[department] += value

    course_gaps = column_gaps(course_order)
    programme_gaps = column_gaps(programme_order, group_of=lambda key: key[0])
    department_gaps = [0.016] * max(0, len(department_order) - 1)
    usable_height = 0.94
    scale = min(
        (usable_height - sum(gaps)) / sum(totals.values())
        for gaps, totals in (
            (course_gaps, course_totals),
            (programme_gaps, programme_totals),
            (department_gaps, department_totals),
        )
    )

    course_pos = layout_column(course_order, course_totals, course_gaps, scale)
    programme_pos = layout_column(programme_order, programme_totals, programme_gaps, scale)
    department_pos = layout_column(
        department_order, department_totals, department_gaps, scale
    )

    palette = colormaps["tab20"]
    dept_colors = {
        dept: palette(index % palette.N) for index, dept in enumerate(department_order)
    }
    course_color = "#7b8794"
    course_x = (0.295, 0.305)
    programme_x = (0.635, 0.645)
    department_x = (0.91, 0.925)

    fig, ax = plt.subplots(figsize=(24, 22))
    # Matplotlib's default subplot leaves generous margins around the axes.
    # Use nearly the full canvas while retaining a narrow band for the title.
    fig.subplots_adjust(left=0.015, right=0.975, bottom=0.012, top=0.955)
    # Crop the unused horizontal part of the logical canvas. ``bbox_inches``
    # alone cannot remove it because Matplotlib treats the full axes as content.
    ax.set_xlim(0.18, 0.985)
    ax.set_ylim(0, 1)
    ax.axis("off")

    course_cursor = {key: bounds[0] for key, bounds in course_pos.items()}
    programme_in_cursor = {key: bounds[0] for key, bounds in programme_pos.items()}
    links = sorted(
        course_programme.items(),
        key=lambda item: (
            course_pos[item[0][0]][0],
            programme_pos[item[0][1]][0],
        ),
    )
    for (course, programme), value in links:
        height = value * scale
        source = (course_cursor[course], course_cursor[course] + height)
        target = (
            programme_in_cursor[programme],
            programme_in_cursor[programme] + height,
        )
        add_flow(
            ax,
            course_x[1],
            programme_x[0],
            source,
            target,
            dept_colors[programme[0]],
            0.32,
        )
        course_cursor[course] += height
        programme_in_cursor[programme] += height

    programme_out_cursor = {key: bounds[0] for key, bounds in programme_pos.items()}
    department_cursor = {key: bounds[0] for key, bounds in department_pos.items()}
    for (programme, department), value in sorted(
        programme_department.items(), key=lambda item: programme_pos[item[0][0]][0]
    ):
        height = value * scale
        source = (
            programme_out_cursor[programme],
            programme_out_cursor[programme] + height,
        )
        target = (
            department_cursor[department],
            department_cursor[department] + height,
        )
        add_flow(
            ax,
            programme_x[1],
            department_x[0],
            source,
            target,
            dept_colors[department],
            0.42,
        )
        programme_out_cursor[programme] += height
        department_cursor[department] += height

    for key, (y0, y1) in course_pos.items():
        ax.add_patch(
            Rectangle((course_x[0], y0), course_x[1] - course_x[0], y1 - y0,
                      facecolor=course_color, edgecolor="white", linewidth=0.3)
        )
        ax.text(
            course_x[0] - 0.006,
            (y0 + y1) / 2,
            textwrap.fill(flows["course_labels"][key], 42),
            ha="right",
            va="center",
            fontsize=6.2,
        )

    for key, (y0, y1) in programme_pos.items():
        ax.add_patch(
            Rectangle((programme_x[0], y0), programme_x[1] - programme_x[0], y1 - y0,
                      facecolor=dept_colors[key[0]], edgecolor="white", linewidth=0.3)
        )
        ax.text(
            programme_x[1] + 0.005,
            (y0 + y1) / 2,
            textwrap.fill(flows["programme_labels"][key], 34),
            ha="left",
            va="center",
            fontsize=6.4,
            bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.78, "pad": 0.5},
        )

    for dept, (y0, y1) in department_pos.items():
        ax.add_patch(
            Rectangle((department_x[0], y0), department_x[1] - department_x[0], y1 - y0,
                      facecolor=dept_colors[dept], edgecolor="white", linewidth=0.4)
        )
        ax.text(
            department_x[1] + 0.008,
            (y0 + y1) / 2,
            f"{dept.upper()}\n{department_totals[dept]:g} CFU",
            ha="left",
            va="center",
            fontsize=8,
            fontweight="bold",
        )

    ax.text(0.30, 0.992, "COURSES", ha="center", va="top", fontsize=13, fontweight="bold")
    ax.text(0.64, 0.992, "AGGREGATED PROGRAMMES", ha="center", va="top", fontsize=13, fontweight="bold")
    ax.text(0.917, 0.992, "DEPARTMENTS", ha="center", va="top", fontsize=13, fontweight="bold")
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, bbox_inches="tight", pad_inches=0.04)
    plt.close(fig)


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
