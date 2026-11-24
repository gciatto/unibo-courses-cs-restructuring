"""Sankey from a hand-edited restructuring draw.io diagram back to the old courses.

Each box of the diagram is a new course: its first line is the title, a line
containing "ECTS" its stated credits, and every line equal to a module key of
the attempt's modules-global.yml one of its modules. Other lines (exam type,
delivery mode, notes) are ignored. Modules expand to their source topics, and
the per-course credit shares in topics-global.yml link those topics to the old
courses, whose names come from topics-of-course-<id>.yml. A flow is the
credits an old course spends on the topics of a new course; a module placed
in several boxes counts fully in each. One Sankey (with its table) is drawn
for all old courses and one for each course_scope of the clustering
classification (internal, weak_internal, external, service, borrow); a scope's
Sankey shows only the boxes its old courses flow into.

    .venv/bin/python -m final_sankey.build_sankey \
        data/restructuring/attempt-.../final/restructuring_final.drawio
"""

from __future__ import annotations

import argparse
import base64
import csv
import difflib
import html
import pathlib
import re
import urllib.parse
import xml.etree.ElementTree as ET
import zlib
from collections import Counter
from dataclasses import dataclass

import numpy as np
import yaml
from scipy.cluster.hierarchy import leaves_list, linkage

from network_analysis.topic_graph import _load_pyplot, _ribbon, course_label, load_course_names, load_course_topics

KEY_LIKE = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)+$")
ECTS_LINE = re.compile(r"\bECTS\b", re.IGNORECASE)
NUMBER = re.compile(r"\d+(?:[.,]\d+)?")
# Boxes with this title (ignoring case and brackets) are blank templates, not courses.
TEMPLATE_TITLE = "template"
# course_scope values of clustering.export_cluster_courses.course_scope; one Sankey each.
SCOPES = ("internal", "weak_internal", "external", "service", "borrow")
DEFAULT_CLASSIFICATION = pathlib.Path("data/clusters/runs/2025-20260715-120359-spectral/cluster_courses.csv")


@dataclass
class NewCourse:
    box_id: str
    title: str
    ects: str
    modules: list[str]


def diagram_models(path: pathlib.Path):
    """The mxGraphModel of every page, decompressing pages saved compressed."""
    for diagram in ET.parse(path).getroot().iter("diagram"):
        model = diagram.find("mxGraphModel")
        if model is None and (diagram.text or "").strip():
            # Compressed page: base64 of raw-deflated, URL-encoded XML.
            raw = zlib.decompress(base64.b64decode(diagram.text), -15).decode("utf-8")
            model = ET.fromstring(urllib.parse.unquote(raw))
        if model is not None:
            yield model


def label_lines(value: str) -> list[str]:
    """Non-empty text lines of a draw.io HTML label (breaks come as <br ...>, <div> or <p>)."""
    text = re.sub(r"(?i)<br[^>]*>|</?(?:div|p)[^>]*>", "\n", value)
    text = html.unescape(re.sub(r"<[^>]+>", "", text)).replace("\xa0", " ")
    return [line.strip() for line in text.split("\n") if line.strip()]


def read_new_courses(path: pathlib.Path, module_keys: set[str]) -> tuple[list[NewCourse], list[str]]:
    """Boxes listing at least one module key, in diagram order, plus warnings."""
    courses, warnings = [], []
    for model in diagram_models(path):
        for cell in model.iter("mxCell"):
            if cell.get("vertex") != "1":
                continue
            lines = label_lines(cell.get("value") or "")
            modules = [line for line in lines if line in module_keys]
            if not modules:
                continue
            box = cell.get("id") or "?"
            title = lines[0]
            if title.strip("[] ").lower() == TEMPLATE_TITLE:
                continue
            if title in module_keys:
                warnings.append(f"box {box} has no title (its first line is module {title})")
                title = f"(untitled box {box})"
            ects = next((line for line in lines[1:] if ECTS_LINE.search(line)), "")
            if not ects:
                warnings.append(f"box {box} '{title}' states no ECTS")
            for line in lines[1:]:
                if line not in module_keys and KEY_LIKE.match(line):
                    guess = difflib.get_close_matches(line, module_keys, n=1)
                    hint = f" (did you mean '{guess[0]}'?)" if guess else ""
                    warnings.append(f"box {box} '{title}': '{line}' is not a module key{hint}")
            courses.append(NewCourse(box, title, ects, list(dict.fromkeys(modules))))
    return courses, warnings


def module_warnings(courses: list[NewCourse], module_topics: dict[str, list[str]]) -> list[str]:
    placed = Counter(module for course in courses for module in course.modules)
    warnings = [f"module {module} is in no box" for module in sorted(set(module_topics) - set(placed))]
    for module, count in sorted(placed.items()):
        if count > 1:
            boxes = [course.title for course in courses if module in course.modules]
            warnings.append(f"module {module} is in {count} boxes: {'; '.join(boxes)}")
    return warnings


def ects_range(text: str) -> tuple[float, float] | None:
    """Stated ECTS as (low, high): "3 ECTS" -> (3, 3), "3/4 ECTS" -> (3, 4); None without a number."""
    numbers = [float(number.replace(",", ".")) for number in NUMBER.findall(text)]
    return (min(numbers), max(numbers)) if numbers else None


def ects_totals(
    courses: list[NewCourse], left: list[str], course_topics: dict[str, dict[str, float]]
) -> tuple[str, str]:
    """Total-ECTS labels for the old courses and the new courses (boxes)."""
    # Per-topic shares are rounded in the YAML, so each course's sum is rounded back first.
    old_total = sum(round(sum(course_topics[old].values()), 1) for old in left)
    ranges = [ects_range(course.ects) for course in courses]
    low = sum(r[0] for r in ranges if r)
    high = sum(r[1] for r in ranges if r)
    new_total = f"{low:g}" if low == high else f"{low:g}-{high:g}"
    notes = []
    ranged = [course.ects for course, r in zip(courses, ranges) if r and r[0] != r[1]]
    if ranged:
        notes.append(f"{', '.join(ranged)} counted as a range")
    unstated = sum(1 for r in ranges if r is None)
    if unstated:
        notes.append(f"{unstated} box{'es' if unstated > 1 else ''} without ECTS not counted")
    suffix = f"\n({'; '.join(notes)})" if notes else ""
    return (
        f"Total: {old_total:g} ECTS in {len(left)} old courses",
        f"Total: {new_total} ECTS in {len(courses)} new courses{suffix}",
    )


def load_course_scopes(path: pathlib.Path) -> dict[str, str]:
    """Course ID -> ``course_scope`` from a clustering cluster_courses.csv (";"-separated).

    IDs are matched exactly: "07276" does not match "07276-A".
    """
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return {row["course_id"].strip(): row["course_scope"].strip() for row in csv.DictReader(handle, delimiter=";")}


def course_flows(
    courses: list[NewCourse],
    module_topics: dict[str, list[str]],
    course_topics: dict[str, dict[str, float]],
) -> dict[tuple[str, int], tuple[int, float]]:
    """(old course, new course index) -> (shared topics, credits the old course spends on them)."""
    flows = {}
    for index, course in enumerate(courses):
        topics = {topic for module in course.modules for topic in module_topics[module]}
        for old, shares in course_topics.items():
            shared = topics.intersection(shares)
            if shared:
                flows[(old, index)] = (len(shared), sum(shares[topic] for topic in shared))
    return flows


def order_old_courses(
    olds: list[str], course_topics: dict[str, dict[str, float]], module_topics: dict[str, list[str]]
) -> list[str]:
    """Old courses using the same modules next to each other.

    Each old course is the vector of credits it spends per module; average-linkage
    clustering on cosine distance with optimal leaf ordering gives the order.
    """
    if len(olds) < 3:
        return sorted(olds)
    topic_module = {topic: module for module, topics in module_topics.items() for topic in topics}
    column = {module: index for index, module in enumerate(sorted(module_topics))}
    vectors = np.zeros((len(olds), len(column)))
    for row, old in enumerate(olds):
        for topic, share in course_topics[old].items():
            if topic in topic_module:
                vectors[row, column[topic_module[topic]]] += share
    tree = linkage(vectors, method="average", metric="cosine", optimal_ordering=True)
    return [olds[index] for index in leaves_list(tree)]


def plot_sankey(
    courses: list[NewCourse],
    flows: dict[tuple[str, int], tuple[int, float]],
    left: list[str],
    names: dict[str, str],
    course_topics: dict[str, dict[str, float]],
    totals: tuple[str, str],
    title: str,
    path: pathlib.Path,
) -> list[int]:
    """Old courses (left) to new courses (right); returns the new courses' top-to-bottom order.

    Each new course sits at the credit-weighted mean position of the old courses
    feeding it, which keeps ribbons short and groups new courses sharing sources.
    """
    plt = _load_pyplot()
    from matplotlib.patches import PathPatch, Rectangle

    totals_left = Counter()
    totals_right = Counter()
    for (old, index), (_, credits) in flows.items():
        totals_left[old] += credits
        totals_right[index] += credits

    # Minimum gaps in credits (0.05 inches each): 0.6 between old courses, which are
    # at least 1 credit tall, and 2 between new courses, which can be much thinner,
    # so every label gets about 0.1 inches. The side needing more room sets the height.
    right_count = len(totals_right)
    height = max(
        sum(totals_left.values()) + 0.6 * (len(left) - 1),
        sum(totals_right.values()) + 2.0 * (right_count - 1),
    )
    gap_left = (height - sum(totals_left.values())) / max(1, len(left) - 1)

    def stack(order, totals, gap):
        intervals, top = {}, height
        for key in order:
            intervals[key] = (top - totals[key], top)
            top -= totals[key] + gap
        return intervals

    left_box = stack(left, totals_left, gap_left)
    centre = {old: sum(left_box[old]) / 2 for old in left}
    weight_sum = Counter()
    for (old, index), (_, credits) in flows.items():
        weight_sum[index] += credits * centre[old]
    right = sorted(totals_right, key=lambda index: (-weight_sum[index] / totals_right[index], index))
    gap_right = (height - sum(totals_right.values())) / max(1, len(right) - 1)
    right_box = stack(right, totals_right, gap_right)

    palette = [plt.get_cmap(name)(index) for name in ("tab20", "tab20b", "tab20c") for index in range(20)]
    colour = {index: palette[position % len(palette)] for position, index in enumerate(right)}

    # About 0.05 inches per credit keeps a 1-credit course label legible; PDF pages max out at 200 inches.
    fig, ax = plt.subplots(figsize=(24, min(199, max(10, height * 0.05 + 2))))
    ax.set_xlim(0, 1)
    # Room above the columns for the totals (three lines, about 0.8 inches at 0.05 inches per unit).
    ax.set_ylim(-1, height + 18)
    ax.axis("off")
    x_left, x_right, bar = 0.3, 0.6, 0.008
    ax.text(x_left + bar, height + 2, f"OLD COURSES\n{totals[0]}",
            ha="right", va="bottom", fontsize=11, fontweight="bold")
    ax.text(x_right, height + 2, f"NEW COURSES\n{totals[1]}",
            ha="left", va="bottom", fontsize=11, fontweight="bold")

    left_cursor = {old: left_box[old][1] for old in left}
    right_cursor = {index: right_box[index][1] for index in right}
    for index in right:
        for old in left:
            if (old, index) not in flows:
                continue
            credits = flows[(old, index)][1]
            band_left = (left_cursor[old] - credits, left_cursor[old])
            band_right = (right_cursor[index] - credits, right_cursor[index])
            left_cursor[old] -= credits
            right_cursor[index] -= credits
            ax.add_patch(PathPatch(
                _ribbon(x_left + bar, band_left, x_right, band_right),
                facecolor=colour[index], edgecolor="none", alpha=0.55,
            ))

    for old in left:
        y0, y1 = left_box[old]
        # Per-topic shares are rounded in the YAML, so their sum is only close to the real credits.
        credits = round(sum(course_topics[old].values()), 1)
        ax.add_patch(Rectangle((x_left, y0), bar, y1 - y0, facecolor="dimgrey", edgecolor="none"))
        ax.text(x_left - 0.004, (y0 + y1) / 2, f"{course_label(old, names)} ({credits:g} ECTS)",
                ha="right", va="center", fontsize=5)
    for index in right:
        y0, y1 = right_box[index]
        course = courses[index]
        ax.add_patch(Rectangle((x_right, y0), bar, y1 - y0, facecolor=colour[index], edgecolor="none"))
        ax.text(x_right + bar + 0.004, (y0 + y1) / 2, f"{course.title} ({course.ects or 'ECTS ?'})",
                ha="left", va="center", fontsize=6)

    ax.set_title(
        f"{title}: old courses (left) to new courses (right)\n"
        "flow width = credits of the old course spent on the topics of the new course's modules",
        fontsize=12,
    )
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return right


def write_table(
    courses: list[NewCourse],
    flows: dict[tuple[str, int], tuple[int, float]],
    order: list[int],
    names: dict[str, str],
    csv_path: pathlib.Path,
    md_path: pathlib.Path,
) -> None:
    """One row per new course in Sankey order; old courses as "<id> <name> (<shared topics>)"."""

    def old_courses(index: int) -> str:
        found = sorted(
            ((old, value) for (old, i), value in flows.items() if i == index),
            key=lambda item: (-item[1][1], -item[1][0], item[0]),
        )
        return "; ".join(f"{course_label(old, names)} ({count})" for old, (count, _) in found)

    rows = [(courses[i].title, courses[i].ects, courses[i].modules, old_courses(i)) for i in order]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["new_course", "ects", "modules", "old_courses"])
        for title, ects, modules, olds in rows:
            writer.writerow([title, ects, "; ".join(modules), olds])
    lines = ["| New course | ECTS | Modules | Old courses (shared topics) |", "|---|---|---|---|"]
    lines += [f"| {title} | {ects} | {', '.join(modules)} | {olds} |" for title, ects, modules, olds in rows]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("drawio", type=pathlib.Path, help="Hand-edited restructuring diagram (.drawio)")
    parser.add_argument(
        "--attempt-dir",
        type=pathlib.Path,
        help="Attempt folder with modules-global.yml, topics-global.yml and topics-of-course-*.yml "
        "(default: the folder above the diagram's folder)",
    )
    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        help="Where to write the Sankey PDF and the table (default: the diagram's folder)",
    )
    parser.add_argument(
        "--classification",
        type=pathlib.Path,
        default=DEFAULT_CLASSIFICATION,
        help="Clustering cluster_courses.csv whose course_scope column splits the old courses; one "
        f"Sankey is drawn for all courses and one per scope (default: {DEFAULT_CLASSIFICATION})",
    )
    args = parser.parse_args()
    if not args.drawio.is_file():
        parser.error(f"Diagram not found: {args.drawio}")
    if not args.classification.is_file():
        parser.error(f"Classification not found: {args.classification}")
    attempt = args.attempt_dir or args.drawio.parent.parent
    output_dir = args.output_dir or args.drawio.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    modules = yaml.safe_load((attempt / "modules-global.yml").read_text(encoding="utf-8"))["modules"]
    module_topics = {str(module["key"]): [str(topic) for topic in module["source_topics"]] for module in modules}
    _, course_topics = load_course_topics(attempt / "topics-global.yml")
    names = load_course_names(attempt)

    courses, warnings = read_new_courses(args.drawio, set(module_topics))
    warnings += module_warnings(courses, module_topics)
    scopes = load_course_scopes(args.classification)
    unclassified = sorted(set(course_topics) - set(scopes))
    if unclassified:
        warnings.append(f"old courses without a course_scope (only in the 'all' Sankey): {', '.join(unclassified)}")
    unknown = sorted(set(scopes) - set(course_topics))
    if unknown:
        warnings.append(f"classified courses not in the attempt: {', '.join(unknown)}")
    unexpected = sorted(set(scopes.values()) - set(SCOPES))
    if unexpected:
        warnings.append(f"unknown course_scope values ignored: {', '.join(unexpected)}")

    for scope in ("all", *SCOPES):
        selected = {
            old: shares for old, shares in course_topics.items() if scope == "all" or scopes.get(old) == scope
        }
        stem = f"{args.drawio.stem}-{scope}"
        flows = course_flows(courses, module_topics, selected)
        left = order_old_courses(sorted({old for old, _ in flows}), selected, module_topics)
        unreached = sorted(set(selected) - set(left))
        if unreached and scope == "all":
            warnings.append(f"old courses with no topic in any box: {', '.join(unreached)}")
        # Only boxes some drawn old course flows into are drawn and totalled.
        connected = sorted({index for _, index in flows})
        totals = ects_totals([courses[index] for index in connected], left, selected)

        sankey_pdf = output_dir / f"{stem}-sankey.pdf"
        table_csv = output_dir / f"{stem}-courses.csv"
        table_md = output_dir / f"{stem}-courses.md"
        order = plot_sankey(courses, flows, left, names, selected, totals, stem, sankey_pdf)
        write_table(courses, flows, order, names, table_csv, table_md)

        print(f"[{scope}] new courses (boxes): {len(connected)} of {len(courses)}; "
              f"old courses: {len(left)}; flows: {len(flows)}")
        print(f"  Old courses: {totals[0]}\n  New courses: {totals[1]}")
        print(f"  Wrote {sankey_pdf.name}, {table_csv.name}, {table_md.name}")
    if warnings:
        print(f"\nWarnings ({len(warnings)}):")
        for warning in warnings:
            print(f"  - {warning}")


if __name__ == "__main__":
    main()
