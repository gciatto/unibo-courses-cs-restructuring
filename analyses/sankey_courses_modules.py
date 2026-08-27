#!/usr/bin/env python3
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.path import Path as MplPath
from matplotlib.patches import PathPatch, Rectangle
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from reportlab.lib.pagesizes import landscape, A3


def ribbon_path(x0, y0a, y0b, x1, y1a, y1b):
    """Closed cubic-Bezier ribbon from vertical interval at x0 to interval at x1."""
    dx = x1 - x0
    verts = [
        (x0, y0a),
        (x0 + 0.42 * dx, y0a), (x1 - 0.42 * dx, y1a), (x1, y1a),
        (x1, y1b),
        (x1 - 0.42 * dx, y1b), (x0 + 0.42 * dx, y0b), (x0, y0b),
        (x0, y0a),
    ]
    codes = [
        MplPath.MOVETO,
        MplPath.CURVE4, MplPath.CURVE4, MplPath.CURVE4,
        MplPath.LINETO,
        MplPath.CURVE4, MplPath.CURVE4, MplPath.CURVE4,
        MplPath.CLOSEPOLY,
    ]
    return MplPath(verts, codes)


def read_course_module_csv(csv_filename):
    """Read a CSV whose first column contains course names and remaining columns are modules.

    Empty cells become NaN (no connection). A literal 0 remains 0 (visible thin connection).
    """
    df = pd.read_csv(csv_filename, index_col=0)

    if df.empty:
        raise ValueError("The CSV contains no course/module data.")

    # Clean accidental whitespace in labels while preserving course/module wording.
    df.index = df.index.astype(str).str.strip()
    df.columns = df.columns.astype(str).str.strip()

    # Force all cells to numeric. Empty cells remain NaN.
    df = df.apply(pd.to_numeric, errors="coerce")

    if df.index.has_duplicates:
        raise ValueError("Course names in the first column must be unique.")
    if df.columns.has_duplicates:
        raise ValueError("Module names in the header row must be unique.")

    return df


def sankey_courses_modules(df, pdf_filename, zero_width=0.16, figsize=None):
    courses = list(df.index)
    modules = list(df.columns)

    # Each module is assumed to have one CFU value across all courses to which it connects.
    # This matches the intended representation: every flow for a given module has the same width.
    module_value = {}
    for module in modules:
        vals = sorted({float(v) for v in df[module].dropna().tolist()})
        positive = sorted({v for v in vals if v > 0})

        if len(positive) > 1:
            raise ValueError(
                f"Module '{module}' contains different positive CFU values {positive}. "
                "This Sankey expects each module to have one constant CFU width."
            )

        module_value[module] = positive[0] if positive else 0.0

    def visual_value(v):
        """Width used for drawing. A semantic 0 gets a tiny visible width."""
        return zero_width if float(v) == 0 else float(v)

    # Course geometry is additive: stack every connected module width.
    course_visual_total = {
        course: sum(
            visual_value(df.loc[course, module])
            for module in modules
            if not pd.isna(df.loc[course, module])
        )
        for course in courses
    }

    # Fit the complete Sankey inside a fixed vertical plotting region.
    # The old version used a fixed scale/gap, so sufficiently many courses
    # pushed the bottom ribbons below y=0 and Matplotlib clipped them.
    y_top = 0.925
    y_bottom = 0.095
    available_height = y_top - y_bottom

    # Keep some vertical breathing room, but shrink gaps automatically as
    # the number of nodes grows.
    gap_fraction = 0.34
    gap_course = (available_height * gap_fraction / max(1, len(courses) - 1)) if len(courses) > 1 else 0.0
    gap_module = (available_height * gap_fraction / max(1, len(modules) - 1)) if len(modules) > 1 else 0.0
    gap_course = min(0.030, gap_course)
    gap_module = min(0.045, gap_module)

    course_units = sum(course_visual_total.values())
    module_units = sum(visual_value(module_value[m]) for m in modules)

    course_room = available_height - gap_course * max(0, len(courses) - 1)
    module_room = available_height - gap_module * max(0, len(modules) - 1)

    # One common scale must be used on both sides so that a module ribbon
    # has the same thickness at the course and module ends.
    scale_candidates = [0.0062]
    if course_units > 0:
        scale_candidates.append(course_room / course_units)
    if module_units > 0:
        scale_candidates.append(module_room / module_units)
    scale = min(scale_candidates)

    x_left, x_right = 0.23, 0.77
    node_width = 0.014

    left_heights = [course_visual_total[c] * scale for c in courses]
    right_heights = [visual_value(module_value[m]) * scale for m in modules]

    def positions(heights, gap):
        total = sum(heights) + gap * max(0, len(heights) - 1)
        # Center the stack within the allowed region.
        top = y_bottom + (available_height + total) / 2
        out = []
        y = top
        for h in heights:
            out.append((y - h, y))
            y -= h + gap
        return out

    left_pos = positions(left_heights, gap_course)
    right_pos = positions(right_heights, gap_module)

    cmap = plt.get_cmap("tab10")
    colors = {module: cmap(i % 10) for i, module in enumerate(modules)}

    if figsize is None:
        # A little extra height for larger course lists keeps text readable.
        figsize = (13, max(7.5, 5.0 + 0.20 * len(courses)))
    fig, ax = plt.subplots(figsize=figsize)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    # Course boxes: visual height is additive, but labels are semantic CFU from the table.
    for i, course in enumerate(courses):
        y0, y1 = left_pos[i]
        ax.add_patch(
            Rectangle(
                (x_left - node_width, y0), node_width, y1 - y0,
                facecolor="white", edgecolor="black", linewidth=0.9, zorder=4,
            )
        )
        semantic_course_cfu = df.loc[course].sum(skipna=True)
        ax.text(
            x_left - node_width / 2, (y0 + y1) / 2, f"{semantic_course_cfu:g}",
            ha="center", va="center", fontsize=7.5, fontweight="bold", zorder=5,
        )
        ax.text(
            x_left - node_width - 0.012, (y0 + y1) / 2, course,
            ha="right", va="center", fontsize=9,
        )

    # Module boxes: box height equals the width of every flow belonging to that module.
    for j, module in enumerate(modules):
        y0, y1 = right_pos[j]
        ax.add_patch(
            Rectangle(
                (x_right, y0), node_width, y1 - y0,
                facecolor=colors[module], edgecolor="black", linewidth=0.7, zorder=4,
            )
        )
        ax.text(
            x_right + node_width / 2, (y0 + y1) / 2, f"{module_value[module]:g}",
            ha="center", va="center", fontsize=7.5, fontweight="bold", zorder=5,
        )
        ax.text(
            x_right + node_width + 0.012, (y0 + y1) / 2, module,
            ha="left", va="center", fontsize=9,
        )

    # Stack ribbons at each course. At each module, all ribbons share the module interval.
    course_cursor = {course: left_pos[i][0] for i, course in enumerate(courses)}

    for j, module in enumerate(modules):
        module_y0, module_y1 = right_pos[j]
        for course in courses:
            value = df.loc[course, module]
            if pd.isna(value):
                continue

            h = visual_value(value) * scale
            course_y0 = course_cursor[course]
            course_y1 = course_y0 + h
            course_cursor[course] = course_y1

            path = ribbon_path(
                x_left, course_y0, course_y1,
                x_right, module_y0, module_y1,
            )
            ax.add_patch(
                PathPatch(
                    path,
                    facecolor=colors[module], edgecolor=colors[module],
                    linewidth=0.25, alpha=0.62, zorder=2,
                )
            )

    ax.text(
        x_left - node_width, 0.985, "COURSES",
        ha="right", va="top", fontsize=12, fontweight="bold",
    )
    ax.text(
        x_right, 0.985, "MODULES",
        ha="left", va="top", fontsize=12, fontweight="bold",
    )

    # Semantic totals are computed directly from the CSV values, never from visual widths.
    total_course_cfu = float(np.nansum(df.to_numpy(dtype=float)))
    total_module_cfu = sum(float(module_value[m]) for m in modules)

    def fmt_cfu(x):
        return str(int(x)) if float(x).is_integer() else f"{x:g}"

    left_bottom = min(y0 for y0, _ in left_pos)
    right_bottom = min(y0 for y0, _ in right_pos)
    ax.text(
        x_left - node_width, left_bottom - 0.018,
        f"Total CFU {fmt_cfu(total_course_cfu)}",
        ha="right", va="top", fontsize=11, fontweight="bold",
    )
    ax.text(
        x_right, right_bottom - 0.018,
        f"Total CFU {fmt_cfu(total_module_cfu)}",
        ha="left", va="top", fontsize=11, fontweight="bold",
    )

    #ax.text(
    #    0.5, 0.018,
    #    "Module color is carried by its flows. Module/flow width is proportional to CFU; "
    #    "course width is the additive sum of connected module widths. "
    #    "0 CFU is a thin ribbon; missing = no ribbon.",
    #    ha="center", va="bottom", fontsize=8,
    #)

    # Render to a temporary PNG, then place it on a single landscape A3 PDF page.
    pdf_path = Path(pdf_filename)
    png_path = pdf_path.with_name(pdf_path.stem + "_tmp.png")
    fig.savefig(png_path, dpi=240, bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)

    page_w, page_h = landscape(A3)
    pdf = canvas.Canvas(str(pdf_path), pagesize=(page_w, page_h))
    img = ImageReader(str(png_path))
    img_w, img_h = img.getSize()
    margin = 18
    ratio = min((page_w - 2 * margin) / img_w, (page_h - 2 * margin) / img_h)
    draw_w, draw_h = img_w * ratio, img_h * ratio
    pdf.drawImage(
        img,
        (page_w - draw_w) / 2,
        (page_h - draw_h) / 2,
        width=draw_w,
        height=draw_h,
        mask="auto",
    )
    pdf.showPage()
    pdf.save()

    png_path.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Create a course-to-module Sankey PDF from a CSV file. "
            "The first column must contain course names; remaining columns are modules."
        )
    )
    parser.add_argument("csv_file", help="Input CSV file")
    args = parser.parse_args()

    csv_path = Path(args.csv_file).expanduser().resolve()
    if not csv_path.is_file():
        parser.error(f"Input file not found: {csv_path}")
    if csv_path.suffix.lower() != ".csv":
        parser.error("Input file must have a .csv extension")

    df = read_course_module_csv(csv_path)
    pdf_path = csv_path.with_suffix(".pdf")
    sankey_courses_modules(df, pdf_path)
    print(f"Saved Sankey PDF to: {pdf_path}")


if __name__ == "__main__":
    main()
