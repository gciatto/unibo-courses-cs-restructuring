#!/usr/bin/env python3

import sys
import json
import textwrap

import yaml
import pandas as pd
import matplotlib.pyplot as plt
import os

# ── Helpers ──────────────────────────────────────────────────────────────────

def load_yaml(path: str) -> list:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def aggregate_people(data: list) -> pd.DataFrame:
    rows = []
    for entry in data:
        dept = (
            entry.get("dept")
            or entry.get("department")
            or "(no department)"
        ).strip().replace("\n", " ")
        if not dept:
            dept = "(no department)"

        dept_people = set()
        if entry.get("programmes") is not None:
            # Current schema: department -> programmes -> course assignments.
            for programme in entry.get("programmes") or []:
                for course in programme.get("courses") or []:
                    uid = str(course.get("teacher_uid") or "").strip()
                    if uid:
                        dept_people.add(uid)
        else:
            # Backward compatibility with department -> courses -> teachers.
            for course in entry.get("courses") or []:
                for person in course.get("teachers") or []:
                    uid = str(person.get("uid") or "").strip()
                    if uid:
                        dept_people.add(uid)

        rows.append({"department": dept, "people": len(dept_people)})
    return (
        pd.DataFrame(rows)
        .sort_values("people", ascending=False)
        .reset_index(drop=True)
    )

def make_bar(df: pd.DataFrame, out: str = "_other_people_by_dept.pdf") -> None:
    bar_df = df.sort_values("people", ascending=True).copy()
    bar_df["short"] = bar_df["department"]
    bar_df["label"] = bar_df["department"]

    fig, ax = plt.subplots(figsize=(11, 10))
    bars = ax.barh(bar_df["label"], bar_df["people"], color="#4C72B0")

    ax.set_title("Total People by Department")
    ax.set_xlabel("Total People")
    ax.set_ylabel("")
    ax.tick_params(axis="y", labelsize=11)

    max_value = float(bar_df["people"].max()) if not bar_df.empty else 0.0
    x_offset = max(1.0, max_value * 0.01)
    for bar, value in zip(bars, bar_df["people"]):
        ax.text(
            float(value) + x_offset,
            bar.get_y() + bar.get_height() / 2,
            f"{int(value)}",
            va="center",
            ha="left",
            fontsize=10,
        )

    # Leave enough room for value labels without reserving a large empty area.
    ax.set_xlim(0, max_value + x_offset * 2)
    fig.subplots_adjust(left=0.16, right=0.995, top=0.95, bottom=0.06)

    if os.path.exists( out ):
        os.remove( out )
    fig.savefig(out, format="pdf", bbox_inches="tight", pad_inches=0.04)
    plt.close(fig)
    print(f"Saved  {out}")


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python file.py <input.yaml>",
              file=sys.stderr)
        sys.exit(1)

    data = load_yaml(sys.argv[1])
    df   = aggregate_people(data)

    print(f"Loaded {len(df)} departments, total people: {df['people'].sum()}")

    make_bar(df)

if __name__ == "__main__":
    main()
