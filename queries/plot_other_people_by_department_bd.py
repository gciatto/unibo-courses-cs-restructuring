#!/usr/bin/env python3

import sys
import json
import textwrap

import yaml
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import cm
import os
import csv

# -- Helpers ------------------------------------------------------------------

REPO_ROOT = ".."
CONTACTS_CSV = os.path.join(REPO_ROOT, "data", "contacts.csv")

# Build a uid -> contact-row lookup so we can resolve each person's role.
contacts = {}
with open(CONTACTS_CSV, encoding="utf-8", newline="") as fh:
    reader = csv.DictReader(fh)
    for row in reader:
        contacts[row["uid"]] = row


def get_role(uid: str) -> str:
    """Look up a person's role in the contacts dict by uid."""
    contact = contacts.get(uid)
    if not contact:
        return "(unknown)"
    return (contact.get("role") or "(unknown)").strip() or "(unknown)"


def load_yaml(path: str) -> list:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def aggregate_people(data: list) -> pd.DataFrame:
    """Aggregate people per department, and split the count by role."""
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
            # Backward compatibility with the former department -> courses ->
            # teachers schema.
            for course in entry.get("courses") or []:
                for person in course.get("teachers") or []:
                    uid = str(person.get("uid") or "").strip()
                    if uid:
                        dept_people.add(uid)

        role_counts = {}
        for uid in sorted(dept_people):
            role = get_role(uid)
            role_counts[role] = role_counts.get(role, 0) + 1

        row = {"department": dept, "people": len(dept_people)}
        row.update(role_counts)
        rows.append(row)

    df = pd.DataFrame(rows).fillna(0)
    role_cols = [c for c in df.columns if c not in ("department", "people")]
    df[role_cols] = df[role_cols].astype(int)

    return df.sort_values("people", ascending=False).reset_index(drop=True)


def make_bar(df: pd.DataFrame, out: str = "_other_people_by_dept_bd.pdf") -> None:
    bar_df = df.sort_values("people", ascending=True).copy()
    bar_df["short"] = bar_df["department"]
    bar_df["label"] = bar_df["department"]

    role_cols = [c for c in df.columns if c not in ("department", "people", "short", "label")]

    fig, ax = plt.subplots(figsize=(11, 10))
    # Build one distinct color per role to avoid repeated legend colors.
    colorway = [cm.tab20(i) for i in range(len(role_cols))] # type: ignore
    left = pd.Series([0] * len(bar_df), index=bar_df.index)

    for i, role in enumerate(role_cols):
        values = bar_df[role]
        bars = ax.barh(
            bar_df["label"],
            values,
            left=left,
            label=role,
            color=colorway[i % len(colorway)],
        )
        labels = [str(int(v)) if float(v) > 0 else "" for v in values]
        ax.bar_label(bars, labels=labels, label_type="center", fontsize=8, color="white")
        left = left + values

    ax.set_title("Total People by Department, Partitioned by Role")
    ax.set_xlabel("Total People")
    ax.set_ylabel("")
    ax.tick_params(axis="y", labelsize=11)

    max_value = float(bar_df["people"].max()) if not bar_df.empty else 0.0
    ax.set_xlim(0, max_value * 1.06 if max_value else 1)
    ax.legend(
        title="Role",
        loc="lower center",
        bbox_to_anchor=(0.5, 1.02),
        ncol=2,
        frameon=True,
    )
    fig.subplots_adjust(left=0.16, right=0.995, top=0.80, bottom=0.06)

    if os.path.exists(out):
        os.remove(out)
    fig.savefig(out, format="pdf", bbox_inches="tight", pad_inches=0.04)
    plt.close(fig)
    print(f"Saved {out}")


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python file.py <yaml_path>",
              file=sys.stderr)
        sys.exit(1)

    data = load_yaml(sys.argv[1])
    df = aggregate_people(data)

    print(f"Loaded {len(df)} departments, total people: {df['people'].sum()}")

    make_bar(df)


if __name__ == "__main__":
    main()
