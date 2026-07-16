import os
import csv
from collections import defaultdict
from typing import Dict

import yaml
import pandas as pd
import matplotlib.pyplot as plt

REPO_ROOT = ".."
CONTACTS_CSV = os.path.join(REPO_ROOT, "data", "contacts.csv")
INPUT_YAML = "disi_service_courses.yaml"
OUTPUT_FILE = "disi_service_courses_credits_per_department_db.pdf"

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


def dept_label_from_course(course: dict) -> str:
    """
    Build one department label for a course from all distinct programme departments.
    If more than one department appears, join them in alphabetical order.
    """
    programmes = course.get("programmes") or []
    depts = {
        str(p.get("dept", "")).strip()
        for p in programmes
        if str(p.get("dept", "")).strip()
    }
    if not depts:
        return "(unknown)"
    return " + ".join(sorted(depts))


def parse_credits(value) -> float:
    """Convert credits to a numeric value when possible."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def main() -> None:
    with open(INPUT_YAML, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or []

    # dept_label -> role -> total credits
    credits_by_dept_role: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for person in data:
        uid = str(person.get("uid", "")).strip()
        if not uid:
            continue

        role = get_role(uid)
        courses = person.get("courses") or []

        for course in courses:
            dept_label = dept_label_from_course(course)
            credits = parse_credits(course.get("credits"))
            credits_by_dept_role[dept_label][role] += credits

    rows = []
    for dept_label, role_map in credits_by_dept_role.items():
        for role, total_credits in role_map.items():
            rows.append(
                {
                    "department": dept_label,
                    "role": role,
                    "credits": total_credits,
                }
            )

    if not rows:
        raise SystemExit("No data found to plot.")

    df = pd.DataFrame(rows)

    pivot = (
        df.pivot_table(
            index="department",
            columns="role",
            values="credits",
            aggfunc="sum",
            fill_value=0,
        )
        .sort_index()
    )

    # Sort bars by total credits descending for readability.
    pivot["__total__"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("__total__", ascending=False)
    pivot = pivot.drop(columns="__total__")

    ax = pivot.plot(
        kind="bar",
        stacked=True,
        figsize=(14, max(6, 0.45 * len(pivot))),
        colormap="tab20",
        edgecolor="black",
        linewidth=0.3,
    )

    ax.set_title("Total course credits by target department, broken down by role")
    ax.set_xlabel("Department (or combined departments from course programmes)")
    ax.set_ylabel("Total credits")
    ax.legend(title="Role", bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(OUTPUT_FILE, dpi=200, bbox_inches="tight")


if __name__ == "__main__":
    main()
