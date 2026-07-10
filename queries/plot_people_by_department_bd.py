#!/usr/bin/env python3
"""
plot_people_by_department_bd.py

Usage:
python plot_people_by_department_bd.py <yaml_path>

Reads the aggregated department-courses YAML file and produces:
- people_by_dept_bar.pdf — horizontal stacked bar chart, all departments,
  with each bar partitioned by the role (from contacts.csv) of the people
  counted in that department.
"""

import sys
import json
import textwrap

import yaml
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
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
        dept = (entry.get("Department") or "(no department)").strip().replace("\n", " ")
        if not dept:
            dept = "(no department)"

        dept_people = set()
        for course in entry.get("Courses"):
            for person in course.get("teachers"):
                dept_people.add(person.get("uid"))

        role_counts = {}
        for uid in dept_people:
            role = get_role(uid)
            role_counts[role] = role_counts.get(role, 0) + 1

        row = {"department": dept, "people": len(dept_people)}
        row.update(role_counts)
        rows.append(row)

    df = pd.DataFrame(rows).fillna(0)
    role_cols = [c for c in df.columns if c not in ("department", "people")]
    df[role_cols] = df[role_cols].astype(int)

    return df.sort_values("people", ascending=False).reset_index(drop=True)


def shorten(name: str, maxlen: int = 38) -> str:
    subs = {
        "Dipartimento di ": "Dip. ",
        "Alma Mater Studiorum - Università di Bologna": "Alma Mater",
        "Centro Interdipartimentale di Ricerca Industriale su ICT": "CIRI ICT",
        "Centro di Ricerca sui Sistemi Elettronici per l'Ingegneria "
        "dell'Informazione\ne delle Telecomunicazioni 'Ercole De Castro' - "
        "ARCES (Advanced Research Center\non Electronic System)": "ARCES",
        "AFORM - Settore Servizi didattici Ingegneria-Architettura - "
        "Ufficio\nServizi di supporto per l'offerta formativa e la "
        "programmazione didattica": "AFORM - Ufficio Off. Formativa",
    }
    for k, v in subs.items():
        name = name.replace(k, v)
    name = name.replace("\n", " ")
    if len(name) > maxlen:
        name = name[:maxlen - 1] + "\u2026"
    return name


def wrap_label(name: str, width: int = 32) -> str:
    return "\n".join(textwrap.wrap(name, width))


def save_meta(pdf_path: str, caption: str, description: str = "") -> None:
    with open(pdf_path + ".meta.json", "w", encoding="utf-8") as f:
        json.dump({"caption": caption, "description": description}, f)


def make_bar(df: pd.DataFrame, out: str = "people_by_dept_bd_bar.pdf") -> None:
    bar_df = df.sort_values("people", ascending=True).copy()
    bar_df["short"] = bar_df["department"].apply(shorten)
    bar_df["label"] = bar_df["short"].apply(wrap_label)

    role_cols = [c for c in df.columns if c not in ("department", "people", "short", "label")]
    colorway = pio.templates["seaborn"].layout.colorway

    fig = go.Figure()
    for i, role in enumerate(role_cols):
        fig.add_trace(go.Bar(
            x=bar_df[role],
            y=bar_df["label"],
            orientation="h",
            name=role,
            marker_color=colorway[i % len(colorway)],
        ))

    fig.update_layout(
        barmode="stack",
        title={"text": ("Total People by Department, Partitioned by Role")},
        height=1000,
        width=1100,
        margin=dict(l=260, r=90, t=110, b=40),
        legend=dict(title="Role", orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    fig.update_xaxes(title_text="Total People")
    fig.update_yaxes(title_text="", tickfont=dict(size=11))

    if os.path.exists(out):
        os.remove(out)
    fig.write_image(out)
    print(f"Saved {out}")


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python plot_people_by_department_bd.py <yaml_path>",
              file=sys.stderr)
        sys.exit(1)

    data = load_yaml(sys.argv[1])
    df = aggregate_people(data)

    print(f"Loaded {len(df)} departments, total people: {df['people'].sum()}")

    make_bar(df)


if __name__ == "__main__":
    main()
