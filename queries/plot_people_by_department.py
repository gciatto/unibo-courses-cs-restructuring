#!/usr/bin/env python3
"""
plot_people_by_department.py

Usage:
    python plot_people_by_department.py <input.yaml>

Reads the aggregated department-courses YAML file and produces:
  - people_by_dept_bar.pdf  — horizontal bar chart, all departments
"""

import sys
import json
import textwrap

import yaml
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import os

# ── Helpers ──────────────────────────────────────────────────────────────────

def load_yaml(path: str) -> list:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def aggregate_people(data: list) -> pd.DataFrame:
    rows = []
    for entry in data:
        dept  = (entry.get("Department") or "(no department)").strip().replace("\n", " ")
        if not dept:
            dept = "(no department)"
        dept_people = set()
        for course in entry.get( "Courses" ):
            for person in course.get( "name" ):
                dept_people.add( person );
        rows.append({"department": dept, "people": len( dept_people )})
    return (
        pd.DataFrame(rows)
        .sort_values("people", ascending=False)
        .reset_index(drop=True)
    )


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
    return "<br>".join(textwrap.wrap(name, width))


def save_meta(pdf_path: str, caption: str, description: str = "") -> None:
    with open(pdf_path + ".meta.json", "w", encoding="utf-8") as f:
        json.dump({"caption": caption, "description": description}, f)


def make_bar(df: pd.DataFrame, out: str = "people_by_dept_bar.pdf") -> None:
    bar_df = df.sort_values("people", ascending=True).copy()
    bar_df["short"] = bar_df["department"].apply(shorten)
    bar_df["label"] = bar_df["short"].apply(wrap_label)

    fig = go.Figure(go.Bar(
        x=bar_df["people"],
        y=bar_df["label"],
        orientation="h",
        marker_color=pio.templates["seaborn"].layout.colorway[0],
        text=bar_df["people"],
        textposition="outside",
        cliponaxis=False,
    ))
    fig.update_layout(
        title={
            "text": ( "Total People by Department" )
        },
        height=1000,
        width=1100,
        margin=dict(l=260, r=90, t=110, b=40),
    )
    fig.update_xaxes(title_text="Total People")
    fig.update_yaxes(title_text="", tickfont=dict(size=11))

    if os.path.exists( out ):
        os.remove( out )
    fig.write_image(out)
    print(f"Saved  {out}")


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python plot_people_by_department.py <input.yaml>",
              file=sys.stderr)
        sys.exit(1)

    data = load_yaml(sys.argv[1])
    df   = aggregate_people(data)

    print(f"Loaded {len(df)} departments, total people: {df['people'].sum()}")

    make_bar(df)

if __name__ == "__main__":
    main()
