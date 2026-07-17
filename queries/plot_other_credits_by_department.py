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


def aggregate_credits(data: list) -> pd.DataFrame:
    rows = []
    for entry in data:
        dept  = (entry.get("department") or "(no department)").strip().replace("\n", " ")
        if not dept:
            dept = "(no department)"
        total = sum(c.get("credits", 0) or 0 for c in (entry.get("courses") or []))
        rows.append({"department": dept, "credits": total})
    return (
        pd.DataFrame(rows)
        .sort_values("credits", ascending=False)
        .reset_index(drop=True)
    )

def make_bar(df: pd.DataFrame, out: str = "_other_credits_by_dept.pdf") -> None:
    bar_df = df.sort_values("credits", ascending=True).copy()
    bar_df["short"] = bar_df["department"]
    bar_df["label"] = bar_df["department"]

    fig, ax = plt.subplots(figsize=(11, 10))
    bars = ax.barh(bar_df["label"], bar_df["credits"], color="#4C72B0")

    ax.set_title("Total Credits by Department")
    ax.set_xlabel("Total Credits")
    ax.set_ylabel("")
    ax.tick_params(axis="y", labelsize=11)

    max_value = float(bar_df["credits"].max()) if not bar_df.empty else 0.0
    x_offset = max(1.0, max_value * 0.01)
    for bar, value in zip(bars, bar_df["credits"]):
        ax.text(
            float(value) + x_offset,
            bar.get_y() + bar.get_height() / 2,
            f"{int(value)}",
            va="center",
            ha="left",
            fontsize=10,
        )

    ax.set_xlim(0, max_value + x_offset * 8)
    fig.subplots_adjust(left=0.26, right=0.98, top=0.90, bottom=0.08)

    if os.path.exists( out ):
        os.remove( out )
    fig.savefig(out, format="pdf")
    plt.close(fig)
    print(f"Saved  {out}")


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python plot_credits_by_department.py <input.yaml>",
              file=sys.stderr)
        sys.exit(1)

    data = load_yaml(sys.argv[1])
    df   = aggregate_credits(data)

    print(f"Loaded {len(df)} departments, total credits: {df['credits'].sum()}")

    make_bar(df)

if __name__ == "__main__":
    main()
