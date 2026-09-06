from __future__ import annotations

import csv
from pathlib import Path

import matplotlib.pyplot as plt
import seaborn as sns


ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data_processed" / "thesis_alignment" / "market_context_2025.csv"
OUTPUT = ROOT / "assets" / "figures" / "figure_2_market_context_and_segmentation.png"


def load_rows() -> list[dict[str, str]]:
    with DATA.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


rows = load_rows()
annual = {r["metric"]: float(r["value"]) for r in rows if r["record_type"] == "annual"}
age = [r for r in rows if r["metric"] == "age_25_64_combined_share"]

sns.set_theme(style="darkgrid")
plt.style.use("dark_background")
fig = plt.figure(figsize=(14, 8), facecolor="#071B2D")
grid = fig.add_gridspec(2, 5, height_ratios=[1.0, 1.65], hspace=0.42, wspace=0.18)

fig.suptitle(
    "MALTA 2025 — MARKET CONTEXT AND SEGMENTATION",
    x=0.055, y=0.965, ha="left", fontsize=19, fontweight="bold", color="#F7E7C4"
)
fig.text(0.055, 0.915, "OFFICIAL NSO TOURISM CONTEXT", color="#50B7D5", fontsize=10.5, fontweight="bold")

cards = [
    ("Inbound tourists", f"{annual['inbound_tourists']:,.0f}"),
    ("Total nights", f"{annual['total_nights']/1_000_000:.1f}m"),
    ("Tourist expenditure", f"€{annual['total_tourist_expenditure']/1_000_000:.1f}m"),
    ("Expenditure per tourist", f"€{annual['expenditure_per_capita']:,.0f}"),
    ("Arrivals vs 2024", f"+{annual['arrival_growth_vs_2024']:.1%}"),
]

for i, (label, value) in enumerate(cards):
    ax = fig.add_subplot(grid[0, i])
    ax.set_facecolor("#102C40")
    ax.text(0.5, 0.61, value, ha="center", va="center", color="#F4C76B", fontsize=20, fontweight="bold")
    ax.text(0.5, 0.27, label, ha="center", va="center", color="#E7EEF2", fontsize=9.4, wrap=True)
    ax.set_xticks([]); ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_color("#31546A")

ax = fig.add_subplot(grid[1, :3])
ax.set_facecolor("#0B2437")
labels = [r["period"] for r in age]
values = [float(r["value"]) * 100 for r in age]
bars = sns.barplot(x=values, y=labels, hue=labels, palette="Blues_r", legend=False, ax=ax)
ax.set_xlim(0, 100)
ax.set_xlabel("Combined share of visitors aged 25–64 (%)", color="#D6E3EA")
ax.set_ylabel("Selected NSO release month", color="#D6E3EA")
ax.set_title("Selected monthly adult visitor profile", loc="left", color="#F7E7C4", fontsize=12, fontweight="bold")
ax.tick_params(colors="#D6E3EA")
for bar, value in zip(bars.patches, values):
    ax.text(value + 1.2, bar.get_y() + bar.get_height()/2, f"{value:.1f}%", va="center", color="#F4C76B", fontweight="bold")

note = fig.add_subplot(grid[1, 3:])
note.set_facecolor("#102C40")
note.set_xticks([]); note.set_yticks([])
for spine in note.spines.values():
    spine.set_color("#31546A")
note.text(0.07, 0.88, "PORTFOLIO PLANNING INTERPRETATION", color="#50B7D5", fontsize=10.5, fontweight="bold")
note.text(0.07, 0.68, "Test", color="#F4C76B", fontsize=10, fontweight="bold")
note.text(0.07, 0.57, "Adult recovery, couples and\nskin-wellness propositions", color="#E7EEF2", fontsize=11, linespacing=1.35)
note.text(0.07, 0.40, "Boundary", color="#F4C76B", fontsize=10, fontweight="bold")
note.text(0.07, 0.34, "Tourism is an external demand pool.\nIt is not measured spa demand, and\nselected months are not a full-year\ndemographic guarantee.", color="#E7EEF2", fontsize=10.3, linespacing=1.35, va="top")

fig.text(0.055, 0.025, "Source: NSO Malta, Inbound Tourism releases (Jan, Mar, Oct and Dec 2025). Data accessed 2026-09-06.", color="#9FB6C4", fontsize=8.6)
OUTPUT.parent.mkdir(parents=True, exist_ok=True)
fig.savefig(OUTPUT, dpi=240, bbox_inches="tight", facecolor=fig.get_facecolor())
plt.close(fig)
print(f"WROTE {OUTPUT}")
