from typing import Dict, List
import yaml
import matplotlib.pyplot as plt

with open('out/conflict_proto_speedup.yaml', 'r', encoding='utf-8') as f:
    conflict_proto_speedup: Dict[int, Dict[str, Dict[str, float]]] = yaml.safe_load(f) or {}

all_protos = set()
for c in conflict_proto_speedup.values():
    all_protos.update(c.keys())

series_avg: Dict[str, List[float]] = {p: [] for p in all_protos}
series_max: Dict[str, List[float]] = {p: [] for p in all_protos}
series_min: Dict[str, List[float]] = {p: [] for p in all_protos}

conflict_rates = []

for cr, proto_speedup in conflict_proto_speedup.items():
    conflict_rates.append(cr)
    for p, speedup in proto_speedup.items():
        series_avg[p].append(speedup['avg'])
        series_max[p].append(speedup['max'])
        series_min[p].append(speedup['min'])

fig, axes = plt.subplots(3, 1, sharex=True)

titles = ["Average", "Best", "Worst"]
y_limits = [(0.9, 2.0), (0.9, 2.0), (0.9, 2.0)]
y_label = "speedup"

axes[-1].set_xticks(list(range(0, 101, 20)))
for ax in axes:
    ax.set_xlim(0, 100)

proto_style = {
    "SwiftPaxos": dict(color="#F39C12", marker="x", linestyle="-", linewidth=2),
    "CURP+":      dict(color="#3B82F6", marker="+", linestyle="-"),
    "EPaxos":     dict(color="#1ABC9C", marker="^", linestyle="-"),
    "FastPaxos":  dict(color="#0EA5E9", marker="o", linestyle="-"),
    "GPaxos":     dict(color="#6366F1", marker="s", linestyle="-"),
    "N2P":        dict(color="#16A34A", marker="v", linestyle="-"),
    "paxos":      dict(color="#000000", marker="o", linestyle="-", linewidth=1.5),
}

def plot_panel(ax, title, data_dict):
    for p in all_protos:
        ys = data_dict[p]
        style = proto_style.get(p, dict(marker="o", linestyle="-"))
        ax.plot(conflict_rates, ys, label=p, **style)
    ax.set_ylim(*y_limits[titles.index(title)])
    ax.set_ylabel(y_label)
    ax.set_title(title)
    ax.grid(True, linestyle=":", alpha=0.4)

plot_panel(axes[0], "Average", series_avg)
plot_panel(axes[1], "Best",    series_max)
plot_panel(axes[2], "Worst",   series_min)

axes[-1].set_xlabel("conflict rate (%)")

axes[0].legend(ncol=3, loc="upper left", fontsize=9, frameon=False)

plt.tight_layout()
plt.savefig("out/speedup_panels.png", dpi=200, bbox_inches="tight")