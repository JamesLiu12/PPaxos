from .log_entry import LogEntry, load_from_yaml
from typing import List, Dict, Optional
from datetime import datetime
import matplotlib.pyplot as plt

# Unit: ms
def cal_latency(entries: List[LogEntry]) -> Optional[float]:
    if not entries:
        return None
    
    return sum(e.rtt for e in entries) / len(entries)

conflict_rates: List[int] = []
speedups_conflict: Dict[int, Dict[str, Dict[str, float]]] = {}

data_files = [f'out/conflict{i * 10}.yaml' for i in range(1)]

conflict = 0

for data_file in data_files:
    data = load_from_yaml(data_file)

    latencies: Dict[str, Dict[str, float]] = {}

    for proto, clients in data.items():
        latencies[proto] = {}
        for client, entries in clients.items():
            latency = cal_latency(entries)
            if latency: 
                latencies[proto][client] = latency
                print(f'[{proto}] [{client}] troughput: {latencies[proto][client]}')

    conflict_rates.append(conflict)
    conflict += 10

all_protos = set()
for c in speedups_conflict.values():
    all_protos.update(c.keys())
all_protos = sorted(all_protos)

series_avg: Dict[str, List[float]] = {p: [] for p in all_protos}
series_max: Dict[str, List[float]] = {p: [] for p in all_protos}
series_min: Dict[str, List[float]] = {p: [] for p in all_protos}

conflict_rates_sorted = sorted(speedups_conflict.keys())

for cr in conflict_rates_sorted:
    per_proto = speedups_conflict[cr]  # {proto: {client: speedup}}
    for p in all_protos:
        d = per_proto.get(p, {})
        series_avg[p].append(cal_speedup_avg(d))
        series_max[p].append(cal_speedup_max(d))
        series_min[p].append(cal_speedup_min(d))

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
        ax.plot(conflict_rates_sorted, ys, label=p, **style)
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