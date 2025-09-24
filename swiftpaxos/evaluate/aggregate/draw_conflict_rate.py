from .log_entry import LogEntry, load_from_yaml
from typing import List, Dict
from datetime import datetime
import matplotlib.pyplot as plt

# Unit: message / sec
def cal_throughput(entries: List[LogEntry]) -> float:
    entries.sort(key=lambda e: (e.date, e.time))
    to_dt = lambda e: datetime.strptime(f"{e.date} {e.time}", "%Y/%m/%d %H:%M:%S")

    first_time = to_dt(entries[0])
    last_time  = to_dt(entries[-1])

    duration = (last_time - first_time).total_seconds()
    
    return len(entries) / duration

def cal_speedup_avg(speedups: Dict[str, float]) -> float:
    vals = speedups.values()
    return sum(vals) / len(speedups)

def cal_speedup_max(speedups: Dict[str, float]) -> float:
    return max(speedups.values())

def cal_speedup_min(speedups: Dict[str, float]) -> float:
    return min(speedups.values())

conflict_rates: List[int] = []
speedups_conflict: Dict[int, Dict[str, Dict[str, float]]] = {}  

data_files = [f'out/conflict{i * 10}.yaml' for i in range(1)]

conflict = 0

for data_file in data_files:
    data = load_from_yaml(data_file)

    throughputs: Dict[str, Dict[str, float]] = {}

    for proto, clients in data.items():
        throughputs[proto] = {}
        for client, entries in clients.items():
            throughputs[proto][client] = cal_throughput(entries)
            print(f'[{proto}] [{client}] troughput: {throughputs[proto][client]}')

    speedups: Dict[str, Dict[str, float]] = {}

    for proto, clients in throughputs.items():
        speedups[proto] = {}
        
        for client, throughput in clients.items():
            speedups[proto][client] = throughputs[proto][client] / throughputs['paxos'][client]
            print(f'[{proto}] [{client}] speedup: {speedups[proto][client]}')

    conflict_rates.append(conflict)
    speedups_conflict[conflict] = speedups
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