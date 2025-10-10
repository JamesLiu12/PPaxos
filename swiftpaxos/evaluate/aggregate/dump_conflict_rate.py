from typing import List, Dict, Optional
import re
import yaml
import os

num_of_commands = 1000
conflicts = [i for i in range(0, 11)]

def read_duration(client: str) -> float:
    last_line: Optional[str] = None
    with open(client, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if stripped:
                last_line = stripped

    if last_line is None:
        raise ValueError(f"No non-empty lines found in {client!r}")

    if "Test took" not in last_line:
        raise ValueError(f"'Test took' not found in the last line of {client!r}")

    duration_token = last_line.split("Test took", 1)[1].strip().split()[0]

    pattern = re.compile(r"(\d+(?:\.\d+)?)(h|ms|µs|μs|us|ns|m|s)")
    unit_to_seconds = {
        "h": 3600.0,
        "m": 60.0,
        "s": 1.0,
        "ms": 1e-3,
        "us": 1e-6,
        "µs": 1e-6,
        "μs": 1e-6,
        "ns": 1e-9,
    }

    total_seconds = 0.0
    for value, unit in pattern.findall(duration_token):
        total_seconds += float(value) * unit_to_seconds[unit]

    if total_seconds == 0.0:
        raise ValueError(f"Unable to parse a duration from {duration_token!r}")

    return total_seconds

proto_conflict_speedup: Dict[str, Dict[int, float]] = {}

for conflict in conflicts:
    dir = f'/exports/paxos/conflict-{conflict}'
    
    proto_client_throughput: Dict[str, Dict[str, float]] = {}
    proto_client_speedup: Dict[str, Dict[str, float]] = {}

    protos = os.listdir(dir)

    for proto in protos:
        proto_client_throughput[proto] = {}
        for client in os.listdir(os.path.join(dir, proto)):
            duration = read_duration(client)
            proto_client_throughput[proto][client] = num_of_commands / duration
            print(f'[{conflict}] [{proto}] [{client}] throughput: {proto_client_throughput[proto][client]}')

    for proto in protos:
        proto_client_speedup[proto] = {}
        for client in os.listdir(os.path.join(dir, proto)):
            proto_client_speedup[proto][client] = proto_client_throughput[proto][client] / proto_client_throughput['paxos'][client]
            print(f'[{conflict}] [{proto}] [{client}] speedup: {proto_client_speedup[proto][client]}')

    for proto in protos:
        speedups = proto_client_speedup[proto]
        proto_conflict_speedup.setdefault(proto, {})[conflict] = sum(speedups) / len(speedups)

    with open('out/proto_conflict_speedup.yaml', 'w', encoding='utf-8') as f:
        yaml.safe_dump(proto_conflict_speedup, f, sort_keys=False)