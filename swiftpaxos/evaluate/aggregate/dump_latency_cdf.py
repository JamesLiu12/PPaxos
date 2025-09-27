from .log_entry import LogEntry, load_from_yaml
from typing import List, Dict, Optional
from datetime import datetime
import yaml

proto_latencies: Dict[str, List[float]]

min_latency = 9999999999.0
min_proto = None
min_client = None

max_latency = 0.0
max_proto = None
max_client = None

data_files = [f'out/conflict{i * 10}.yaml' for i in range(1)]

conflict = 0

for data_file in data_files:
    data = load_from_yaml(data_file)

    throughputs: Dict[str, Dict[str, float]] = {}

    for proto, clients in data.items():
        proto_latencies[proto] = []
        for client, entries in clients.items():
            latencies = [entry.rtt for entry in entries]
            proto_latencies[proto] += latencies
            latency = sum(latencies) / len(latencies)
            
            if latency < min_latency:
                min_latency = latency
                min_proto = proto
                min_client = client
            
            if latency > max_latency:
                max_latency = latency
                max_proto = proto
                max_client = client

    conflict += 10