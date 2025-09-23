from log_entry import LogEntry, load_from_yaml
from typing import List, Dict
from datetime import datetime

# Unit: message / sec
def cal_throughput(entries: List[LogEntry]):
    entries.sort(key=lambda e: (e.date, e.time))
    to_dt = lambda e: datetime.strptime(f"{e.date} {e.time}", "%Y/%m/%d %H:%M:%S")

    first_time = to_dt(entries[0])
    last_time  = to_dt(entries[-1])

    duration = (last_time - first_time).total_seconds()
    
    return len(entries) / duration

data_files = [f'out/conflict{i * 10}.yaml' for i in range(1)]

result: Dict[str, Dict[str, List[LogEntry]]] = {}

for data_file in data_files:
    data = load_from_yaml(data_file)
    
    for proto, clients in data.items():
        result[proto] = {}
        for client, entries in clients.items():
            result[proto][client] = cal_throughput(entries)
            print(f'[{proto}] [{client}] troughput: {result[proto][client]}')