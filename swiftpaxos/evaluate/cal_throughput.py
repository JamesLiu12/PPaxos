import re
from config_loader import ConfigLoader
import os
from node import Node

ConfigLoader()

test_number = 1

result_dir = os.path.join(Node.nfs_server_path, str(test_number))

def traverse_results(result_dir: str):
    if not os.path.isdir(result_dir):
        print(f'result_dir not found or not a directory: {result_dir}')
        return

    for proto_name in os.listdir(result_dir):
        proto_path = os.path.join(result_dir, proto_name)
        if not os.path.isdir(proto_path):
            continue

        for entry in os.listdir(proto_path):
            if not entry.startswith('client'):
                continue
            file_path = os.path.join(proto_path, entry)
            if not os.path.isfile(file_path):
                continue

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    logs = f.readlines()
            except UnicodeDecodeError:
                with open(file_path, 'r', encoding='gbk', errors='ignore') as f:
                    logs = f.readlines()

            filtered = list(filter(is_useful, (line.strip() for line in logs)))
            
            print(f'[{proto_name}] {entry}')
            for line in filtered:
                print(line)

def is_useful(log: str):
    data = log.split(' ')
    return len(data) == 3 and is_date(data[0]) and is_time(data[1]) and is_float(data[2])

def is_date(data: str):
    return bool(re.fullmatch(r'\d{4}/\d{2}/\d{2}', data))

def is_time(data: str):
    return bool(re.fullmatch(r'\d{2}:\d{2}:\d{2}', data))

def is_float(data: str):
    return bool(re.fullmatch(r'(?:\d+\.\d*|\.\d+)', data))

if __name__ == '__main__':
    traverse_results(result_dir)