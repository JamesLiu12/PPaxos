from master import Master
from server import Server
from client import Client
from node import Node
import os
import yaml
import threading
from typing import List, Dict

def read_config(path) -> Dict:
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def run_node(node: Node):
    node.kill()
    node.init_repo()
    node.run()

if __name__ == "__main__":
    config = read_config("evaluate/config.yaml")

    Node.repo_url = config["repo_url"]
    Node.repo_path = config["repo_path"]
    Node.working_dir = config["working_dir"]

    master_conf = config["master"]
    master = Master(
        master_conf["address"],
        master_conf["user"],
        master_conf["identity_file"],
        master_conf["config_path"],
        master_conf.get("protocol", "paxos")
    )

    server_conf = config["servers"]
    servers = [
        Server(
            server_conf["address"],
            server_conf["user"],
            server_conf["identity_file"],
            server_conf["config_path"],
            server_conf["alias"],
            server_conf.get("protocol", "paxos")
        ) for server_conf in config["servers"]
    ]

    client_conf = config["clients"]
    clients = [
        Client(
            client_conf["address"],
            client_conf["user"],
            client_conf["identity_file"],
            client_conf["config_path"],
            client_conf["alias"]
        ) for client_conf in config["clients"]
    ]

    threads: List[threading.Thread] = []
    # Master
    threads.append(threading.Thread(target=run_node, args=(master,)))
    # Servers
    for server in servers:
        threads.append(threading.Thread(target=run_node, args=(server,)))
    # Clients
    for client in clients:
        threads.append(threading.Thread(target=run_node, args=(client,)))

    for t in threads:
        t.start()
    for t in threads:
        t.join()