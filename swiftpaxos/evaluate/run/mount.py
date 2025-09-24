from node import Server, Client
from config_loader import ConfigLoader
from typing import List

if __name__ == "__main__":
    config_loader = ConfigLoader()
    nodes: List[Server | Client] = config_loader.servers + config_loader.clients

    for node in nodes:
        node.mount()