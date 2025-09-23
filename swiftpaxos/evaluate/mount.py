from node import Node
from config_loader import ConfigLoader
from typing import List

if __name__ == "__main__":
    config_loader = ConfigLoader()
    nodes: List[Node] = config_loader.servers + config_loader.clients
    master: Node = config_loader.master

    for node in nodes:
        node.run_cmds([
            "sudo apt-get update -y",
            "sudo apt-get install -y nfs-common",
            "sudo mkdir -p /mnt/nfs/paxos",
            f"sudo mount {master.address}:/exports/paxos /mnt/nfs/paxos"
            ])