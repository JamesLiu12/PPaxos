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
            f"sudo mkdir -p {Node.nfs_client_path}",
            f"sudo mount {master.address}:{Node.nfs_server_path} {Node.nfs_client_path}"
            ])