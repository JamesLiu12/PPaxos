from master import Master
from server import Server
from client import Client
from node import Node
from typing import List, Dict
import os
import yaml

class ConfigLoader:
    def __init__(self, config_path = "evaluate/config.yaml"):
        self.master: Master = None
        self.servers: List[Master] = []
        self.clients: List[Client] = []

        config = self._read_config(config_path)

        Node.repo_url = config["repo_url"]
        Node.repo_path = config["repo_path"]
        Node.working_dir = config["working_dir"]
        Node.nfs_server_path = config["nfs_server_path"]
        Node.nfs_client_path = config["nfs_client_path"]
        Node.test_number = config["test_number"]

        master_conf = config["master"]
        self.master = Master(
            master_conf["address"],
            master_conf["user"],
            master_conf["identity_file"],
            master_conf["config_path"],
            master_conf.get("protocol", "paxos")
        )

        server_conf = config["servers"]
        self.servers = [
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
        self.clients = [
            Client(
                client_conf["address"],
                client_conf["user"],
                client_conf["identity_file"],
                client_conf["config_path"],
                client_conf["alias"]
            ) for client_conf in config["clients"]
        ]

    def _read_config(self, path) -> Dict:
        with open(path, 'r') as f:
            return yaml.safe_load(f)    
