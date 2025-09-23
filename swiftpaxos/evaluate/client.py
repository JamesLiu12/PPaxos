from node import Node
import os

class Client(Node):
    def __init__(self, address: str, user: str, identity_file: str, config_path: str, alias: str, protocol: str = "paxos"):
        super().__init__(address, user, identity_file, config_path)
        self.alias = alias
        self.protocol = protocol

    def run(self):
        log_dir = os.path.join(Node.nfs_client_path, Node.start_time, self.protocol)
        log_file = os.path.join(log_dir, self.alias)
        self.run_cmd(f"mkdir -p {log_dir}")
        self.run_cmd(f"touch {log_file}")
        return self.run_cmd(f"{os.path.join(Node.working_dir, 'swiftpaxos')}", 
                            "-run client", 
                            f"-config {self.config_path}", 
                            f"-alias {self.alias}",
                            f"-protocol {self.protocol}",
                            f"-log /mnt/nfs/paxos/{Node.start_time}/{self.protocol}/{self.alias}")
    
    def kill(self):
        return self.run_cmd("pkill -f swiftpaxos")