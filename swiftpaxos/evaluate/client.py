from node import Node
import os

class Client(Node):
    def __init__(self, address: str, user: str, identity_file: str, config_path: str, alias: str, protocol: str = "paxos"):
        super().__init__(address, user, identity_file, config_path)
        self.alias = alias
        self.protocol = protocol

    def run(self):
        self.run_cmd(f"mkdir -p /mnt/nfs/paxos/{Node.start_time}/{self.protocol}/{self.alias}")
        return self.run_cmd(f"{os.path.join(Node.working_dir, 'swiftpaxos')}", 
                            "-run client", 
                            f"-config {self.config_path}", 
                            f"-alias {self.alias}",
                            f"-protocol {self.protocol}",
                            f"-log /mnt/nfs/paxos/{Node.start_time}/{self.protocol}/{self.alias}")
    
    def kill(self):
        return self.run_cmd("pkill -f swiftpaxos")