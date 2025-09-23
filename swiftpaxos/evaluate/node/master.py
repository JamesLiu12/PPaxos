from .node import Node
import os

class Master(Node):
    def __init__(self, address: str, user: str, identity_file: str, config_path: str, protocol: str = "paxos"):
        super().__init__(address, user, identity_file, config_path)
        self.protocol = protocol

    def run(self):
        return self.run_cmd(f"{os.path.join(Node.working_dir, 'swiftpaxos')}", 
                            "-run master", 
                            f"-config {self.config_path}", 
                            f"-protocol {self.protocol}")
    
    def kill(self):
        return self.run_cmd("pkill -f swiftpaxos")