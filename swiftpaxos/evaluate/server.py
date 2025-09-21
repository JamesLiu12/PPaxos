from node import Node
import os

class Server(Node):
    def __init__(self, address: str, user: str, identity_file: str, config_path: str, alias: str, protocol: str = "paxos"):
        super().__init__(address, user, identity_file, config_path)
        self.alias = alias
        self.protocol = protocol

    def run(self):
        return self.run_cmd(f"{os.path.join(Node.working_dir, 'swiftpaxos')}", 
                            "-run server", 
                            f"-config {self.config_path}", 
                            f"-alias {self.alias}"
                            f"-protocol {self.protocol}")
    
    def kill(self):
        return self.run_cmd("pkill -f swiftpaxos")