from typing import List
import subprocess
import os
from abc import ABC, abstractmethod

class Node(ABC):
    repo_url = "https://github.com/JamesLiu12/PPaxos"
    repo_path = "~/PPaxos/"
    working_dir = os.path.join(repo_path, "swiftpaxos")
    
    def __init__(self, address: str, user: str, identity_file: str, config_path: str):
        self.address = address
        self.user = user
        self.identity_file = identity_file
        self.config_path = os.path.join(Node.working_dir, config_path)
        
    def ssh_cmd(self, *remote_cmd) -> List[str]:
        cmd = [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-i", self.identity_file,
            f"{self.user}@{self.address}",
            *remote_cmd
        ]
        print(cmd)
        return cmd
    
    def run_cmd(self, *remote_cmd):
        return subprocess.run(self.ssh_cmd(*remote_cmd))
    
    def init_repo(self, forced = False):
        check_cmd = self.ssh_cmd(f"test -d {Node.repo_path}")
        dir_exists = subprocess.run(check_cmd).returncode == 0
        if dir_exists and not forced:
            return None
        if dir_exists:
            self.run_cmd(f"rm -rf {Node.repo_path}")
        clone_cmd = self.ssh_cmd(f"git clone {Node.repo_url} {Node.repo_path} && cd {Node.working_dir} && go build -buildvcs=false")
        return subprocess.run(clone_cmd)
    
    @abstractmethod
    def run():
        pass

    @abstractmethod
    def kill():
        pass