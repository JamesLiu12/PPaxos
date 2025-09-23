from node import Node
from config_loader import ConfigLoader

if __name__ == "__main__":
    config_loader = ConfigLoader()
    master: Node = config_loader.master
    master.run_cmds([
        "sudo apt-get update -y",
        "sudo apt-get install -y nfs-kernel-server",
        "sudo mkdir -p /exports/paxos",
        "sudo rm -f /etc/exports",
        "sudo touch /etc/exports",
        'sudo bash -c "echo /exports/paxos *\(rw,no_subtree_check\) >> /etc/exports"',
        "sudo exportfs -ra",
        "sudo systemctl restart nfs-kernel-server"
        ])