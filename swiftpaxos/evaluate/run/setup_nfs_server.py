from node import Node
from config_loader import ConfigLoader

if __name__ == "__main__":
    config_loader = ConfigLoader()
    master: Node = config_loader.master
    master.run_cmds([
        "sudo apt-get update -y",
        "sudo apt-get install -y nfs-kernel-server",
        f"sudo mkdir -p {Node.nfs_server_path}",
        f"sudo chmod 777 {Node.nfs_server_path}",
        "sudo rm -f /etc/exports",
        "sudo touch /etc/exports",
        f'sudo bash -c "echo {Node.nfs_server_path} *\(rw,no_subtree_check\) >> /etc/exports"',
        "sudo exportfs -ra",
        "sudo systemctl restart nfs-kernel-server"
        ])