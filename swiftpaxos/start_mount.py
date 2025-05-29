import subprocess
import yaml
from dotenv import load_dotenv
import os

CONTAINERS_FILE = "./containers.yml"

load_dotenv('hosts.env')
hosts = [
    "18.181.68.30",
    "3.115.96.150",
    "54.178.167.57",
    "54.95.19.6"
]

containers = []

for i in range(len(hosts)):
    # each region takes a subnet
    host = hosts[i % len(hosts)]
    print(host)
    #print(PORT1, PORT2)
    ssh_command = ["sudo mount 57.182.103.74:/home/ubuntu/share share"]

    '''
    ssh_command = [
        "for pkg in docker.io docker-doc docker-compose docker-compose-v2 podman-docker containerd runc; do sudo apt-get remove $pkg; done",
        "sudo apt-get update",
        "sudo apt-get upgrade",
        "sudo apt-get install ca-certificates curl gnupg -y",
        "sudo install -m 0755 -d /etc/apt/keyrings",
        "curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg",
        "sudo chmod a+r /etc/apt/keyrings/docker.gpg",
        "echo \
        \"deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
        $(. /etc/os-release && echo \"$VERSION_CODENAME\") stable\" | \
        sudo tee /etc/apt/sources.list.d/docker.list > /dev/null",
        "sudo apt-get update",
        "sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y",
        "sudo apt install nfs-common -y",
        "sudo apt install nfs-server -y",
        "sudo systemctl start nfs-server",
        "sudo systemctl start rpcbind",
        "sudo systemctl enable nfs-server",
        "sudo systemctl enable rpcbind",
        "sudo docker swarm join --token SWMTKN-1-24uixvfxt6kbbrrt0q3sxpdnowl16mt8z4nhh3xetcq2hx5zuc-bwarqqxyucxt93u5e07r4rt59 57.182.103.74:2377",
        "mkdir share",
        "sudo exportfs -r",
        "sudo mount 57.182.103.74:/home/ubuntu/share share"
    ]
    '''

    for command in ssh_command:
        print(command)
        subprocess.call(['ssh', '-o', 'StrictHostKeyChecking=no', host, command])

with open(CONTAINERS_FILE, 'w+') as f:
    data = {}
    data['containers'] = containers
    yaml.dump(data, f)