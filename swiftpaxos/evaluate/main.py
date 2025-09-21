from master import Master
from server import Server
from client import Client
from node import Node
import os

# master = Master("ec2-95-40-58-105.ap-east-1.compute.amazonaws.com", 
#                 "ubuntu", 
#                 "~/roxwell.pem",  
#                 f"{os.path.join(Node.working_dir, 'aws.conf')}")

# master.kill()
# master.init_repo()
# master.run()

# server = Server("ec2-95-40-58-105.ap-east-1.compute.amazonaws.com", 
#                 "ubuntu", 
#                 "~/roxwell.pem", 
#                 f"{os.path.join(Node.working_dir, 'aws.conf')}", 
#                 "roxwell-1")

# server.kill()
# server.init_repo()
# server.run()

client = Client("ec2-95-40-58-105.ap-east-1.compute.amazonaws.com", 
                "ubuntu", 
                "~/roxwell.pem", 
                f"{os.path.join(Node.working_dir, 'aws.conf')}", 
                "roxwell-1")

client.kill()
client.init_repo()
client.run()