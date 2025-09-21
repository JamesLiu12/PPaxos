from master import Master
from node import Node
import os

master = Master("ec2-95-40-58-105.ap-east-1.compute.amazonaws.com", 
                "ubuntu", 
                "~/roxwell.pem", 
                "master", 
                f"{os.path.join(Node.working_dir, 'aws.conf')}")

master.kill()
master.init_repo()
master.run()