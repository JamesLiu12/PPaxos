import os
import csv

# Outputs csv file 

# ROOT_PATH = "/mnt/share/exp"
ROOT_PATH = "out"

#Compute average for an alias
def get_avg_latency(exp, protocol, alias):
    folder_path = f"{ROOT_PATH}/exp{exp}/{protocol}/{alias}"
    file_names = [x for x in os.listdir(folder_path) if x.split("_")[0] == alias]

    avg = 0
    count = 0
    for file_name in file_names:
        f = open(f"{folder_path}/{file_name}", "r")

        line = f.readline()
        while line:
            arr = line.split()
            if not (len(arr) < 4 or arr[2] != "latency"):
                count+=1
                latency = float(arr[3])
                avg = (avg*(count - 1) + latency)/count
            line = f.readline()
        f.close()
    return avg, count

#Compute average per protocol e.g. Per aws region
def compute_protocol_avgs(exp, protocol):
    folder_path = f"{ROOT_PATH}/exp{exp}/{protocol}"
    alias_folder = [x for x in os.listdir(folder_path)]

    avgs = {}
    for i in range(len(alias_folder)):
        avg, count = get_avg_latency(exp, protocol, alias_folder[i])
        avgs[alias_folder[i]] = {"avg": avg, "count": count}
    return avgs

experiment_number = 5
folder_path = f"{ROOT_PATH}/exp{experiment_number}"
protocols = [x for x in os.listdir(folder_path) if os.path.isdir(f"{folder_path}/{x}")]


protocol_avgs = []
for protocol in protocols:
    avgs = compute_protocol_avgs(experiment_number, protocol)
    for alias in avgs:
        alias_avg = avgs[alias]
        protocol_avgs.append({"protocol": protocol, "alias": alias, "req_count": alias_avg["count"], "latency": alias_avg["avg"]})

#Write to a csv
#protocol, alias, req_count, latency
field_names = ["protocol", "alias", "req_count", "latency"]
f = open(f"{folder_path}/agg.csv", "w")
writer = csv.DictWriter(f, fieldnames=field_names)

writer.writeheader()
for avg in protocol_avgs:
    writer.writerow(avg)

f.close() 
