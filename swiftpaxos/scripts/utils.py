import json

def read_json(path, roles):
    objs = []

    f = open(path, "r")

    js = json.load(f)["nodes"]
    for j in js:
        role = j["roles"]
        if role in roles:
            objs.append(j)

    f.close()
    return objs

def read_conf(path, key):
    f = open(path, "r")
    val = json.load(f)[key]
    f.close()
    return val

#Testing
if __name__ == "__main__":
    print(json.dumps(read_json("scripts/conf.json", ["replica"]), indent=4))