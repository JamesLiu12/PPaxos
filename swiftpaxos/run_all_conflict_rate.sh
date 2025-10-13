#!/bin/bash

for c in {4..4}; do
    conflict=$((c * 10))
    python3 -m evaluate.run.change_conflict "$conflict"
    python3 -m evaluate.run.change_proto "paxos"
    python3 -m evaluate.run.change_all "$conflict" "paxos"
    python3 -m evaluate.run.run
    python3 -m evaluate.run.kill_all

done
