#!/bin/bash

for c in {0..10}; do
    python3 -m evaluate.run.change_conflict "$c"
    python3 -m evaluate.run.run
    python3 -m evaluate.run.kill_all
done
