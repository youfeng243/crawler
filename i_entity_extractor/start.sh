#!/usr/bin/env sh
filename=$1
source ../env.sh
nohup python server.py -f 'entity_extractor.toml' 2>&1 &>>server.err &
