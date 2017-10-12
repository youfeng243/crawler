#!/usr/bin/env bash

CURRENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ ! -n "$CRAWLER_PATH" ]; then
    #export CRAWLER_PATH='/data/crawler'
    export CRAWLER_PATH="${CURRENT_DIR}"
fi
mkdir -p "$CURRENT_DIR/logs"
Prog=/home/work/env/python-2.7/bin/python
PATH="/home/work/env/python-2.7/bin/:$HOME/.local/bin:$HOME/bin:$PATH"
export PATH
