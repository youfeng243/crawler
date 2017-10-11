#!/bin/bash

# 在每个含有build_thrift.sh的目录中执行build_thrift.sh

CURRENT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCRIPT_FILENAME="build_thrift.sh"

cd "$CURRENT_DIR"

for script in `find . -name build_thrift.sh`; do
    cd `dirname $script`
    bash -x `basename $script`
    RET=$?
    if [[ $RET -eq 0 ]]; then
        echo -e "\033[42;37m\033[1m $script run finished ! \033[0m" >&2
    else
        echo -e "\033[41;37m\033[1m $script run failed ! \033[0m" >&2
        exit $RET
    fi
    cd ..
done

