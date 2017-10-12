#!/bin/bash

while true
do

    python_pid=`ps -ef|grep "scheduler.toml"|grep -v grep|awk '{print $2}'`
    if [ "$python_pid" == '' ]; then
        source `dirname $0`/../env.sh
        FileName="$CRAWLER_PATH/i_scheduler/scheduler.toml"
        nohup python server.py -f ${FileName} > /dev/null 2>&1  &
    fi

sleep 10
done

