#!/usr/bin/env bash

pid=`ps -ef|grep "scheduler.toml"|grep -v grep|awk '{print $2}'`
if [ "$pid" != "" ]; then
    kill $pid
    rm -rf /tmp/scheduler.pid
fi
