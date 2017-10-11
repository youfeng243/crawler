#!/usr/bin/env sh

set -e 
filename='/home/work/apps/crawler/deploy_conf/new_scheduler.toml'

cd /home/work/apps/crawler/i_scheduler
source ../env.sh
nohup python server.py -f $filename 2>&1 &>>server.err &

pid=`ps -ef|grep "scheduler.toml"|grep -v grep|awk '{print $2}'`
if [ "$pid" != "" ]; then
    echo $pid > /tmp/scheduler.pid
fi
