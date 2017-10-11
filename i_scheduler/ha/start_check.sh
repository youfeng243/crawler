#!/usr/bin/env bash
set -e

my_state='master'
master_ip='172.16.0.7'
slave_ip='172.16.0.6'

echo $$ >/tmp/check.pid
function check_pid(){
    pid=`ps -ef|grep scheduler.litao|grep -v grep|awk '{print $2}'`
    echo $pid
}

while true
do
    if [ "$my_state" = "master" ]; then
        pid=$(check_pid)
        if [ "$pid" = "" ]; then
            cd /home/litao/monit-5.21.0/bin
            ./monit
            ./monit start scheduler
        else
            sleep 6
        fi
    fi
done
