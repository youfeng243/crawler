#!/bin/bash

source `dirname $0`/../env.sh

Prog=python
FileName="$CRAWLER_PATH/deploy_conf/new_scheduler.toml"
#FileName='/Users/yanghaoying/Work/i_crawler/i_scheduler/server.py'
Cut=${FileName%%.*}

start() {
	status
    cd bin
    chmod 0700 monitrc
    ./monit 
    ./monit start scheduler

}

stop() {
	status
    cd bin
    ./monit stop scheduler
}

restart() {
	stop
	start
}


status() {
	Pid=`ps -ef | grep $FileName | grep $Prog | grep -v grep | awk '{print $2}'`
    [ -n $Pid ] && echo "$Pid" && return 1
}


case "$1" in
	start|stop|restart|status)
  		$1
		;;
	*)
		echo $"Usage: $0 {start|stop|status|restart}"
		exit 1
esac
