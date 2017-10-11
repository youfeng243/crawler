#!/bin/bash

source `dirname $0`/../env.sh

Prog=python
FileDir="$CRAWLER_PATH/i_extractor/"
cd $FileDir
start() {
	nohup $Prog server.py -f $1 2>&1>server.err &
	sleep 1
	if [ -f "server.pid" ]; then
	    echo "start success"
	else
	    echo "start failed"
	fi
}

stop() {
    if [ -f "server.pid" ]; then
	    cat server.pid |xargs kill
	    while [ -f "server.pid" ]; do
	        sleep 1
	        echo "stopping"
	    done
	    echo "stopped"
	else
	    echo "Is not running"
	fi
}


status() {
	echo
	Pid=`cat server.pid`
	[ -z "$Pid" ] && echo "is not Running!" && echo "" && return 0
	[ -n "$Pid" ] && echo "is Running" && echo "" && return 1
}


case "$1" in
	start|stop|status)
  		$1 $2
		;;
	*)
		echo $"Usage: $0 {start|stop|status}"
		exit 1
esac

