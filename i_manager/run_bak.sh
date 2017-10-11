#!/bin/bash

source `dirname $0`/../env.sh

Prog=python
FileName="$CRAWLER_PATH/i_manager/server.py"
Cut=${FileName%%.*}

start() {
	status
	[ $? -eq 1 ] && return 1
	echo $"Starting $FileName ... "
	nohup $Prog $FileName 1>$Cut".out" 2>$Cut".err" &
	retval=$?
	echo 
	$Prog --version
	status
	return $retval
}

stop() {
	status
	[ $? -eq 0 ] && return 0
	echo $"Stopping $FileName ... "
	sleep 1
	Pid=`ps -ef | grep $FileName | grep $Prog | grep -v grep | awk '{print $2}'`
	kill -9 $Pid
	retval=$?
	status
	return $retval
}

restart() {
	stop
	start
}


status() {
	echo 
	Pid=`ps -ef | grep $FileName | grep $Prog | grep -v grep | awk '{print $2}'`
	[ -z "$Pid" ] && echo "$FileName is not Running!" && echo "" && return 0
	[ -n "$Pid" ] && echo "$FileName is Running" && echo "" && return 1
}


case "$1" in
	start|stop|restart|status)
  		$1
		;;
	*)
		echo $"Usage: $0 {start|stop|status|restart}"
		exit 1
esac
