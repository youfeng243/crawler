#!/bin/bash

source `dirname $0`/../env.sh

Prog=python
FileDir="$CRAWLER_PATH/i_downloader/"
cd $FileDir
start() {
	nohup $Prog server.py -f 'downloader.toml' &>server.err &
	sleep 1
}

stop() {
    if [ -f "server.pid" ]; then
#	    cat server.pid |xargs kill
	    ps ux | grep download | grep server.py | grep -v grep | awk '{print $2}'| xargs kill -9
	    ps ux | grep phantomjs | grep -v grep | awk '{print $2}'| xargs kill -9
	    rm server.pid
	    sleep 1
	fi
}


status() {
    if [ -f "server.pid" ]; then
	    Pid=`cat server.pid`
	    [ -n "$Pid" ] &&  echo "${Pid}"
	fi
}


case "$1" in
	start|stop|status)
  		$1 ../deploy_conf/new_downloader.toml
		;;
	*)
		echo $"Usage: $0 {start|stop|status}"
		exit 1
esac

