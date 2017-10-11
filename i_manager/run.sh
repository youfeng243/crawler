#!/bin/bash
source `dirname $0`/../env.sh
Prog=python
FileName="$CRAWLER_PATH/i_manager/server.py"
Cut=${FileName%%.*}

start() {
    cd ../
    sh build_thrift_all.sh
    cd i_manager
    if [[ -f server.pid ]]; then
	echo 'i_manager server is running'
	exit 1
	fi
	uwsgi uwsgi.ini
	echo 'start'
}

stop() {
    if [[ -f server.pid ]]; then
	    uwsgi --stop server.pid
	    pid=`cat server.pid`
	    while [[ 1 ]]
	    do
	        pcnt=`ps -ux |grep  $pid|awk  '{print $2}'|wc -l`
            if [[ $pcnt != 1 ]];then
                sleep 2
            else
                break
            fi
        done
            rm server.pid
            echo 'stopped'
    fi
}

restart() {
	stop
	start
}


status() {
    if [[ -f server.pid ]]; then
        pid=`cat server.pid`
        pcnt=`ps -ux |grep $pid|awk  '{print $2}'|wc -l`
        if [[ $pcnt != 2 ]];then
            #不存在
            return 0
        else
            #存在
            echo "$pid"
            return 1
        fi
    else
        #不存在
        return 0
    fi
}

case "$1" in
	start|stop|restart|status)
  		$1
		;;
	*)
		echo $"Usage: $0 {start|stop|status|restart}"
		exit 1
esac
