#!/bin/bash

source `dirname $0`/../env.sh

Prog=python
FileName="$CRAWLER_PATH/i_scheduler/scheduler.toml"

start() {
	status
    if [ $? = 0 ]; then
        echo "调度程序已经启动, 不需要再启动..."
        return
    fi
    nohup sh scheduler_monitor.sh > /dev/null 2>&1  &
    echo "调度程序启动成功..."
}

stop() {

    ps -ef | grep scheduler_monitor |grep -v grep|awk '{print $2}' | xargs kill -9
    ps -ef|grep "scheduler.toml"|grep -v grep|awk '{print $2}' | xargs kill -9
    echo "调度器关闭完成..."

}

restart() {
	stop
	start
}


status() {
	Pid=`ps -ef | grep scheduler_monitor |grep -v grep|awk '{print $2}'`
    [ "$Pid" != "" ] && echo "$Pid" && return 0
    return 1
}


case "$1" in
	start|stop|restart|status)
  		$1
		;;
	*)
		echo $"Usage: $0 {start|stop|status|restart}"
		exit 1
esac
