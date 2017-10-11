#!/usr/bin/env python
# -*- coding:utf-8 -*-
import threading
import time

import redis

from i_util.tools import multi_thread_singleton


class SERVER_STATUS(object):
    ACTIVE = 0
    WARNING = 1
    DEAD = 2

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


@multi_thread_singleton
class ServerManager(object):
    """
    ACTIVE_EXPIRED: 检查间隔时间
    ALERT_EXPIRED: 无响应警告超时
    DEAD: 确定服务已经挂
    """
    ACTIVE_EXPIRED =  10
    ALERT_EXPIRED = 60
    DEAD_EXPIRED = 20 * 60

    def __init__(self, redis_conf, logger):
        self.redis_connector = redis.Redis(
            host=redis_conf['host'],
            port=redis_conf['port'],
            password=redis_conf['password']
        )
        self.logger = logger
        self.listen_thread = threading.Thread(target=self.__listening_server)
        self.listen_thread.setDaemon(True)
        self.listen_thread.start()
        self.last_check_time = time.strftime(DATETIME_FORMAT)

    def start(self):
        self.listen_thread.start()

    def __listening_server(self):
        while True:
            try:
                self.__check()
            except Exception as e:
                self.logger.error(e.message)
            time.sleep(self.ACTIVE_EXPIRED)

    def __check(self):
        now = time.time()
        self.last_check_time = time.strftime(DATETIME_FORMAT, time.localtime(now))
        servers = self.redis_connector.hgetall("i_crawler_server")
        for k, v in servers.items():
            last_active_report = float(v)
            if now - last_active_report >= self.DEAD_EXPIRED:
                self.redis_connector.hdel("i_crawler_server", k)
    def get_server_info(self, server_name = None):
        now = time.time()
        result = {}
        servers = self.redis_connector.hgetall('i_crawler_server')
        for k, v in servers.items():
            server, host, port = k.split(':')
            last_active_report = float(v)
            if not result.has_key(server):
                result[server] = []
            dis_time = now - last_active_report
            if dis_time >= self.ALERT_EXPIRED:
                result[server].append({
                    "server":"%s:%s" % (host, port),
                    "status":SERVER_STATUS.WARNING,
                    "last_active_report":time.strftime(DATETIME_FORMAT, time.localtime(last_active_report))
                })
            else:
                result[server].append({
                    "server":"%s:%s" %(host, port),
                    "status":SERVER_STATUS.ACTIVE,
                    "last_active_report":time.strftime(DATETIME_FORMAT, time.localtime(last_active_report))
                })
        if server_name:
            return {
                server_name:result.get(server_name, [])
            }
        else:

            return result



if __name__ == "__main__":
    from conf import REDIS, log
    s1 = ServerManager(logger=log, redis_conf=REDIS)
    s2 = ServerManager(logger=log, redis_conf=REDIS)