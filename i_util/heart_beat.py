#!/usr/bin/env python
# -*- coding:utf-8 -*-
import fcntl
import socket
import struct
import threading
import time

import redis


def get_interface_ip(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack('256s', bytes(ifname[:15], 'utf-8'))
        # Python 2.7: remove the second argument for the bytes call
    )[20:24])
def get_ip_address():
    import os
    ip = None
    try:
        ip = socket.gethostbyname(socket.getfqdn())
        if ip and not ip.startswith('127'):return ip
    except Exception:
        pass
    if not ip or ip.startswith('127'):
        try:
            ip = os.popen('hostname --all-ip-address').readlines()[0].split(' ')[0]
            if ip and not ip.startswith('127'):return ip
        except Exception as e:
            pass
    if not ip or ip.startswith('127'):
        interfaces = ["eth0","eth1","eth2","wlan0","wlan1","wifi0","ath0","ath1","ppp0"]
        for ifname in interfaces:
            try:
                ip = get_interface_ip(ifname)
            except Exception:
                pass

    if not ip or ip.startswith('127'):
        raise Exception("can't get real local ip!")
    return None


class HeartbeatThread(threading.Thread):
    EXPIREDS = 30
    def __init__(self, server_name, conf):
        self.backend_host = conf['backend']['host']
        self.backend_port = conf['backend']['port']
        self.server_name = server_name
        self.server_host = conf['server']['host']
        self.server_port = conf['server']['port']
        self.backend_passwd = conf['backend']['password']
        self.log = conf['log']
        threading.Thread.__init__(self)
        self.backend = redis.Redis(host=self.backend_host, port=self.backend_port,db=0, password=self.backend_passwd)
        self.daemon = True
        self.running = True

        self.server_host = get_ip_address()
        #print socket.gethostbyname(socket.getfqdn())

    def run(self):
        while self.running:
            try:
                pipeline = self.backend.pipeline()

                now = time.time()
                server = "%s:%s:%s" % (self.server_name, self.server_host, self.server_port)
                pipeline.hset("i_crawler_server", server, now)
                pipeline.execute()
                self.log.info("report %s activate success." % server)

            except Exception, e:
                self.log.error("report activate failed, because %s" % e.message)
            time.sleep(self.EXPIREDS)

    def stop(self):
        self.running = False


if __name__ == "__main__":
    import logging
    from i_util.logs import LogHandler

    # log = LogHandler(config.logname, logging.DEBUG)


    log = LogHandler('test', logging.DEBUG)

    conf = {
        "backend": {"host": "101.201.102.37",
                    "port": 6379,
                    "password": "haizhi@)"},
        "server_name": "test",
        "server": {"host": "127.0.0.1",
                   "port": 1001},
        "log": log
    }

    obj = HeartbeatThread("extractor", conf=conf)
    obj.run()
