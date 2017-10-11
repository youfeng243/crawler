#-*- coding: utf-8 -*-
import itertools
from beanstalkc import Connection
from beanstalkc import SocketError

class PyBeanstalk(object):

    def __init__(self, host, port=11300):
        """
        Args:
            host: host1_ip:host2_ip:...
            port: 11300
        """
        self.host = host
        self.port = port
        self.current_use_server_index = 0
        self.servers = []
        hosts =host.split(":")
        serverlist = itertools.product(hosts, [port])
        for s in serverlist:
            conn = Connection(host=s[0], port=s[1], connect_timeout=20)
            conn.connect()
            self.servers.append(conn)
    def __del__(self):
        for conn in self.servers:
            try:
                conn.close()
            except:
                pass
        del self.servers[:]
    #beanstalk重连
    def reconnect(self):
        #不在这里做重连操作
        pass

    def put(self, tube, body, priority=2**31, delay=0, ttr=30):
        conn = None
        while True:
            try:
                conn = self.servers[self.current_use_server_index]
                conn.use(tube)
                if len(tube) >= 3145728:
                    return None
                result = conn.put(body, priority, delay, ttr)
                return {'server':"{}:{}".format(conn.host, conn.port), "data":result}
            except SocketError:
                try:
                    conn.reconnect()
                except:
                    self.current_use_server_index = (self.current_use_server_index + 1) % len(self.servers)

    def reserve(self, tube, timeout=20):
        job = None
        begin_idx = self.current_use_server_index
        end_idx = self.current_use_server_index + len(self.servers)
        for idx in xrange(begin_idx, end_idx):
            valid_idx = idx % len(self.servers)
            conn = self.servers[valid_idx]
            try:
                conn.watch(tube)
                job = conn.reserve(1)
                if job:
                    self.current_use_server_index = valid_idx
                    break
            except SocketError:
                try:
                    conn.reconnect()
                except:
                    self.current_use_server_index = (self.current_use_server_index +1) % len(self.servers)
            except Exception as e:
                pass
        return job

    def clear(self, tube):
        try:
            for conn in self.servers:
                conn.watch(tube)
                while 1:
                    job = conn.reserve(1)
                    if job is None:
                        break
                    else:
                        job.delete()
        except Exception, e:
            print e

    def stats_tube(self, tube):
        result = []
        for conn in self.servers:
            try:
                status = conn.stats_tube(tube)
                result.append({"status":"ok", 'data':status, 'server':"{}:{}".format(conn.host, conn.port)})
            except Exception as e:
                result.append({"status":"fail", "data":str(e), 'server':"{}:{}".format(conn.host, conn.port)})

        return result
    def get_tube_count(self, tube):
        result = []
        for conn in self.servers:
            try:
                status = conn.stats_tube(tube)
                result.append({"status":"ok", 'data':status['current-jobs-ready'], 'server':"{}:{}".format(conn.host, conn.port)})
            except Exception as e:
                result.append({"status":"fail", "data":str(e), 'server':"{}:{}".format(conn.host, conn.port)})

        return result
if __name__ == '__main__':
    pybeanstalk = PyBeanstalk('101.201.102.37')
    job =  pybeanstalk.reserve('extract_info')
    print len(job.body)
    job.delete()
