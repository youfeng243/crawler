#!/usr/bin/Python
# coding=utf-8
import signal
import sys
import os
import traceback
import getopt
import pytoml
from scheduler import Scheduler
from scheduler_proccessor import SchedulerProccessor 
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
sys.path.append('..')
from i_util.thread_pool import ThreadPool
from i_util.input_thread import InputThread
from i_util.heart_beat import HeartbeatThread
# from i_util.logs import LogHandler
from common.log import log
from bdp.i_crawler.i_scheduler import SchedulerService
from i_util.i_crawler_util import str_dict
from i_util.ProfilingUtil import profiler_creator
from i_util.ProfilingUtil import profiling_signal_handler


class SchedulerServer(object):
    def __init__(self, conf, scheduler):
        self.conf = conf
        self.log = conf['log']
        self.scheduler = scheduler
        self.process_thread_num = conf['server']['process_thread_num']
        thread_locals = {'processor': (SchedulerProccessor, (conf['log'], self.scheduler)), 'profiler': (profiler_creator, ())}
        self.process_pool = ThreadPool(self.process_thread_num, thread_locals)
        self.input_thread = InputThread(conf['beanstalk_conf'], conf['log'], self.process_pool)
        self.heartbeat_thread = HeartbeatThread('scheduler', self.conf)

    def start(self):
        self.input_thread.start()
        self.heartbeat_thread.start()
        self.log.info("start_server\tSchedulerServer!")

    def stop(self, message):
        self.input_thread.stop()
        self.heartbeat_thread.stop()
        self.log.info("stop_server\tstatus:%s!" % message)
        exit(1)


class ScheduleHandler(object):

    def __init__(self, scheduler):
        self.scheduler = scheduler

    def schedule_tasks(self, index_task, item_tasks):
        return self.scheduler.schedule_tasks(index_task, item_tasks)

    def start_one_site_tasks(self, site):
        return self.scheduler.start_one_site_tasks(site)

    def stop_one_site_tasks(self, site):
        return self.scheduler.stop_one_site_tasks(site)

    def clear_one_site_cache(self, site):
        return self.scheduler.clear_one_site_cache(site)

    def restart_seed(self, seed_id, site):
        return self.scheduler.restart_seed(seed_id, site)

    def dispatch_task(self):
        task = self.scheduler.dispatch_task()
        str_dict(task)
        return task


def main(conf):
    # 启动调度器
    scheduler = Scheduler(conf)
    scheduler.start()
    # 启动输入流程
    scheduler_server = SchedulerServer(conf, scheduler)
    scheduler_server.start()

    def signal_handler(signalnum, frame):
        conf['log'].info("received a signal:%s" % signalnum)
        scheduler_server.stop("stops")
        exit(1)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGUSR1, lambda a, b: profiling_signal_handler("scheduler", a, b))

    try:
        handler = ScheduleHandler(scheduler)
        processor = SchedulerService.Processor(handler)
        transport = TSocket.TServerSocket(port=conf['server']['port'])
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory, daemon=True)
        server.setNumThreads(conf['server']['server_thread_num'])
        server.serve()
    except Exception, e:
        conf['log'].error(str(traceback.format_exc()))
        scheduler_server.stop("fail")
        os.exit(1)
    scheduler_server.stop("success")


def usage():
    pass


if __name__ == '__main__':
    try:
        file_path = './scheduler.toml'
        opt, args = getopt.getopt(sys.argv[1:], 'f:', ['help'])
        for name, value in opt:
            if name == "-f":
                file_path = value
            elif name in ("-h", "--help"):
                usage()
                sys.exit()
            else:
                assert False, "unhandled option"

        with open(file_path, 'rb') as config:
            conf = pytoml.load(config)

        log.init_log(conf, level=conf['logger']['level'], console_out=conf['logger']['console'],
                     name=conf['server']['name']+str(conf['server']['port']))
        conf['log'] = log

        main(conf)
        exit(0)

    except Exception, e:
        print traceback.format_exc()
        sys.exit(1)
