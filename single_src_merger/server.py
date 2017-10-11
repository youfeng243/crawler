#!/usr/bin/Python
# coding=utf-8
import os
import signal
import sys
import getopt
import pytoml

from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport

sys.path.append('..')
from bdp.i_crawler.single_source_merge import SingleSourceMergeService
from i_util.heart_beat import HeartbeatThread
from bdp.i_crawler.single_source_merge.ttypes import ResultResp
import traceback
import json
from single_src_merger import SingleSourceMerger
from common.record_lock_manager import RecordLockManager
from i_util.tools import crawler_basic_path, remove_pid_file

from common.log import log
from single_src_merger_worker import SMWorkerPool
from common.singleton_holder import singletons
from common.interruptable_thrift_server import InterruptableThreadPoolServer
from i_util.tools import make_pid_file


class SMThriftHandler(object):
    def __init__(self, conf, worker_pool):
        self.conf = conf
        self.log = log
        self.worker_pool = worker_pool

    def reload(self, topic_id=-1):
        '''重新加载topic schema和解析器'''
        data = {}
        try:
            self.log.info("start reload topic_id[%s]" % topic_id)
            data = singletons[SingleSourceMerger.__name__].reload(topic_id)
            self.worker_pool.reload_all(topic_id)
            self.log.info("finish reload topic_id[%s]" % topic_id)
            msg  = "finish reload topic_id[%s]" % topic_id
        except:
            self.log.error("reload fail reason[%s]" % traceback.format_exc())
            msg = "reload fail reason[%s]" % traceback.format_exc()

        resp = ResultResp(code=1, msg=msg, data=json.dumps(data))
        return resp


running = True

def exit_isr(a, b):
    global running
    pool = singletons[SMWorkerPool.__name__]
    log.debug("main proc : waiting for sub processes to die...")
    res = pool.stop_all()
    if res:
        log.debug("main proc : all sub processes gracefully stopped")
        running = False
    else:
        log.debug("main proc : thread pool is not in running state")

    # sys.exit(0)

# sys.argv.extend('-f server1.toml'.split())

def main(conf):
    global running
    signal.signal(signal.SIGTERM, exit_isr)
    signal.signal(signal.SIGINT, exit_isr)
    pid_file = make_pid_file(sys.path[0])
    try:

        # This is only used by thrift, each process has its own EntityExtractor instance
        singletons[SingleSourceMerger.__name__] = SingleSourceMerger(conf)
        singletons[SMWorkerPool.__name__] = SMWorkerPool(init_count=conf['server']['server_process_num'],
                                                         conf=conf)
        singletons[RecordLockManager.__name__] = RecordLockManager(conf, prefix='single-src',
                                                                   backlog_path=os.path.join(crawler_basic_path, 'ss_backlog'))

        worker_pool = singletons[SMWorkerPool.__name__]
        worker_pool.start_all()
        conf['log'] = log
        heartbeat_thread = HeartbeatThread("single_src_merge", conf)
        heartbeat_thread.start()
        try:
            handler = SMThriftHandler(conf, worker_pool)
            processor = SingleSourceMergeService.Processor(handler)
            transport = TSocket.TServerSocket(port=conf['server']['port'])
            tfactory  = TTransport.TBufferedTransportFactory()
            pfactory  = TBinaryProtocol.TBinaryProtocolFactory()
            server    = InterruptableThreadPoolServer(processor, transport, tfactory, pfactory)
            server.setNumThreads(1)
            server.serve()
        except Exception, e:
            log.error(str(e))
            log.error(traceback.format_exc())
        if running:
            worker_pool.stop_all()
    finally:
        remove_pid_file(sys.path[0])




def usaget():
    pass


# sys.argv.extend("-f server1.toml".split())


if __name__ == '__main__':
    try:
        file_path = './single_src_merge.toml'
        opt, args = getopt.getopt(sys.argv[1:], 'f:', ['help'])
        for name, value in opt:
            if name == "-f":
                file_path = value
            elif name in ("-h", "--help"):
                usaget()
                sys.exit()
            else:
                assert False, "unhandled option"
        with open(file_path, 'rb') as config:
            conf = pytoml.load(config)
        log.init_log(conf, console_out=conf['logger']['console'], name="sm_main_proc")
        main(conf)
    except:
        print traceback.format_exc()
