#!/usr/bin/Python
# coding=utf-8
import json
import os
import signal
import sys
sys.path.append('..')
import traceback

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from bdp.i_crawler.i_data_saver import DataSaverService
from common.interruptable_thrift_server import InterruptableThreadPoolServer
from common.singleton_holder import singletons
from common.log import log
from i_data_saver.data_saver import DataSaver
from i_data_saver.data_saver_worker import DatasaverWorkerPool
from i_util.tools import make_pid_file, remove_pid_file

import getopt
import pytoml
from i_util.heart_beat import HeartbeatThread
# beanstalk服务的入口

# thrift调用的入口
class DataSaverHandler(object):
    def __init__(self):
        self.data_saver = singletons[DataSaver.__name__]

    def check_data(self, entity):
        data = json.loads(entity)
        return self.data_saver.check_data(data, save=False)

    def get_schema(self, topic_id):
        return self.data_saver.get_schema(topic_id)

    def reload(self,topic_id=-1):
        # TODO: topic_manager.reload, 各个validator.reload, 各个merger.reload
        data={}
        try:
            data= self.data_saver.reload(topic_id)
            singletons[DatasaverWorkerPool.__name__].reload_all(topic_id)
        except Exception as e:
            log.warning(traceback)
        return data
#server running flag
running = False

def exit_isr(a, b):
    global running
    pool = singletons[DatasaverWorkerPool.__name__]
    log.debug("main proc : waiting for sub processes to die...")
    res = pool.stop_all()
    if res:
        running = False
        log.debug("main proc : all sub processes gracefully stopped")
    else:
        log.debug("main proc : thread pool is not yet in running state")


def main(conf):
    global running
    signal.signal(signal.SIGTERM, exit_isr)
    signal.signal(signal.SIGINT, exit_isr)
    try:
        pid_file = make_pid_file(sys.path[0])
        try:
            datasaver = DataSaver(conf)
            singletons[DataSaver.__name__] = datasaver
            worker_pool = DatasaverWorkerPool(init_count=conf['server']['server_process_num'], conf=conf)
            singletons[DatasaverWorkerPool.__name__] = worker_pool
            worker_pool.start_all()
            running = True
            heartbeat_thread = HeartbeatThread("datasaver", conf)
            heartbeat_thread.start()

            handler = DataSaverHandler()
            processor = DataSaverService.Processor(handler)
            transport = TSocket.TServerSocket(port=conf['server']['port'])
            tfactory = TTransport.TBufferedTransportFactory()
            pfactory = TBinaryProtocol.TBinaryProtocolFactory()
            server = InterruptableThreadPoolServer(processor, transport, tfactory, pfactory)
            server.setNumThreads(conf['server']['server_thread_num'])
            log.debug("start main thrift")
            server.serve()
        except Exception, e:
            log.debug(traceback.format_exc())
        if running:
            worker_pool.stop_all()
    finally:
        remove_pid_file(sys.path[0])

def usaget():
    pass
if __name__ == '__main__':
    try:
        file_path='./data_saver.toml'
        opt,args=getopt.getopt(sys.argv[1:],'f:',['help'])
        for name,value in opt:
            if name == "-f":
                file_path=value
            elif name in ("-h", "--help"):
                sys.exit()
            else:
                assert False, "unhandled option"
        config = None
        with open(file_path, 'rb') as config_file:
            config = pytoml.load(config_file)
        log.init_log(conf=config,
                     level=config['logger']['level'],
                     console_out=config['logger']['console'],
                     name=config['server']['name'] + "-" + "main"
                     )
        config['log'] = log
        main(config)
    except getopt.GetoptError:
        print traceback.format_exc()
    except Exception as e:
        print traceback.format_exc()
        os._exit(1)

