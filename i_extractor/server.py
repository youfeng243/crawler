#!/usr/bin/Python
# coding=utf-8
import getopt
import os
import signal
import sys
sys.path.append('..')
import traceback

import pytoml
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport
from extractor_worker import ExtractorWorkerPool
from i_util.heart_beat import HeartbeatThread
from bdp.i_crawler.i_extractor import ExtractorService
from extractor import Extractor
from common.interruptable_thrift_server import InterruptableThreadPoolServer
from common.singleton_holder import singletons
from common.log import log
from i_util.tools import make_pid_file, remove_pid_file


class ExtractorHandler():
    def save_parser_config(self, config_json):
        return singletons[Extractor.__name__].save_parser_config(config_json=config_json)

    def reload_parser_config(self, parser_id = "-1"):
        singletons[ExtractorWorkerPool.__name__].reload_all(parser_id)
        return singletons[Extractor.__name__].reload_parser_config(parser_id)

    def extract(self, download_rsp):
        return singletons[Extractor.__name__].extract(download_rsp)

#server running flag
running = False

def exit_isr(a, b):
    global running
    pool = singletons[ExtractorWorkerPool.__name__]
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
            extractor = Extractor(conf)
            singletons[Extractor.__name__] = extractor
            worker_pool = ExtractorWorkerPool(init_count=conf['server']['server_process_num'], conf=conf)
            singletons[ExtractorWorkerPool.__name__] = worker_pool
            worker_pool.start_all()
            running = True
            heartbeat_thread = HeartbeatThread("extractor", conf)
            heartbeat_thread.start()

            handler = ExtractorHandler()
            processor = ExtractorService.Processor(handler)
            transport = TSocket.TServerSocket(port=conf['server']['port'])
            tfactory = TTransport.TBufferedTransportFactory()
            pfactory = TBinaryProtocol.TBinaryProtocolFactory()
            server    = InterruptableThreadPoolServer(processor, transport, tfactory, pfactory)
            server.setNumThreads(conf['server']['server_thread_num'])
            log.debug("start main thrift")
            server.serve()
        except Exception, e:
            log.debug(traceback.format_exc())
        if running:
            worker_pool.stop_all()
    finally:
        remove_pid_file(sys.path[0])

if __name__ == '__main__':
    try:
        file_path='./extractor.toml'
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
