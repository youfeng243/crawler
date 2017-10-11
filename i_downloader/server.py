#!/usr/bin/Python
# -*- coding: utf-8 -*-
import signal
import sys
import os
sys.path.append('..')
from common.interruptable_thrift_server import InterruptableThreadPoolServer

from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from bdp.i_crawler.i_downloader import DownloadService
import pytoml
import getopt
from i_util.heart_beat import HeartbeatThread
from i_util.tools import make_pid_file, remove_pid_file
from downloader_worker import DownloaderWorkerPool
from common.singleton_holder import singletons
from downloader_handle import DownloadHandler
from common.log import log

#server running flag
running = False

def exit_isr(a, b):
    global running
    pool = singletons[DownloaderWorkerPool.__name__]
    log.debug("main proc : waiting for sub processes to die...")
    pool.running = False
    pool.stop_all()
    log.debug("main proc : all sub processes gracefully stopped")

def main(conf):
    global running
    signal.signal(signal.SIGTERM, exit_isr)
    signal.signal(signal.SIGINT, exit_isr)
    handler = None
    try:
        make_pid_file(sys.path[0])
        try:
            handler = DownloadHandler(conf)
            #多进程启动
            worker_pool = DownloaderWorkerPool(init_count=conf['server']['server_process_num'], conf=conf)
            singletons[DownloaderWorkerPool.__name__] = worker_pool
            worker_pool.start_all()
            running = True
            #心跳开始
            heartbeat_thread = HeartbeatThread("downloader", conf)
            heartbeat_thread.start()
            #thrift接口
            processor = DownloadService.Processor(handler)
            transport = TSocket.TServerSocket(port=conf['server'].get('port'))
            tfactory = TTransport.TBufferedTransportFactory()
            pfactory = TBinaryProtocol.TBinaryProtocolFactory()
            server    = InterruptableThreadPoolServer(processor, transport, tfactory, pfactory)
            server.setNumThreads(conf['server'].get('server_thread_num'))
            server.serve()
        except Exception as e:
            import traceback
            conf.get('log').error(e.message)
        if running:
            worker_pool.stop_all()
        if handler:
            # 必须在server阻塞完成之后调用hander的stop方法去回收phantomjs
            handler.stop()
    except:
        pass
    finally:
        remove_pid_file(sys.path[0])
    os.kill(os.getpid(), signal.SIGKILL)
def usaget():
    pass
if __name__ == '__main__':
    try:
        file_path='./downloader.toml'
        opt,args=getopt.getopt(sys.argv[1:],'f:',['help'])
        for name,value in opt:
            if name == "-f":
                file_path=value
            elif name in ("-h", "--help"):
                usaget()
                sys.exit()
            else:
                assert False, "unhandled option"
        with open(file_path, 'rb') as config:
            config = pytoml.load(config)
        log.init_log(conf=config,
                     level=config['logger']['level'],
                     console_out=config['logger']['console'],
                     name=config['server']['name'] + "-" + "main"
                     )
        config['log'] = log
        main(config)
    except getopt.GetoptError:
        sys.exit()

