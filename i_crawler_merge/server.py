# coding:utf-8

import sys


sys.path.append('..')
import threading
import signal
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer
from convert_processor import ConvertProccessor
from select_processor import SelectProcessor
from i_util.pybeanstalk import PyBeanstalk
from i_util.heart_beat import HeartbeatThread
from i_util.tools import make_pid_file, remove_pid_file
from bdp.i_crawler.i_crawler_merge import CrawlerMergeService
from bdp.i_crawler.i_downloader.ttypes import RetStatus
import MySQLdb, json, traceback, getopt, pytoml
from common.log import log
from common.singleton_holder import singletons
from common.interruptable_thrift_server import InterruptableThreadPoolServer
from crawler_merge_worker import CrawlerMergeWorkerPool


class CrawlerMergeHandler(object):
    def __init__(self, conf, convert, select_handler):
        self.conf = conf
        self.log = conf['log']
        self.convert = convert
        self.select_handler = select_handler
        self.beanstalk = PyBeanstalk(conf.get('beanstalk_conf').get('host'), conf.get('beanstalk_conf').get('port'))

    def to_string(self, link_info):
        str_entity = None
        try:
            tMemory_b = TMemoryBuffer()
            tBinaryProtocol_b = TBinaryProtocol.TBinaryProtocol(tMemory_b)
            link_info.write(tBinaryProtocol_b)
            str_entity = tMemory_b.getvalue()
        except EOFError, e:
            self.log.warning("can't write LinkAttr to string")
        return str_entity

    def merge(self, page_parseinfo):
        link_attr = self.convert.start_convert(page_parseinfo)
        try:
            if isinstance(self.conf.get('beanstalk_conf').get('output_tube'), list):
                for output_tube in self.conf.get('beanstalk_conf').get('output_tube'):
                    self.beanstalk.put(output_tube, self.to_string(link_attr))
            else:
                self.beanstalk.put(self.conf.get('beanstalk_conf').get('output_tube'), self.to_string(link_attr))
        except Exception, e:
            self.log.error("put msg from:{0}\tresult:{1}".format(self.conf.get('beanstalk_conf').get('output_tube'), str(e)))
        return link_attr

    # 每5条发送数据到队列
    def back_work(self, site, url_format, limit, start, extra_filter, tube_name):
        self.log.info(extra_filter)
        mod = 5
        start += 1
        limit -= 1
        times = limit / mod
        if limit % mod != 0:
            results = self.select_handler.select_webpage(site, url_format, limit % mod, start,extra_filter)
            for result in results:
                self.beanstalk.put(tube_name, self.to_string(result))
        start += limit % mod
        for i in xrange(times):
            results = self.select_handler.select_webpage(site, url_format, mod, start + i * mod,extra_filter)
            for result in results:
                self.beanstalk.put(tube_name, self.to_string(result))
            if len(results) != mod:
                break

    def select(self, site, url_format, limit, start=0, extra_filter='{}',tube_name='download_rsp'):
        ret = RetStatus()
        first_result = self.select_handler.select_webpage(site, url_format, 1, start,extra_filter)
        if not first_result:
            ret.status = 0
            return ret
        else:
            try:
                self.beanstalk.put(tube_name, self.to_string(first_result[0]))
                ret.status = 1
                if limit > 1:
                    # 创建守护进程,完成发送剩下数据
                    back_t = threading.Thread(target=self.back_work, args=(site, url_format, limit, start, extra_filter,tube_name))
                    back_t.setDaemon(True)
                    back_t.start()
                return ret
            except Exception, e:
                log.error("put msg from:{0}\tresult:{1}".format(tube_name, str(e)))
                ret.status = 0
                ret.errormessage = e.message
                return ret

    def select_one(self, url):
        result = self.select_handler.select_webpage_by_url(url)
        return result

#server running flag
running = False

def exit_isr(a, b):
    global running
    pool = singletons[CrawlerMergeWorkerPool.__name__]
    log.debug("main proc : waiting for sub processes to die...")
    pool.running = False
    pool.stop_all()
    log.debug("main proc : all sub processes gracefully stopped")


def main(conf):
    global running
    signal.signal(signal.SIGTERM, exit_isr)
    signal.signal(signal.SIGINT, exit_isr)

    try:
        pid_file = make_pid_file(sys.path[0])
        try:
            converter = ConvertProccessor(conf)
            selecter= SelectProcessor(conf)
            worker_pool = CrawlerMergeWorkerPool(init_count=conf['server']['server_process_num'], conf=conf)
            singletons[CrawlerMergeWorkerPool.__name__] = worker_pool
            worker_pool.start_all()
            running = True
            heartbeat_thread = HeartbeatThread("crawl_merge", conf)
            heartbeat_thread.start()

            handler = CrawlerMergeHandler(conf, converter, selecter)
            processor = CrawlerMergeService.Processor(handler)
            transport = TSocket.TServerSocket(port=conf['server'].get('port'))
            tfactory = TTransport.TBufferedTransportFactory()
            pfactory = TBinaryProtocol.TBinaryProtocolFactory()
            server = InterruptableThreadPoolServer(processor, transport, tfactory, pfactory)
            server.setNumThreads(conf['server']['server_thread_num'])
            server.serve()
        except Exception, e:
            conf.get('log').error(traceback.format_exc())
    except:
        pass
    finally:
        remove_pid_file(sys.path[0])
    if running:
        worker_pool.stop_all()


# 读取server.py的启动配置
def get_conf(file_path='./crawler_merge.toml'):
    try:
        opt, args = getopt.getopt(sys.argv[1:], 'f:', ['help'])
        for name, value in opt:
            if name == "-f":
                file_path = value
            else:
                assert False, "unhandled option"
        with open(file_path, 'rb') as config:
            conf = pytoml.load(config)
        log.init_log(conf=conf,
                     level=conf['logger']['level'],
                     console_out=conf['logger']['console'],
                     name=conf['server']['name']+ "-" + "main"
                     )
        conf['log'] = log
        return conf
    except:
        print traceback.format_exc()
        sys.exit(1)


if __name__ == '__main__':
    conf = get_conf()
    main(conf)
