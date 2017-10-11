# coding=utf-8
import random
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer
from Queue import PriorityQueue
import threading, time, redis, traceback, json, sys
from config_loader import SchedulerConfigLoader 
from beanstalkc import BeanstalkcException
from beanstalkc import UnexpectedResponse
from beanstalkc import CommandFailed
from beanstalkc import DeadlineSoon
from beanstalkc import SocketError
from beanstalkc import Connection
sys.path.append('..')
import pytoml
import getopt
from i_util.tools import str_obj
from i_util.i_crawler_services import ThriftDownloader
from i_util.pybeanstalk import PyBeanstalk
from i_util.logs import LogHandler
from bdp.i_crawler.i_downloader.ttypes import DownLoadReq


class CrawlSelector(threading.Thread):

    def __init__(self, log, selector_conf, beanstalk_conf, scheduler=None):
        threading.Thread.__init__(self)
        self.daemon = True
        self.running = False
        self.log = log
        # 下载统计信息

        self.site_static = {}
        self.scheduler = scheduler
        self.download_req_num = 0
        # 下载器配置信息
        # self.downloaders = []
        self.downloader_num = 0
        # self.downloader_conf = downloader_conf

        # for downloader in self.downloader_conf:
        #     try:
        #         self.downloaders.append(ThriftDownloader(downloader['host'], downloader['port']))
        #         self.downloader_num += 1
        #     except Exception, e:
        #         self.log.error('Add_downloader\t' + traceback.format_exc())
        # 选择器配置
        self.selector_conf = selector_conf
        # beanstalk 队列设置
        self.beanstalk_conf = beanstalk_conf
        self.out_beanstalk = PyBeanstalk(beanstalk_conf['host'], beanstalk_conf['port'])
        self.output_tube = beanstalk_conf['output_tube']
        self.wlock = threading.Lock()

    def req_to_string(self, req):
        str_req = ""
        try:
            tMemory_b = TMemoryBuffer()
            tBinaryProtocol_b = TBinaryProtocol(tMemory_b)
            req.write(tBinaryProtocol_b)
            str_req = tMemory_b.getvalue() 
        except:
            self.log.error('crawled_failt\terror:%s' % (traceback.format_exc()))
        return str_req

    def run(self):
        self.running = True
        while self.running:
            reqs = None
            url = None
            try:
                if self.scheduler:
                    reqs = self.scheduler.dispatch()
                if reqs:
                    for req in reqs:
                        req_str = self.req_to_string(req)
                        self.out_beanstalk.put(self.output_tube, req_str)
                        self.log.info('start_crawl\turl:%s\tdownload_type:%s\tsession:%s' % (req.url, req.download_type,
                                                                                             req.session_commit))
                time.sleep(self.selector_conf['select_seed_sleep_time'])
            except SocketError as e:
                time.sleep(30)
                self.log.error('beanstalk\tconnect\tfail\tstart\treconnect')
                try:
                    self.out_beanstalk.reconnect()
                    self.log.error('beanstalk\treconnect\tsuccess')
                except Exception as e:
                    self.log.error('beanstalk\treconnect\tfail')
            except Exception, e:
                self.log.error('crawled_failt\turl:%s\terror:%s' % (url, traceback.format_exc()))

    def stop(self):
        self.running = False


def usage():
    pass


def main(conf):
    selector = CrawlSelector(conf['log'], conf['selector_conf'], conf['beanstalk_conf'], None)
    selector.start()
    while True:
        time.sleep(20)
    selector.stop()

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
            conf['log']=LogHandler(conf['server']['name']+str(conf['server']['port']))

        main(conf)

    except getopt.GetoptError:
        sys.exit()
