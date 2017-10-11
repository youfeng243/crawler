# coding:utf-8

import sys

sys.path.append('..')

from bdp.i_crawler.i_crawler_merge import CrawlerMergeService

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
import traceback


class ThriftCrawlerMerge(object):
    def __init__(self, conf):
        self.conf = conf
        transport = TSocket.TSocket(self.conf.host, self.conf.port)
        self.transport = TTransport.TBufferedTransport(transport)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = CrawlerMergeService.Client(self.protocol)

    def merge(self, page_parseinfo):
        rsp = None
        try:
            self.transport.open()
            rsp = self.client.merge(page_parseinfo)
        except:
            print traceback.format_exc()
        finally:
            self.transport.close()
        return rsp

    def select(self, site, url_format, limit, start=0, extra_filter='{}',tube_name='czj_download_rsp'):
        rsp = None
        try:
            self.transport.open()
            rsp = self.client.select(site, url_format, limit, start, extra_filter,tube_name)
        except:
            print traceback.format_exc()
        finally:
            self.transport.close()
        return rsp

    def select_one(self, url):
        rsp = None
        try:
            self.transport.open()
            rsp = self.client.select_one(url)
        except:
            print traceback.format_exc()
        finally:
            self.transport.close()
        return rsp


if __name__ == '__main__':
    import conf

    # from data import Data
    #
    # # page_parseinfo_list = Data().get_page_parseinfo_list()
    #
    # page_parseinfo_list = []
    # for i in xrange(10):
    #     page_parseinfo_list.append(Data().get_page_parseinfo())
    # for i in xrange(10):
    #     page_parseinfo_list.append(Data().get_page_parseinfo2())
    #
    # for page_parseinfo in page_parseinfo_list:
    #     client = ThriftCrawlerMerge(conf)
    #     rsp = client.merge(page_parseinfo)
    #     print len(rsp.normal_crawl_his),rsp.normal_crawl_his

    # 调用58上的thrift服务,调用58上crawler_merge的beanstalk队列
    # conf.host = '101.201.102.37'
    conf.host = '127.0.0.1'
    # conf.host = '101.201.100.58'

    client = ThriftCrawlerMerge(conf)
    rsp = client.select('caseshare.cn', 'http://www.caseshare.cn/full/\d+.html', 20000000, 0,
                        '{"download_time":{"$gte":"2016-11-02 16:00:00","$lte":"2016-11-02 17:00:00"}}', 'online_download_rsp')
    # print rsp

    # client = ThriftCrawlerMerge(conf)
    #
    # rsp = client.select_one(
    #         'http://www.zbs365.com/zhongbiao/type-p342.html')
    # print rsp
