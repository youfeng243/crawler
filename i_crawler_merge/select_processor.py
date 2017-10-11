# coding:utf-8

import sys

sys.path.append('..')

from mongo import PyMongo
from bdp.i_crawler.i_downloader.ttypes import DownLoadRsp
import traceback
import time
from i_util.tools import get_url_info
from i_util.tools import url_encode
from i_util.logs import LogHandler
from i_util.pybeanstalk import PyBeanstalk
from thrift.protocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer


class SelectProcessor(object):
    def __init__(self, conf):
        self.log = conf['log']
        self.conf = conf
        self.beanstalk_conf = conf['beanstalk_conf']
        try:
            self.mongo_client_web = PyMongo(
                self.conf['webpage_db']['host'],
                self.conf['webpage_db']['port'],
                self.conf['webpage_db']['db'],
                self.conf['webpage_db']['username'],
                self.conf['webpage_db']['password']
            )
            self.beanstalk_client = PyBeanstalk(self.beanstalk_conf['host'], self.beanstalk_conf['port'])
        except:
            self.log.error(traceback.format_exc())

    def get_download_rsp(self, result):
        url = result['url']
        content = result['content'].encode('utf-8')
        content_type = result.get('content_type', 'text/html')
        page_size = len(content)
        return DownLoadRsp(
            url=url, download_time=int(time.time()), status=0, content_type=content_type,
            page_size=page_size, elapsed=100, content=content,
            redirect_url=url, src_type='webpage', http_code=200
        )

    # 通过url_format批量查询,并发送到队列
    def select_webpage(self, site, url_format, limit, start, extra_filter):
        try:
            collection_names = self.mongo_client_web.get_collection_names()
            #i_util中需提供一个函数计算主域
            domain = ""
            for collection_name in collection_names:
                prefix_domain = "." + collection_name
                if site.endswith(collection_name) or site.endswith(prefix_domain):
                    domain = collection_name
                    break
            if domain:
                item_cursor = self.mongo_client_web.select_by_url_format(domain, site , url_format, limit, start, extra_filter)
                return item_cursor
        except:
            self.log.error("select_webpage\tsite:{0}\turl_format\t{1}\terror:{2}".format(site, url_format, traceback.format_exc()))
        self.log.info("select_webpage\tfinish\tsite:{0}\turl_format:{1}".format(site, url_format))
        return None

    def select_webpage_to_mq(self, condition):
        url_format = condition.get('url_format',"")
        site = condition.get('site', "")
        limit = int(condition.get('limit', -1))
        start = int(condition.get('start', 0))
        extra_filter = condition.get('extra_filter', '{}')
        self.log.info("select_webpage_mq\tstart\tsite:{0}\turl_format:{1}".format(site, url_format))
        req_num = 0
        all_num = start
        if site:
            item_cursor = self.select_webpage(site, url_format, limit, start, extra_filter)
            if item_cursor:
                download_time = ""
                for item in item_cursor:
                    download_time = item.get("download_time", "")
                    all_num += 1
                    if item.get('content'):
                        download_rsp = self.get_download_rsp(item)
                        download_str = self.to_string(download_rsp)
                        req_num += 1
                        self.beanstalk_client.put(self.beanstalk_conf['output_tube'], download_str)
                    if all_num % 100 == 1 :
                        #print url_format, all_num, req_num, (all_num % 100 == 1)
                        self.log.info("select_webpage_mq\trunning\tsite:{0}\turl_format:{1}\tall_num:{2}\treq_num:{3}\tdownload_time:{4}".format(site, url_format,all_num, req_num, download_time))
        self.log.info("select_webpage_mq\tfinish\tsite:{0}\turl_format:{1}\treq_num:{2}".format(site, url_format,req_num))

    def select_webpage_to_list(self, condition):
        return None
    # 通过url查询单条数据,并发送到队列
    def select_webpage_by_url(self, url):
        self.log.info("select_webpage_by_url start\turl:{}".format(url))
        url = url_encode(url)
        download_result = DownLoadRsp(
            url=url, download_time=int(time.time()), status=1, content_type='text/html',
            page_size=0, elapsed=100, content=None,
            redirect_url=url, src_type='webpage', http_code=0
        )
        try:
            query_item = {'url': url}
            domain = get_url_info(url).get('domain')
            result = self.mongo_client_web.find_first(domain, query_item)
            if result and (result.get('content')):
                download_result = self.get_download_rsp(result)
        except:
            self.log.error("select_webpage_by_url\turl\t{0}\terror:{1}"
                           .format(url, traceback.format_exc()))
        self.log.info("select_webpage_by_url finish\turl:{}".format(url))
        return download_result
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


def main():
    logger = LogHandler('select_webpage');
    #for result in results:
    #    print processor.to_string(result)
if __name__ == '__main__':
    main()
