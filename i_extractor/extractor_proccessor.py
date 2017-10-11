#!/usr/bin/Python
# coding=utf-8
import sys

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer

sys.path.append('..')
from bdp.i_crawler.i_downloader.ttypes import DownLoadRsp
from i_util.normal_proccessor import NormalProccessor
from extractor import Extractor
from extractor_statistic import ExtractorStatistics
class ExtractorProccessor(NormalProccessor):
    def __init__(self, conf):
        self.log = conf['log']
        self.extractor = Extractor(conf)
        self.task_collector = ExtractorStatistics(conf['task_collect_db'])
        self.task_collector.start()

    def to_string(self, pageparse_info):
        str_parse = None
        try:
            tMemory_b = TMemoryBuffer()
            tBinaryProtocol_b = TBinaryProtocol(tMemory_b)  
            pageparse_info.write(tBinaryProtocol_b)
            str_parse = tMemory_b.getvalue()
            self.log.info('data-length is {}'.format(str(len(str_parse))))
        except EOFError, e:
            self.log.warning("cann't write PageParseInfo to string")
        return str_parse

    def stop(self):
        self.task_collector.stop()

    def _build_task_record(self, parserpage):
        record = {}
        record['site'] = parserpage.base_info.site
        record['ex_status'] = parserpage.extract_info.ex_status
        record['topic_id'] = parserpage.extract_info.topic_id
        record['crawl_status'] = parserpage.crawl_info.status_code
        record['url'] = parserpage.base_info.url
        return record

    def do_task(self, body):
        download_rsp = DownLoadRsp()
        try:
            tMemory_o = TMemoryBuffer(body)  
            tBinaryProtocol_o = TBinaryProtocol(tMemory_o)  
            download_rsp.read(tBinaryProtocol_o)
            parserpage = self.extractor.extract(download_rsp)
            self.task_collector.task_stats(parserpage)
            self.log.debug(parserpage)
            return self.to_string(parserpage), parserpage.extract_info.topic_id
        except EOFError, e:
            self.log.warning("cann't read DownLoadRsp from string")
            return None
        #return download_rsp
    def do_output(self,body):
        return True;

def test(conf):
    proccessor = ExtractorProccessor(conf.log, 1)
    print proccessor.do_task("test");

if __name__ == '__main__':
    import conf
    test(conf)
