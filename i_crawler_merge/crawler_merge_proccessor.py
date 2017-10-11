# coding:utf-8

import sys

sys.path.append('..')

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer

from bdp.i_crawler.i_extractor.ttypes import PageParseInfo
from i_util.normal_proccessor import NormalProccessor
from convert_processor import ConvertProccessor

class CrawlerMergeProccessor(NormalProccessor):
    def __init__(self, conf):
        self.log = conf['log']
        self.convert = ConvertProccessor(conf)

    def to_string(self, link_info):
        if link_info is None:return None
        str_entity = None
        try:
            tMemory_b = TMemoryBuffer()
            tBinaryProtocol_b = TBinaryProtocol(tMemory_b)
            link_info.write(tBinaryProtocol_b)
            str_entity = tMemory_b.getvalue()
        except EOFError, e:
            self.log.warning("can't write LinkAttr to string")
        return str_entity

    def do_task(self, body):
        page_parseinfo = PageParseInfo()
        try:
            tMemory_o = TMemoryBuffer(body)
            tBinaryProtocol_o = TBinaryProtocol(tMemory_o)
            page_parseinfo.read(tBinaryProtocol_o)
            return self.to_string(self.convert.start_convert(page_parseinfo))
        except EOFError, e:
            self.log.warning("can't read PageParseInfo from string")
            return None

    def do_output(self, body):
        return True
