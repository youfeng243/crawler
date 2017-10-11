#!/usr/bin/Python
# coding=utf-8
import sys
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer

sys.path.append('..')

from bdp.i_crawler.i_extractor.ttypes import PageParseInfo
from i_util.normal_proccessor import NormalProccessor
from single_src_merger import SingleSourceMerger
from common.log import log

class SingleSrcMergerProccessor(NormalProccessor):
    def __init__(self, conf):
        self.log = log
        self.extract_obj = SingleSourceMerger(conf)

    def to_string(self, link_info):
        str_entity = None
        try:
            tMemory_b = TMemoryBuffer()
            tBinaryProtocol_b = TBinaryProtocol(tMemory_b)
            link_info.write(tBinaryProtocol_b)
            str_entity = tMemory_b.getvalue()
        except:
            self.log.warning("cann't write EntityExtractorInfo to string")
        return str_entity

    def do_task(self, body):
        parse_info = None
        if body is not None:
            parse_info = PageParseInfo()
            try:
                tMemory_o = TMemoryBuffer(body)
                tBinaryProtocol_o = TBinaryProtocol(tMemory_o)
                parse_info.read(tBinaryProtocol_o)
                ret = self.extract_obj.do_single_src_merge(parse_info)
            except EOFError, e:
                self.log.warning("cann't read PageParseInfo from string")
                return None, None
        else:
            return None, None
        if int(ret.get('CODE', -10000)) < 0:
            return None, None
        topic_id = parse_info.extract_info.topic_id
        entity_data_list = ret.get('LIST', [])
        msg_list = []
        # if len(entity_data_list) > 0:
        for entity_data in entity_data_list:
            msg_list.append(entity_data)
        # else:
        #     return None
        self.log.info("Topic_id:%s\tsend_msg_num:%s"%(ret.get('TOPIC_ID'), len(msg_list)))
        return msg_list, topic_id

    def do_output(self, body):
        return True




if __name__ == '__main__':
    pass
