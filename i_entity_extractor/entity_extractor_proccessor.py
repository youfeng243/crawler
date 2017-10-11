#!/usr/bin/Python
# coding=utf-8
import sys
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer

sys.path.append('..')

from bdp.i_crawler.i_extractor.ttypes import PageParseInfo
from bdp.i_crawler.i_entity_extractor.ttypes import EntityExtractorInfo
from i_util.normal_proccessor import NormalProccessor
from entity_extractor import EntityExtractor
from entity_statistic import EntityStatistics
from common.log import log
import json


class DeployMethod(object):
    AFTER_MULTI_SRC = 1
    AFTER_EXTRACTOR = 2

class EntityExtractorProccessor(NormalProccessor):

    def __init__(self, conf):
        self.log = log
        self.extract_obj = EntityExtractor(conf)
        self.task_collector = EntityStatistics(conf['task_collect_db'])
        self.task_collector.start()
        if conf.get("use_old_deploy", False):
            self.deploy_method = DeployMethod.AFTER_EXTRACTOR
        else:
            self.deploy_method = DeployMethod.AFTER_MULTI_SRC

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

    def stop(self):
        self.task_collector.stop()

    def do_task_thrift(self, body):
        parse_info = PageParseInfo()
        try:
            tMemory_o = TMemoryBuffer(body)
            tBinaryProtocol_o = TBinaryProtocol(tMemory_o)
            parse_info.read(tBinaryProtocol_o)
            ret = self.extract_obj.entity_extractor(parse_info)
        except EOFError, e:
            self.log.warning("cann't read PageParseInfo from string")
            return None

        if int(ret.get('CODE', -10000)) < 0:
            return None

        entity_data_list = ret.get('LIST', [])
        msg_list = []
        if len(entity_data_list) > 0:
            for entity_data in entity_data_list:
                if isinstance(entity_data, EntityExtractorInfo):
                    msg_list.append(self.to_string(entity_data))

        else:
            return None
        self.log.info("Topic_id:%s\tsend_msg_num:%s" % (ret.get('TOPIC_ID'), len(msg_list)))
        return msg_list

    def do_task_json(self, body):
        try:
            j = json.loads(body)
            topic_id = j['_topic_id']
            del j['_topic_id']
            processed_datas = self.extract_obj.process_json(j, topic_id)
            if not processed_datas:
                self.task_collector.inc_fail_parse(topic_id)
            else:
                self.task_collector.inc_success_parse(topic_id)
            rets = []
            for d in processed_datas:
                rets.append(json.dumps(d))
            return rets
        except EOFError, e:
            self.log.warning("cann't read PageParseInfo from string")
            self.log.error(str(e.message))
            return None
        return None


    def do_task(self, body):
        if self.deploy_method == DeployMethod.AFTER_EXTRACTOR:
            return self.do_task_thrift(body)
        elif self.deploy_method == DeployMethod.AFTER_MULTI_SRC:
            return self.do_task_json(body)
            # return self.do_task_json('{"company":"AAA", "ccc":1, "_topic_id":49, "_record_id":"12341234"}')

    def do_output(self, body):
        return True




if __name__ == '__main__':
    pass
