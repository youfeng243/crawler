# coding=utf8
import copy
import sys

import datetime

sys.path.append('..')
from bdp.i_crawler.i_entity_extractor.ttypes import EntityExtractorInfo, EntitySource
import common_parser_lib.toolsutil as toolsutil
from common_parser_lib.parser_tool import parser_tool
import json
import traceback
import time
from common.topic_manager import TopicManager
from common.validate_manager import ValidateManager, ServerType
from common.log import log
from entity_extractor_route import EntityExtractorRoute


class EntityExtractor(object):

    validator_used = ['pk', 'required_attr', 'jsonschema']
    all_validators = ['meta', 'pk', 'jsonschema']

    def __init__(self, conf):
        self.conf = conf
        self.log = log
        self.parser_tool = parser_tool
        self.route = EntityExtractorRoute(conf)
        self.topic_manager = TopicManager(conf)
        self.validate_manager = ValidateManager(self.topic_manager, conf, 'all')
        self.count = 0

    def reload(self):
        self.topic_manager.reload(-1)
        return True

    # TODO: need to change extractor route to use the common topic manager, this is temporary code
    def add_topic(self, topic_info):
        resp = self.route.add_topic(topic_info)
        self.topic_manager.reload(-1)
        return resp

    def insert_extractor(self, extractor_info):
        resp = self.route.insert_extractor(extractor_info)
        self.topic_manager.reload(-1)
        return resp


    def process_json(self, j, topic_id):
        """
        消息队列处理
        :param j:  json data
        :param topic_id: topic_id
        :return: [{topic_id:主题ID(int), data:{解析结果(json)}}]
        """
        result_list = []
        try:
            extractor = self.route.get_extractor(topic_id)
            formatted_json = extractor.process_json(j, topic_id)
            if formatted_json is None:
                return None

            def process_single_result(single_result):
                after_process_json = extractor.after_process(single_result)
                if after_process_json is None:
                    return None
                else:
                    result_list.append({"topic_id": topic_id, "data": after_process_json})
            if isinstance(formatted_json, list):
                for entity_data in formatted_json:
                    process_single_result(entity_data)
            else:
                process_single_result(formatted_json)

        except Exception, e:
            self.log.error("extract_error\tmsg:%s" % (traceback.format_exc()))
        return result_list

    def process_extract_result(self, j, topic_id, deploy_after_multi_src=True, parse_info=None, resp=None):
        if not deploy_after_multi_src:
            entity_source = EntitySource(url=parse_info.base_info.url, site_id=parse_info.base_info.site_id,
                                         site=parse_info.base_info.site, download_time=parse_info.crawl_info.download_time)
            entity = json.dumps(j)
            entity_extract_data = EntityExtractorInfo(entity_data=entity, entity_source=entity_source, topic_id=topic_id)
            resp["LIST"].append(entity_extract_data)

    def entity_extractor(self, parse_info):
        """
        :param parse_info:PageParseInfo thrift定义数据
        :return: {"CODE":错误码(int), "MSG":提示信息(string), "LIST":[解析结果列表(EntityInfo)]}
        """
        '''实体解析总入口'''
        # 1 加载入参
        begin_time   = time.time()
        base_info    = parse_info.base_info
        extract_info = parse_info.extract_info
        ex_status    = extract_info.ex_status
        topic_id     = extract_info.topic_id

        # 2 初始化返回结构体
        resp = toolsutil.result()

        # 3 检测异常
        if topic_id and topic_id != -1:
            topic_id = int(topic_id)
        else:
            resp['CODE']   = -10000
            resp['MSG']    = 'topic_id error,topic_id:%s'%topic_id
            self.log.warning('topic_id_error,\ttopic_id:%s \turl:%s'%(topic_id, base_info.url))
            return resp

        extract_data_len = len(extract_info.extract_data) if extract_info.extract_data else 0

        extractor = self.route.get_extractor(topic_id)

        self.log.info("start_entity_extract\turl:%s\textract_data:%s\ttopic_id:%s\tex_status:%s\tparser:%s" % (
            base_info.url, extract_data_len, topic_id, ex_status, extractor.__class__))

        if not extractor:
            resp['CODE'] = -10000
            resp['MSG'] = "extractor is None, topic_id:%s" % topic_id
            self.log.error("extractor is None, topic_id:%s" % topic_id)
        elif ex_status != 2 or extract_data_len == 0:
            resp['CODE'] = -10000
            resp['MSG'] = "extract_status fail or extract_data_len = 0"
        else:
            # 4 实体解析预处理
            extract_data_list = extractor.before_extract(extract_info)
            num_extract_data  = 0

            for extract_data in extract_data_list:
                # 5 实体解析主处理
                if int(topic_id) == 32 and base_info.url.find('wenshu.court.gov.cn/List/ListContent?HZPOST') != -1:
                    doc_id        = extract_data.get("doc_id","")
                    base_info.url = "http://wenshu.court.gov.cn/CreateContentJS/CreateContentJS.aspx?DocID=%s"%doc_id
                num_extract_data += 1
                try:
                    entity_data = extractor.entity_extract(parse_info, extract_data)
                    if isinstance(entity_data, list):
                        for entity in entity_data:
                            j = extractor.after_process(entity)
                            self.process_extract_result(j, topic_id, False, parse_info, resp)

                    else:
                        j = extractor.after_process(entity_data)
                        self.process_extract_result(j, topic_id, False, parse_info, resp)

                    resp["MSG"] += " %s extract_data in extract_data_list parser success" % num_extract_data
                except:
                    self.log.error("extract_error\tmsg:%s" % (traceback.format_exc()))
                    resp["MSG"] += " %s extract_data in extract_data_list error, ret:[%s] " % (
                    num_extract_data, traceback.format_exc())
                    resp["CODE"] = -10000

        if resp["CODE"] == 10000:
            self.log.info("haizhi- url = {} topic_id = {} 实体解析成功!".format(base_info.url, topic_id))
        else:
            self.log.error("haizhi- url = {} topic_id = {} 实体解析失败!".format(base_info.url, topic_id))

        end_time = time.time()
        self.log.info("finish_entity_extract\turl:%s\ttopic_id:%s\ttimecost:%.2f" % (base_info.url, topic_id, (end_time - begin_time) * 1000))
        resp['TOPIC_ID'] = topic_id
        return resp

    def after_entity_extract(self, extractor, base_info, entity_data, extract_data, topic_id):
        '''实体解析后续处理'''
        entity_extract_data = None
        entity_data   = extractor.after_extract(base_info.url, entity_data, extract_data)
        if entity_data:
            entity_source = EntitySource(url=base_info.url, site_id=base_info.site_id, site=base_info.site)
            entity = json.dumps(entity_data)
            entity_extract_data = EntityExtractorInfo(entity_data=entity, entity_source=entity_source, topic_id=topic_id)
        return entity_extract_data

if __name__ == "__main__":
    import sys
    sys.path.append('../')
    sys.path.append('../../')
    sys.path.append('../../../')

    extract_data = {
        "court": "山东省高级人民法院",
    }
    entity_data = {'province': '\xe5\xb1\xb1\xe4\xb8\x9c',
                   'court': '\xe5\xb1\xb1\xe4\xb8\x9c\xe7\x9c\x81\xe9\xab\x98\xe7\xba\xa7\xe4\xba\xba\xe6\xb0\x91\xe6\xb3\x95\xe9\x99\xa2',
                   'Param': '\xe8\xa3\x81\xe5\x88\xa4\xe6\x97\xa5\xe6\x9c\x9f:2016-10-25 TO 2016-10-25,\xe6\xb3\x95\xe9\x99\xa2\xe5\x9c\xb0\xe5\x9f\x9f:\xe5\xb1\xb1\xe4\xb8\x9c\xe7\x9c\x81,\xe4\xb8\xad\xe7\xba\xa7\xe6\xb3\x95\xe9\x99\xa2:\xe5\xb1\xb1\xe4\xb8\x9c\xe7\x9c\x81\xe9\xab\x98\xe7\xba\xa7\xe4\xba\xba\xe6\xb0\x91\xe6\xb3\x95\xe9\x99\xa2'}



