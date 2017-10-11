# coding=utf8
import sys

sys.path.append('../../')
import json
import copy
import traceback
import time
import re
import os
from i_util import tools
from i_entity_extractor.common_parser_lib import toolsutil
from i_entity_extractor.common_parser_lib.parser_tool import parser_tool
from i_util.tools import crawler_basic_path, lookup_dict_path


class DefaultExtractor(object):
    def __init__(self, topic_info, log):
        self.log = log
        self.topic_id = topic_info['topic_id']
        self.schema = topic_info['schema']
        self.primary_keys = topic_info['primary_keys']
        self.table_name = topic_info['table_name']
        self.parser_tool = parser_tool
        self.schema_keys = self.schema.get("properties", {}).keys()
        self.basic_path = crawler_basic_path

    def before_extract(self, extract_info):
        extract_data_list = []
        root_node = json.loads(extract_info.extract_data)

        if root_node and root_node.has_key("datas"):
            if isinstance(root_node["datas"], list):
                for data in root_node["datas"]:
                    for key,value in root_node.items():
                        if key != "datas" and (not data.has_key(key)):
                            data[key] = value
                    extract_data_list.append(data)
            else:
                self.log.info("datas_error, extract_data:%s" % root_node)
                return extract_data_list

        if len(extract_data_list) == 0:
            extract_data_list.append(root_node)
        return extract_data_list

    def entity_extract(self, parse_info, extract_data):
        extract_info = parse_info.extract_info
        url = parse_info.base_info.url
        self.log.info("%s begin process \turl:%s\ttopic_id:%s" % (type(self).__name__, url, extract_info.topic_id))
        entity_data = self.format_extract_data(extract_data, extract_info.topic_id)
        self.log.info("%s end process \turl:%s\ttopic_id:%s" % (type(self).__name__, url, extract_info.topic_id))
        return entity_data

    def process_json(self, j, topic_id):
        self.log.info("%s begin process\ttopic_id:%s" % (type(self).__name__, topic_id))
        entity_data = self.format_extract_data(j, topic_id)
        self.log.info("%s end process\ttopic_id:%s" % (type(self).__name__, topic_id))
        return entity_data

    def format_extract_data(self, extract_data, topic_id):
        return extract_data

    def after_process(self, entity_data):
        '''格式化解析数据'''
        begin_time = time.time()
        if self.schema:
            properties = self.schema.get("properties", {})
            for key, value in properties.items():
                if key not in entity_data:
                    continue
                type = value.get('type', '')
                data_value = entity_data.get(key)

                if data_value == None:
                    del entity_data[key]
                elif type == 'number':
                    try:
                        data_value = str(data_value).replace(',', '').replace('，', '')
                        entity_data[key] = round(float(data_value), 2)
                    except:
                        entity_data[key] = 0.0
                elif type == 'array':
                    if isinstance(data_value, basestring):
                        # pars = data_value.split(';')
                        pars = re.split(';|,', data_value)
                        entity_data[key] = []
                        for par in pars:
                            if len(par.strip()) > 0:
                                entity_data[key].append(par.strip())
                if ('_date' in key or '_time' in key) and data_value != None:
                    try:
                        if len(data_value) > 10:
                            tmp_data_value = int(data_value[:-3])
                            data_value = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tmp_data_value))
                    except:
                        pass

                    if '_date' in key:
                        entity_data[key] = toolsutil.norm_date(self.parser_tool.date_parser.get_date_list(data_value))
                    elif '_time' in key:
                        entity_data[key] = toolsutil.norm_date_time(self.parser_tool.date_parser.get_date_list(data_value))

        else:
            self.log.error("read schema error,schema:%s" % self.schema)
            return None

        result_data = copy.deepcopy(entity_data)
        for key, value in result_data.items():
            if isinstance(value, basestring):
                result_data[key] = value.strip()

        cost_time = (time.time() - begin_time) * 1000
        self.log.info("topic_id:%s\tformat_data_success\ttime_cost:%.2f" % (self.topic_id, cost_time))

        return result_data


    #
    # def after_extract(self, url, entity_data, extract_data):
    #     '''格式化解析数据'''
    #     begin_time = time.time()
    #     if self.schema:
    #         properties = self.schema.get("properties", {})
    #         for key, value in properties.items():
    #             if key not in entity_data:
    #                 continue
    #             type = value.get('type', '')
    #             data_value = entity_data.get(key)
    #
    #             if data_value == None:
    #                 entity_data[key] = None
    #             elif type == 'number':
    #                 try:
    #                     data_value = str(data_value).replace(',','').replace('，','')
    #                     entity_data[key] = round(float(data_value), 2)
    #                 except:
    #                     entity_data[key] = 0.0
    #             elif type == 'array':
    #                 if isinstance(data_value, basestring):
    #                     # pars = data_value.split(';')
    #                     pars = re.split(';|,', data_value)
    #                     entity_data[key] = []
    #                     for par in pars:
    #                         if len(par.strip()) > 0:
    #                             entity_data[key].append(par.strip())
    #
    #             if '_date' in key or '_time' in key:
    #                 if data_value != None:
    #                     try:
    #                         if len(data_value) > 10:
    #                             tmp_data_value = int(data_value[:-3])
    #                             data_value = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tmp_data_value))
    #                     except:
    #                         pass
    #                     entity_data[key] = toolsutil.norm_date_time(
    #                         self.parser_tool.date_parser.get_date_list(data_value))
    #     else:
    #         self.log.error("read schema error,schema:%s" % self.schema)
    #         return None
    #
    #     result_data = self.norm_entity_data(entity_data, extract_data)
    #
    #     cost_time = (time.time() - begin_time) * 1000
    #     self.log.info("topic_id:%s\tformat_data_success\ttime_cost:%.2f" % (self.topic_id, cost_time))
    #
    #     return result_data

    def norm_entity_data(self, entity_data, extract_data):
        '''格式化实体解析数据'''
        result_data = copy.deepcopy(entity_data)
        # for key, value in entity_data.items():
        #    if value == "" or value == []:
        #        extract_data_value = extract_data.get(key,value)
        #        if type(value) == type(extract_data_value):
        #            result_data[key] = extract_data_value
        #    if value == None:
        #        result_data.pop(key)

        for key, value in result_data.items():
            if isinstance(value, basestring):
                 result_data[key] = value.strip()
        return result_data

    def get_topic_specific_meta(self):
        return None

if __name__ == "__main__":
    sys.path.append('../../')
    topic_id = 123
    from i_entity_extractor.entity_extractor_route import entity_route_obj

    topic_info = entity_route_obj.all_topics.get(topic_id, None)
    import common

    obj = DefaultExtractor(topic_info, common.log)
    extract_data = {}
    entity_data = {}
    result_data = obj.after_extract('', entity_data, extract_data)
    print len(result_data.keys())
    print result_data
