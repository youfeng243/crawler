# coding=utf-8
# 审判流程实体解析

import json
import sys
import re

sys.path.append("..")
sys.path.append("../../")
sys.path.append("../../../")

from crawler.i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from crawler.i_entity_extractor.common_parser_lib import toolsutil
import judge_process_conf
import copy


class JudgeProcessExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.litigant_seps = ['\r','\n','。','，','|',' ',';',',','、']
        self.plaintiff_regex_list = []
        for plaintiff_keyword in judge_process_conf.plaintiff_keyword_list:
            if not plaintiff_keyword:
                continue
            plaintiff_pattern = plaintiff_keyword + "[:：]?" + "(\S+)"
            self.plaintiff_regex_list.append(re.compile(unicode(plaintiff_pattern)))

        self.defendant_regex_list = []
        for defendant_keyword in judge_process_conf.defendant_keyword_list:
            if not defendant_keyword:
                continue
            defendant_pattern = defendant_keyword + "[:：]?" + "(\S+)"
            self.defendant_regex_list.append(re.compile(unicode(defendant_pattern)))

        self.config_path  = self.basic_path + "i_entity_extractor/extractors/judge_process/mapping.conf"
        self.mapping_conf = self.read_config(self.config_path)

    def split_person(self, persons_str):
        person_list = []
        pars = re.split(';|, ', persons_str.strip())
        for par in pars:
            if len(par.strip()) > 0:
                person_list.append(par.strip())
        return person_list

    def read_config(self,conf_file):
        mapping_conf = {}
        file = open(conf_file)
        for line in file:
            pars = line.strip().split(',')
            if len(pars) >= 2:
                key = pars[0].encode("utf8")
                value = pars[1].encode("utf8")
                mapping_conf[key] = value
        return mapping_conf

    def format_extract_data(self, extract_data, topic_id):
        '''格式化数据'''
        entity_data = copy.deepcopy(extract_data)
        court = extract_data.get("court")
        province = extract_data.get("province")
        if not province:
            province = self.parser_tool.province_parser.get_province(court)
            entity_data["province"] = province

        extra_data = extract_data.get("extra_data", [])
        if extra_data:
            defendant_list = []
            plaintiff_list = []
            for extra in extra_data:
                identity_type = extra.get("identity_type", "")
                name = extra.get("name", "")
                if unicode(identity_type) in judge_process_conf.defendant_keyword_list:
                    defendant_list.append(name)
                elif unicode(identity_type) in judge_process_conf.plaintiff_keyword_list:
                    plaintiff_list.append(name)
                else:
                    pass
            litigant_list = list(set(defendant_list + plaintiff_list))
            litigants = ','.join(litigant_list)

            entity_data["defendant_list"] = defendant_list
            entity_data["plaintiff_list"] = plaintiff_list
            entity_data["litigant_list"] = litigant_list
            entity_data["litigants"] = litigants
        else:
            litigants = extract_data.get("litigant_list","")
            litigants = unicode(litigants)
            defendant_ret = ""
            entity_data["defendant_list"] = []
            entity_data["plaintiff_list"] = []
            for defendant_regex in self.defendant_regex_list:
                defendant_ret = toolsutil.re_findone(defendant_regex,litigants)
                if defendant_ret:
                    entity_data["defendant_list"] = toolsutil.my_split(defendant_ret,self.litigant_seps)
                    break

            for plaintiff_regex in self.plaintiff_regex_list:
                ret_list = toolsutil.re_findall(plaintiff_regex,litigants)
                if ret_list:
                    for ret in ret_list:
                        if defendant_ret and defendant_ret in ret:
                            continue
                        entity_data["plaintiff_list"] = toolsutil.my_split(ret,self.litigant_seps)
                    break

            if len(entity_data["defendant_list"]) > 0 or len(entity_data["plaintiff_list"]) > 0:
                entity_data["litigant_list"] = entity_data["defendant_list"] + entity_data["plaintiff_list"]
            else:
                entity_data["plaintiff_list"] = extract_data.get("plaintiff_list", "")
                entity_data["defendant_list"] = extract_data.get("defendant_list", "")
                entity_data["litigant_list"]  = extract_data.get("litigant_list", "")
                if extract_data.has_key("info"):
                    info = extract_data.get("info")
                    for item in info:
                        for key, value in item.items():
                            base_key = key[:3]
                            base_value = "value" + key[3:]
                            if base_key == "key" and item.has_key(base_value):

                                key = item[key].encode("utf8")
                                value = item[base_value].encode("utf8")
                                key_values = [(key, value)]
                                key_pars = key.split("\t")
                                value_pars = value.split("\t")
                                if len(key_pars) > 1 and len(key_pars) == len(value_pars):
                                    index = 0
                                    while index < len(key_pars):
                                        key_values.append((key_pars[index], value_pars[index]))
                                        index += 1
                        for key,value in key_values:
                            for key_conf,value_conf in self.mapping_conf.items():
                                if key_conf in key:
                                    entity_data[value_conf] = value
                                    break

                if isinstance(entity_data["defendant_list"], basestring):
                    entity_data["defendant_list"] = toolsutil.my_split(entity_data.get("defendant_list",""),self.litigant_seps)
                if isinstance(entity_data["plaintiff_list"], basestring):
                    entity_data["plaintiff_list"] = toolsutil.my_split(entity_data.get("plaintiff_list",""),self.litigant_seps)
                if isinstance(entity_data["litigant_list"], basestring):
                    entity_data["litigant_list"]  = toolsutil.my_split(entity_data.get("litigant_list",""),self.litigant_seps)

            if not entity_data.get("litigant_list"):
                entity_data["litigant_list"] = entity_data["plaintiff_list"] + entity_data["defendant_list"]
            entity_data["litigants"]      = ','.join(entity_data["litigant_list"])

        return entity_data


if __name__ == '__main__':
    import pytoml
    import sys
    import time
    from common.log import log

    sys.path.append('../../')
    from i_entity_extractor.conf import get_config
    from bdp.i_crawler.i_extractor.ttypes import BaseInfo, CrawlInfo, ExtractInfo, PageParseInfo

    with open('../../entity.toml', 'rb') as config:
        conf = pytoml.load(config)
    log.init_log(conf, console_out=conf['logger']['console'])

    topic_id = 37
    from i_entity_extractor.entity_extractor_route import EntityExtractorRoute

    route = EntityExtractorRoute(conf)
    topic_info = route.all_topics.get(topic_id, None)
    begin_time = time.time()
    obj = JudgeProcessExtractor(topic_info, log)


    extract_data = {
    "_in_time": "2017-04-14 15:06:35.109133",
    "_site_record_id": "865f7ff8efceb8b786a5f034f7a98d23",
    "_src": [
        {
            "site": "12368.szcourt.gov.cn",
            "site_id": 4431956319340339700,
            "url": "http://12368.szcourt.gov.cn/frontend/anjiangongkai/caseOpen/23768665500246698?check="
        }
    ],
    "_utime": "2017-04-14 15:06:35.109133",
    "court": "深圳法院",
    "yinfo": [
        {
            "key": "案号：",
            "value": "(2016)粤0391民初1987号"
        },
        {
            "key": "承办法官：",
            "value": "胡劭"
        },
        {
            "key": "法官助理：",
            "value": "张晓莉"
        },
        {
            "key": "被告 ：",
            "value": "ZHAOHUI ZENG  深圳市汉威视讯技术有限公司  钟玲"
        },
        {
            "key": "原告 ：",
            "value": "深圳先德华锐投资企业（有限合伙）  深圳招科创新投资基金合伙企业（有限公司）  郑颖"
        },
        {
            "key": "立案时间：",
            "value": "2016-10-19"
        },
        {
            "key": "开庭时间：",
            "value": "2017-06-30"
        },
        {
            "key": "案件状态：",
            "value": "审理"
        }
    ],
    "defendant_list":["fds","fdsfd"],
    "plaintiff_list":"a",
    "province": "广东"
}
    entity_data = obj.format_extract_data(extract_data,topic_id)
    entity_data = obj.after_process(entity_data)
    print "-----------------------------"
    for key, value in entity_data.items():
        if isinstance(value, list):
            for i in value:
                print key, ":", i
        elif isinstance(value, dict):
            for key2, value2 in value.items():
                print key2, ":", value2
        else:
            print key, ":", value

    print "time_cost:", time.time() - begin_time
