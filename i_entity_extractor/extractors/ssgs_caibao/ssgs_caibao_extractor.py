# coding=utf-8
# 上市公司财报实体解析

import json
import copy
import re

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib import toolsutil


class SsggCaibaoExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.config_path = self.basic_path + "i_entity_extractor/extractors/ssgs_caibao/mapping.conf"
        self.mapping_conf = self.read_config()
        self.public_sector_regex = re.compile("\D+")
        self.public_sector_dict = {
            "szmb" : u"深市主板",
            "szsme": u"中小企业板",
            "szcn": u"创业板",
            "shmb": u"沪市主板",
            "hkmb": u"香港主板",
            "hkgem": u"香港创业板",
        }


    def read_config(self):
        mapping_conf = {}
        file = open(self.config_path)
        for line in file:
            pars = line.strip().split(',')
            if len(pars) >= 2:
                key = pars[0].encode("utf8")
                value = pars[1].encode("utf8")
                mapping_conf[key] = value
        return mapping_conf

    def format_extract_data(self, extract_data, topic_id):
        '''实体解析抽取数据'''
        entity_data = copy.deepcopy(extract_data)
        if entity_data.has_key("code"):
            code_content = entity_data.get("code")
            ret = toolsutil.re_findone(self.public_sector_regex,code_content)
            if ret:
                entity_data["public_sector"] = self.public_sector_dict.get(ret)

        if entity_data.has_key("info"):
            entity_data.pop("info")

            for item in extract_data["info"]:
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
                        for key, value in key_values:
                            for key_conf, value_conf in self.mapping_conf.items():
                                if key_conf in key:
                                    if unicode(key_conf) == u"注册资本":
                                        src_money = key+value
                                        value,unit = self.parser_tool.money_parser.transfer_money(src_money)
                                    entity_data[value_conf] = value
                                    break


        return entity_data


if __name__ == '__main__':
    import pytoml
    import sys

    sys.path.append('../../')
    from conf import get_config
    from bdp.i_crawler.i_extractor.ttypes import BaseInfo, CrawlInfo, ExtractInfo, PageParseInfo

    with open('../../entity.toml', 'rb') as config:
        config = pytoml.load(config)
    conf = get_config(config)
    import common

    topic_id = 102
    from entity_extractor_route import EntityExtractorRoute

    route = EntityExtractorRoute(conf)
    topic_info = route.all_topics.get(topic_id, None)
    obj = SsggCaibaoExtractor(topic_info, common.log)
    src_url = ""
    extract_data = {
    "code": "szcn300599",
    "info": [
        {
            "key": "公司全称",
            "value": "广东雄塑科技集团股份有限公司"
        },
        {
            "key": "英文名称",
            "value": "Guangdong Xiongsu Technology Group Co., Ltd."
        },
        {
            "key": "注册地址",
            "value": "广东省佛山市南海区九江镇龙高路敦根路段雄塑工业园"
        },
        {
            "key": "公司简称",
            "value": "雄塑科技"
        },
        {
            "key": "法定代表人",
            "value": "黄淦雄"
        },
        {
            "key": "公司董秘",
            "value": "彭晓伟"
        },
        {
            "key": "注册资本(万元)",
            "value": "30,400.0000"
        },
        {
            "key": "行业种类",
            "value": "橡胶和塑料制品业"
        },
        {
            "key": "邮政编码",
            "value": "528203"
        },
        {
            "key": "公司电话",
            "value": "0757-81868066"
        },
        {
            "key": "公司传真",
            "value": "0757-81868318"
        },
        {
            "key": "公司网址",
            "value": "www.xiongsu.cn"
        },
        {
            "key": "上市时间",
            "value": "2017-01-23"
        },
        {
            "key": "招股时间",
            "value": "2017-01-11"
        },
        {
            "key": "发行数量（万股）",
            "value": "7,600"
        },
        {
            "key": "发行价格（元）",
            "value": "7.04"
        },
        {
            "key": "发行市盈率（倍）",
            "value": "22.98"
        },
        {
            "key": "发行方式",
            "value": "网下询价发行,上网定价发行"
        },
        {
            "key": "主承销商",
            "value": "广发证券股份有限公司"
        },
        {
            "key": "上市推荐人",
            "value": ""
        },
        {
            "key": "保荐机构",
            "value": "广发证券股份有限公司"
        },
        {
            "key": "股票代码",
            "value": "300599"
        }
    ]
}
    data = json.dumps(extract_data)
    extract_info = ExtractInfo(ex_status=2, extract_data=data)
    base_info = BaseInfo(url=src_url)
    parser_info = PageParseInfo(base_info=base_info, extract_info=extract_info)
    entity_data = obj.entity_extract(parser_info, extract_data)

    entity_data = obj.after_extract(base_info.url, entity_data, extract_data)

    for key, value in entity_data.items():
        if isinstance(value, list):
            for i in value:
                print key, ":", i
        elif isinstance(value, dict):
            for key2, value2 in value.items():
                print key2, ":", value2
        else:
            print key, ":", value
