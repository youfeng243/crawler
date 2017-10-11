# coding=utf-8
# 上市公司财报实体解析

import json
import copy

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib import toolsutil


class SsggCaibaoProfitExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.quarter_map = {u"-03-31": u"一季", u"-06-30": u"中期", u"-09-30": u"三季", u"-12-31": u"年度", }
        self.year_mode_list  = {u"2014",u"2015",u"2016"}
        self.month_mode_map  = {u"1-3":u"-03-31", u"1-6":u"-06-30",u"1-9":u"-09-30",u"1-12":u"-12-31",u"一季度":u"-03-31",u"一季":u"-03-31",u"中期":u"-06-30",u"三季度":u"-09-30",u"三季":u"-09-30",u"年度":u"-12-31",}
        self.config_path = self.basic_path + "i_entity_extractor/extractors/ssgs_caibao_profit/mapping.conf"
        self.mapping_conf = self.read_config(self.config_path)
        self.profit_config_path = self.basic_path + "i_entity_extractor/extractors/ssgs_caibao_profit/profit_mapping.conf"
        self.profit_mapping_conf = self.read_config(self.profit_config_path)
        self.money_type_list = [u'美元', u'欧元', u'港元', u'港币']
        self.title_map = {u"108":u"利润表",u"110":u"资产负债表",u"111":u"现金流量表",u"112":u"公司综合能力指标"}


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
        if not extract_data.get('year_month'):
            return {}

        topic_id = str(topic_id)
        if topic_id == "108":
            entity_data = self.profit_format_extract_data(extract_data)
        else:
            extract_data["table"] = self.title_map.get(topic_id,"")
            entity_data           = self.format_extract_data_inner(extract_data)

        return entity_data

    def profit_format_extract_data(self, extract_data):
        '''利润表解析抽取数据'''
        month = year = publish_time = code = ''
        entity_data = copy.deepcopy(extract_data)

        if entity_data.has_key('year_month'):
            year_month = entity_data.get('year_month', '')
            for year_mode in self.year_mode_list:
                if year_mode in year_month:
                    year = year_mode
                    break

            for key, value in self.month_mode_map.items():
                if key in year_month:
                    month = value
                    break

            for key in self.quarter_map.keys():
                if key in year_month:
                    month = key
                    break

        if month and year:
            publish_time = year + month

        if entity_data.has_key('code'):
            code = toolsutil.re_find_one('\d+', entity_data.get('code', ''))
            if code:
                entity_data["code"] = code

        if entity_data.has_key('money_unit'):
            money_unit = entity_data.get("money_unit", "")
            found = False
            for money_type in self.money_type_list:
                if money_type in unicode(money_unit):
                    entity_data["money_unit"] = money_type
                    found = True
                    break
            if not found:
                entity_data["money_unit"] = u'元'

        caibao_type = self.quarter_map.get(month,"")
        title = publish_time + caibao_type + u'利润表'

        data_info = {}
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
                            key = key.strip().replace(" ", "")
                            if toolsutil.re_find_one(u'\d+\.\d+', key):
                                continue
                            data_info[key] = value
                            for key_conf, value_conf in self.profit_mapping_conf.items():
                                if key_conf in key:
                                    entity_data[value_conf] = value
                                    break

        entity_data["data_info"] = data_info
        entity_data["title"] = title
        entity_data["caibao_type"] = caibao_type
        entity_data["publish_time"] = publish_time

        return entity_data

    def format_extract_data_inner(self, extract_data):
        '''实体解析抽取数据'''
        month = year = publish_time = code = ''
        entity_data = copy.deepcopy(extract_data)

        if entity_data.has_key('year_month'):
            year_month = entity_data.get('year_month', '')
            for year_mode in self.year_mode_list:
                if year_mode in year_month:
                    year = year_mode
                    break

            for key,value in self.month_mode_map.items():
                if key in year_month:
                    month = value
                    break

            for key in self.quarter_map.keys():
                if key in year_month:
                    month = key
                    break


        if month and year:
            publish_time = year + month

        if entity_data.has_key('code'):
            code = toolsutil.re_find_one('\d+', entity_data.get('code', ''))
            if code:
                entity_data["code"] = code

        if entity_data.has_key('money_unit'):
            money_unit = entity_data.get("money_unit", "")
            found = False
            for money_type in self.money_type_list:
                if money_type in unicode(money_unit):
                    entity_data["money_unit"] = money_type
                    found = True
                    break
            if not found:
                entity_data["money_unit"] = u'元'

        caibao_type = self.quarter_map.get(month,"")
        title = publish_time + caibao_type + extract_data.get("table","")

        data_info = {}
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
                            key = key.strip().replace(" ","")
                            if toolsutil.re_find_one(u'\d+\.\d+',key):
                                continue
                            data_info[key] = value
                            for key_conf, value_conf in self.mapping_conf.items():
                                if key_conf in key:
                                    entity_data[value_conf] = value
                                    break

        entity_data["data_info"] = data_info
        entity_data["title"] = title
        entity_data["caibao_type"] = caibao_type
        entity_data["publish_time"] = publish_time

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

    topic_id = 112
    from entity_extractor_route import EntityExtractorRoute

    route = EntityExtractorRoute()
    topic_info = route.all_topics.get(topic_id, None)
    obj = SsggCaibaoProfitExtractor(topic_info, common.log)
    src_url = ""
    extract_data = {
    "code": "股票代码：000062 股票简称：深圳华强",
    "info": [
        {
            "key": "项目 / 报告期",
            "value": "2016三季"
        },
        {
            "key": "项目 / 报告期",
            "value": "2016三季"
        },
        {
            "key": "基本每股收益(元)",
            "value": "0.373"
        },
        {
            "key": "净利润率(%)",
            "value": "7.04"
        },
        {
            "key": "每股净资产(元)",
            "value": "5.277"
        },
        {
            "key": "总资产报酬率(%)",
            "value": "4.27"
        },
        {
            "key": "净资产收益率—加权平均(%)",
            "value": "7.25"
        },
        {
            "key": "存货周转率",
            "value": "3.80"
        },
        {
            "key": "扣除后每股收益(元)",
            "value": "0.36"
        },
        {
            "key": "固定资产周转率",
            "value": "10.71"
        },
        {
            "key": "流动比率(倍)",
            "value": "1.55"
        },
        {
            "key": "总资产周转率",
            "value": "0.61"
        },
        {
            "key": "速动比率 (倍)",
            "value": "1.14"
        },
        {
            "key": "净资产比率(%)",
            "value": "59.23"
        },
        {
            "key": "应收帐款周转率(次)",
            "value": "4.61"
        },
        {
            "key": "固定资产比率(%)",
            "value": "5.20"
        },
        {
            "key": "资产负债比率(%)",
            "value": "39.54"
        },
        {
            "key": "39.54",
            "value": ""
        }
    ],
    "money_unit": "公司综合能力指标 (单位：人民币元)",
    "year_month": "2016三季"
}
    data = json.dumps(extract_data)
    extract_info = ExtractInfo(ex_status=2, extract_data=data,topic_id=112)
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
