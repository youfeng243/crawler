# coding=utf-8
# 失信信息实体解析

import json
import sys

sys.path.append("..")
sys.path.append("../../")

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
import traceback
import copy
from i_entity_extractor.common_parser_lib import toolsutil
import re
import shixin_conf


class ShixinExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.money_regex = re.compile(u'\d+\.\d+万元|\d+万元|\d+\.\d+元|\d+元')
        self.money_regex_chs = re.compile(
            u'[一二三四五六七八九十壹贰叁肆伍陆柒捌玖拾]万\S+元|[一二三四五六七八九十壹贰叁肆伍陆柒捌玖拾]千\S+元|[一二三四五六七八九十壹贰叁肆伍陆柒捌玖拾]百\S+元')
        money_pattern_list = []
        money_pattern_list.append(u'¥\d+\.\d+')
        for keyword in shixin_conf.money_keyword_list:
            pattern = keyword + '\d+\.\d+万'
            money_pattern_list.append(pattern)
            pattern = keyword + '\d+万'
            money_pattern_list.append(pattern)
            pattern = keyword + '\d+\.\d+'
            money_pattern_list.append(pattern)
            pattern = keyword + '\d+'
            money_pattern_list.append(pattern)

        money_patterns = '|'.join(money_pattern_list)

        self.money_regex_last = re.compile(money_patterns)

        self.money_regex3 = re.compile(u'\d+\.\d+|\d+')


    def format_extract_data(self, extract_data, topic_id):
        '''解析数据'''
        entity_data   = copy.deepcopy(extract_data)
        tmp_max_money = extract_data.get("duty", "")
        tmp_max_money = unicode(tmp_max_money.replace(" ", ""))
        tmp_max_money_list = toolsutil.my_split(tmp_max_money, [',', '，', '。'])
        ret_list = toolsutil.re_findall(self.money_regex, tmp_max_money)
        money_list = []
        for row_content in tmp_max_money_list:
            ret_chs = toolsutil.re_findone(self.money_regex_chs, unicode(row_content))
            if ret_chs:
                chs_money = self.parser_tool.money_parser.trans_chs_money(ret_chs)
                money_list.append(float(chs_money[0]))
        if ret_list:
            for ret in ret_list:
                digit_money = self.parser_tool.money_parser.transfer_money(ret)
                money_list.append(float(digit_money[0]))
        if money_list == []:
            ret_list2 = toolsutil.re_findall(self.money_regex_last, tmp_max_money)
            if ret_list2:
                for ret in ret_list2:
                    digit_money = self.parser_tool.money_parser.transfer_money(ret)
                    money_list.append(float(digit_money[0]))
        if money_list == []:
            ret3 = toolsutil.re_findone(self.money_regex3, tmp_max_money)
            if ret3 and ret3 == tmp_max_money:
                money_list.append(float(ret3))
        if money_list != []:
            max_money = max(money_list)
            sum_money = sum(money_list)
        else:
            max_money = 0
            sum_money = 0

        court = entity_data.get("court","")
        province = self.parser_tool.province_parser.get_province(court)

        entity_data["max_money"] = max_money
        entity_data["sum_money"] = sum_money
        entity_data["province"]  = province

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

    topic_id = 38
    from entity_extractor_route import EntityExtractorRoute

    route = EntityExtractorRoute()
    topic_info = route.all_topics.get(topic_id, None)
    obj = ShixinExtractor(topic_info, common.log)
    extract_data = {
    "duty" : "给付申请人二万三千元，一百八十八元，232万元,200元",

    "disrupt_type_name" : "其他有履行能力而拒不履行生效法律文书确定义务",
    "_src" : [
        {
            "url" : "http://shixin.court.gov.cn/findDetai?id=4066570",
            "site_id" : 2039908405575752188,
            "site" : "shixin.court.gov.cn"
        }
    ],
    "_record_id" : "da06907636bd57713fa2bd5368d73f27",
    "court" : "遵义市中级人民法院",
    "i_name" : "仁怀市蓝天生物燃料有限公司",
    "_in_time" : "2016-10-19 11:47:14",
    "reg_date" : "2015-10-21 00:00:00",
    "case_id" : "(2015)遵市法执字第00153号",
    "performance" : "全部未履行",
    "_utime" : "2016-11-08 16:48:25",
    "province" : "贵州",
    "party_type_name" : "581",
    "gist_id" : "（2015）遵市法民初字第212号",
    "gist_unit" : "贵州省遵义中级人民法院",
    "publish_date" : "2016-05-06 00:00:00",
    "business_entity" : "徐家涛",
    "registered_code" : "622467051",
    "unique_id" : "4066570"
}
    src_url = ""
    data = json.dumps(extract_data)
    extract_info = ExtractInfo(ex_status=2, extract_data=data)
    base_info = BaseInfo(url=src_url)
    parser_info = PageParseInfo(base_info=base_info, extract_info=extract_info)
    entity_data = obj.entity_extract(parser_info, extract_data)
    entity_data = obj.after_extract(src_url, entity_data, extract_data)
    print src_url

    print "--------------------------"
    for key, value in entity_data.items():
        if isinstance(value, list):
            for i in value:
                print key, ":", i
        elif isinstance(value, dict):
            for key2, value2 in value.items():
                print key2, ":", value2
        else:
            print key, ":", value
