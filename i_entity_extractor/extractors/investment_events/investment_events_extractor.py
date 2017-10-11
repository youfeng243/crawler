# coding=utf8
import sys
import re
import time
sys.path.append('../../')
from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib import etl_tool
import copy


class InvestmentEventsExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)

    def format_extract_data(self, extract_data, topic_id):
        """实体解析抽取数据"""
        item = copy.deepcopy(extract_data)

        # 处理投资时间 invest_date
        invest_date = item.get('invest_date', '')
        item['invest_date'] = etl_tool.regex_remove_time(invest_date).strip()

        # 处理地区

        lst_location = etl_tool.regex_chinese(item.get('region', ''))
        map_region = etl_tool.province_city_district(lst_location)
        for region_key in map_region.keys():
            item[region_key] = map_region[region_key]
        if u'市' in item.get('region', ''):
            item['city'] = item.get('region', '')

        # 投资金额
        amount = item.get('amount', '')
        res_amount = self.parser_tool.money_parser.new_trans_money(amount, u"万")
        item['amount'] = res_amount[0]
        item['units'] = res_amount[1]
        item['currency'] = res_amount[2]

        # setup_date = item.get(u'setup_date', u'')
        # setup_date = etl_tool.str2datetime(setup_date, '%Y-%m-%d %H:%M:%S')
        # item["setup_date"] = setup_date if setup_date == None else setup_date.strftime("%Y-%m-%d")
        return item



if __name__ == '__main__':
    import pytoml
    import sys

    sys.path.append('../../')
    from conf import get_config
    with open('../../entity.toml', 'rb') as config:
        config = pytoml.load(config)
    conf = get_config(config)
    import common

    topic_id = 32
    from entity_extractor_route import EntityExtractorRoute

    route = EntityExtractorRoute()
    topic_info = route.all_topics.get(topic_id, None)

    obj = InvestmentEventsExtractor(topic_info, common.log)
    extract_data = {
        "profile" : "2016年10月13日，上海复星创富投资管理有限公司、联创策源投资咨询（北京）有限公司和浅石创投联合投资北京诸葛找房信息技术有限公司3,000.00万元人民币。",
    "pull_full_name" : "北京诸葛找房信息技术有限公司",
    "industry_class_csrc" : "",
    "source_site" : "私募通",
    "title" : "复星资本、策源创投和浅石创投投资诸葛找房",
    "region" : "广州",
    "pull_industry" : "",
    "pushs" : [
        {
            "push_vcpe_type" : "",
            "push_full_name" : "",
            "push_type" : "天使投资",
            "push_short_name" : "策源创投"
        },
        {
            "push_vcpe_type" : "",
            "push_full_name" : "",
            "push_type" : "天使投资",
            "push_short_name" : "浅石创投"
        },
        {
            "push_vcpe_type" : "",
            "push_full_name" : "",
            "push_type" : "成长资本",
            "push_short_name" : "复星资本"
        }
    ],
    "industry_class_gb" : "",
    "amount" : "RMB 30.00 M",
    "pull_short_name" : "",
    "invest_date" : "2016-10-13 00:00:00",
    "_record_id" : "e5d66c4f36e3557395bb04c647d29d66",
    "pull_headquarters" : "",
    "stage" : "种子期",
    "round" : "Pre-A",
    "industry_class_qk" : ""
    }
    entity_data = obj.format_extract_data(extract_data)

    print "-----------------------------"
    for key, value in entity_data.items():
        if isinstance(value, list):
            for v in value:
                print key, ":", v
        elif isinstance(value, dict):
            for key2, value2 in value.items():
                print key2, ":", value2
        else:
            print key, ":", value

    keys = ['province', 'pushs', 'source_site', 'currency', 'pull_full_name', 'industry_class_csrc', 'district', 'title', 'industry_class_gb', 'pull_short_name', 'units', 'invest_date', 'profile', 'city', 'pull_headquarters', 'stage', 'region', 'amount', 'industry_class_qk', 'round', 'pull_industry', '_in_time', '_src', '_record_id', '_id']
    transfer_data(keys, 'investment_events')
