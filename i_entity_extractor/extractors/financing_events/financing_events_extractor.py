# coding=utf8
import sys
import re
import time
sys.path.append('../../')
sys.path.append('../../../')
from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib import etl_tool

import copy


class FinancingEventsExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)

    def format_extract_data(self, extract_data, topic_id):
        """实体解析抽取数据"""
        item = copy.deepcopy(extract_data)

        proportion_share_transfer = item.get('proportion_share_transfer', '')
        item['proportion_share_transfer'] = etl_tool.regex_remove_na(proportion_share_transfer).strip()

        # 处理行业 行业按级拆分
        industry = item.get('industry', '')
        lst_industry = etl_tool.regex_chinese(industry)
        item['industry'] = '/'.join(lst_industry)

        # 处理省市县的拆分
        lst_location = etl_tool.regex_chinese(item.get('region', ''))
        map_region = etl_tool.map_region(lst_location)
        for region_key in map_region.keys():
            item[region_key] = map_region[region_key]
        item['region'] = ''.join(lst_location)

        # 融资金额
        amount = item.get('amount', '')
        res_amount = self.parser_tool.money_parser.new_trans_money(amount, u"万", False)
        item['amounts'] = res_amount[0]
        item['units'] = res_amount[1]
        item['currency'] = res_amount[2] if res_amount[2] else u'人民币'

        # 发布时间
        public_date = item.get(u'public_date', u'')
        public_date = etl_tool.str2datetime(public_date, '%Y-%m-%d %H:%M:%S')
        item["public_date"] = '' if not public_date else public_date.strftime("%Y-%m-%d")
        return item




if __name__ == '__main__':
    import pytoml
    import sys
    import time
    from common.log import log

    sys.path.append('../../')

    with open('../../entity.toml', 'rb') as config:
        conf = pytoml.load(config)

    log.init_log(conf, console_out=conf['logger']['console'])
    conf['log'] = log

    topic_id = 33
    from i_entity_extractor.entity_extractor_route import EntityExtractorRoute
    from i_entity_extractor.common_parser_lib.mongo import MongDb
    import json

    route = EntityExtractorRoute(conf)
    topic_info = route.all_topics.get(topic_id, None)
    begin_time = time.time()
    obj = FinancingEventsExtractor(topic_info, log)
    extract_data = {
    "_site_record_id": "http://need.pedata.cn/265460.html",
    "amount": "",
    "amounts": "NaN",
    "city": "",
    "currency": "",
    "describe": "投资界消息，拟融资企业无锡睿泰科技有限公司参加“无锡服务外包企业投融资合作对接洽谈会”。",
    "district": "",
    "enterprise_full_name": "无锡睿泰科技有限公司",
    "enterprise_short_name": "",
    "enterprise_short_name_en": "",
    "enterprise_site": "",
    "industry": "软件外包",
    "information_sources": "投资界资讯",
    "innotree_score": "",
    "mode": "私募融资",
    "phone": "",
    "project_highlights": "",
    "project_name": "睿泰科技",
    "proportion_share_transfer": "",
    "province": "",
    "public_date": "2011-11-01",
    "region": "北京 · 朝阳区",
    "round": "A",
    "source_site": "私募通",
    "tag": [],
    "units": "万元"
}
    entity_data = obj.format_extract_data(extract_data,topic_id)

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

    keys = ['units', 'source_site', 'describe', 'currency', 'tag', 'city', 'enterprise_short_name_en', 'district', 'amounts', 'innotree_score', 'public_date', 'founders', 'province', 'project_name', 'phone', 'enterprise_full_name', 'information_sources', 'proportion_share_transfer', 'enterprise_short_name', 'industry', 'region', 'enterprise_site', 'amount', 'project_highlights', 'mode', 'round', '_in_time', '_src', '_record_id', '_id']
    transfer_data(keys, 'financing_events')
    print keys
