# coding=utf8
import sys
import re
import time
sys.path.append('../../')
from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib import etl_tool
from i_entity_extractor.common_parser_lib import toolsutil as tool
import copy


class ListingEventsExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)

    def format_extract_data(self, extract_data, topic_id):
        """实体解析抽取数据"""
        item = copy.deepcopy(extract_data)

        # 处理行业
        industry = item.get(u'industry', u'')
        lst_industry = etl_tool.regex_chinese(industry)
        item['industry'] = '/'.join(lst_industry)

        # 处理上市时间
        date = item.get(u'market_date', u'')
        if date:
            market_date = date.replace(u'- -', u'')
        else:
            market_date = ''
        res_market_date = etl_tool.str2datetime(market_date, '%Y-%m-%d')
        item[u'market_date'] = res_market_date if res_market_date == None else res_market_date.strftime("%Y-%m-%d")

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

    obj = ListingEventsExtractor(topic_info, common.log)
    extract_data = {
        "_site_record_id": "http://www.pedata.cn/ipo/321436101.html",
    "accounting_firm": "大华会所",
    "enterprise_full_name": "广东芳源环保股份有限公司",
    "equity": "31,000,000",
    "exchanges": "全国中小企业股份转让系统(新三板)",
    "industry": "互联网 电商",
    "law_firm": "广东华商律所",
    "lead_underwriter": "华创证券",
    "market_date": "2016-10-21",
    "site_url": "http://www.pedata.cn/ipo/321436101.html",
    "source_site": "私募通",
    "stock_code": "839247"
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
    keys = ['lead_underwriter', 'source_site', 'accounting_firm', 'enterprise_full_name', 'exchanges', 'stock_code', 'industry', 'site_url', 'law_firm', 'equity', 'market_date', '_in_time', '_src', '_record_id', '_id']
    transfer_data(keys, 'listing_events')
    print keys

