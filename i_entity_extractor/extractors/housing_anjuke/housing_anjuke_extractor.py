# coding=utf-8
# 安居客实体解析  ershoufang_anjuke

import json
import sys
import re

sys.path.append("..")
sys.path.append("../../")

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib import toolsutil
import copy


class HousingAnjukeExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.lst_schema = [u'building_usage', u'years', u'decoration']


    def format_extract_data(self, extract_data, topic_id):
        '''格式化数据'''
        entity_data = copy.deepcopy(extract_data)

        # 1 "暂无"用空代替(类型building_usage,年代years,装修程度decoration)
        for schema in self.lst_schema:
            schema_value = entity_data.get(schema, '')
            entity_data[schema] = entity_data[schema] = u'' if schema_value == u'暂无' else schema_value

        # 2 处理参考首付
        deal_price = entity_data.get(u'reference_payment', u'')
        res_deal_price = self.parser_tool.money_parser.new_trans_money(deal_price, u"万", True)
        entity_data['reference_payment'] = res_deal_price[0]
        entity_data['reference_payment_unit'] = res_deal_price[1]
        entity_data['deal_price_ccy'] = res_deal_price[2]

        # 3 处理发布时间
        listing_date = entity_data.get(u'listing_date', u'')
        listing_date = toolsutil.etl_sub_str(listing_date, 0, 10)
        entity_data['listing_date'] = listing_date

        room_pattern = entity_data.get(u'room_pattern', u'')
        rooms = re.findall(u'(\d+)室', room_pattern)
        hall = re.findall(u'(\d+)厅', room_pattern)
        kitchen = re.findall(u'(\d+)厨', room_pattern)
        bathrooms = re.findall(u'(\d+)卫', room_pattern)
        entity_data['rooms'] = rooms[0] if len(rooms) > 0 else u''
        entity_data['hall'] = hall[0] if len(hall) > 0 else u''
        entity_data['kitchen'] = kitchen[0] if len(kitchen) > 0 else u''
        entity_data['bathrooms'] = bathrooms[0] if len(bathrooms) > 0 else u''
        return entity_data


if __name__ == '__main__':

    import pytoml
    import sys

    sys.path.append('../../')
    from conf import get_config
    from bdp.i_crawler.i_extractor.ttypes import BaseInfo, ExtractInfo, PageParseInfo

    with open('../../entity.toml', 'rb') as config:
        config = pytoml.load(config)
    conf = get_config(config)
    import common

