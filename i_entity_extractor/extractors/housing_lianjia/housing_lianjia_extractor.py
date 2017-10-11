# coding=utf-8
# 二手房-链家实体解析  ershoufang_lianjia 103

import json
import sys
import re

sys.path.append("..")
sys.path.append("../../")

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib import toolsutil
import copy


class HousingLianjiaExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.lst_schema = [u'inside_square', u'deal_type', u'type_building'
            , u'mortgage', u'elevator', u'heat', u'own_year'
            , u'house_type', u'unique', u'type_xiaoqu', u'property_right']

    def format_extract_data(self, extract_data, topic_id):
        '''格式化数据'''
        entity_data = copy.deepcopy(extract_data)

        # "暂无"用空代替(套内面积:inside_square 交易权属:deal_type 建筑类型 type_building 抵押信息 mortgage 配备电梯 elevator
        # 供暖方式 heat 房本年限 own_year 户型结构 house_type 是否唯一 unique 小区类型 type_xiaoqu  产权所属 property_right )

        for schema in self.lst_schema:
            schema_value = entity_data.get(schema, u'')
            entity_data[schema] = u'' if schema_value == u'暂无' else schema_value

        room_pattern = entity_data.get(u'room_pattern', u'')
        rooms = re.findall(u'(\d+)室', room_pattern)
        hall = re.findall(u'(\d+)厅', room_pattern)
        kitchen = re.findall(u'(\d+)厨', room_pattern)
        bathrooms = re.findall(u'(\d+)卫', room_pattern)
        entity_data[u'rooms'] = rooms[0] if len(rooms) > 0 else u''
        entity_data[u'hall'] = hall[0] if len(hall) > 0 else u''
        entity_data[u'kitchen'] = kitchen[0] if len(kitchen) > 0 else u''
        entity_data[u'bathrooms'] = bathrooms[0] if len(bathrooms) > 0 else u''

        listing_date = entity_data.get(u'listing_date', None)
        # 去掉空行
        if listing_date == None:
            listing_date = ""
        listing_date = toolsutil.etl_sub_str(listing_date, 0, 10)
        entity_data[u'listing_date'] = listing_date
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

