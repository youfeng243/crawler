# coding=utf-8
# 小区信息-安居客实体解析  neighborhood_anjuke

import json
import sys
import re

sys.path.append("..")
sys.path.append("../../")

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from pyquery import PyQuery as py
import copy


class NeighborhoodAnjukeExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)


    def format_extract_data(self, extract_data, topic_id):
        '''格式化数据'''
        entity_data = copy.deepcopy(extract_data)

        # "暂无数据"用空代替(开发商 developer,物业费用 property_fee,物业公司 property,总建面 total_floor_area,
        # 简介 basic_profile ,总户数 households ,物业类型 property_types,容积率 capacity_rate,停车位 parking_space,本月均价 avg_price)
        # lst_schema=['developer','property_fee','property','total_floor_area','basic_profile','households','property_types','parking_space','capacity_rate','avg_price']
        # for schema in lst_schema:
        #     schema_name = item.get(schema, '')
        #     item[schema] = regex_remove_non_num(schema_name).strip()
        name = entity_data.get(u'name', None)
        address = entity_data.get(u'address', None)
        if name is None and address is None:
            return {}

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
    from entity_extractor_route import EntityExtractorRoute

    route = EntityExtractorRoute()
    topic_id = 161
    topic_info = route.all_topics.get(topic_id, None)
    obj = KtggExtractor(topic_info, common.log)

