# coding=utf-8
# 小区信息-链家实体解析  xiaoqu_lianjia  99

import json
import sys
import re

sys.path.append("..")
sys.path.append("../../")

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
import copy


class NeighborhoodLianjiaExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)


    def format_extract_data(self, extract_data, topic_id):
        '''格式化数据'''
        entity_data = copy.deepcopy(extract_data)

        lst_schema = [u'developer', u'property_fee', u'property']
        for schema in lst_schema:
            schema_value = entity_data.get(schema, u'')
            entity_data[schema] = u'' if schema_value == u'暂无' else schema_value

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

