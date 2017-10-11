# coding=utf-8
# 土地招拍挂解析 land_auction

import json
import sys
import re

sys.path.append("..")
sys.path.append("../../")
sys.path.append("../../..")

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib.province_parser import ProvinceParser
from i_entity_extractor.common_parser_lib import toolsutil
import copy

province_city = '../../../i_entity_extractor/dict/province_city.conf'
phone_city = '../../../i_entity_extractor/dict/phonenum_city.conf'
region_city = '../../../i_entity_extractor/dict/region_city.conf'
city_city = '../../../i_entity_extractor/dict/city.conf'


class LandAuctionExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.lst_schema = [u'building_usage', u'years', u'decoration']
        self.province_parser = ProvinceParser(province_city, phone_city, region_city, city_city)

    def format_extract_data(self, extract_data, topic_id):
        '''格式化数据'''

        entity_data = copy.deepcopy(extract_data)

        # 面积单位
        acreage = extract_data.get('acreage', '')
        acreage_unit = re.sub(u'\d|\.', '', acreage)
        entity_data['acreage_unites'] = acreage_unit

        # 省分和城市
        text = entity_data.get('approved_unit', '') + entity_data.get('district', '')
        province = entity_data.get('province', '')
        city = entity_data.get('city', '')

        if not province:
            province = self.province_parser.get_province(text, 1)
        if not city:
            city = self.province_parser.get_region(text, 1)
        if province in city:
            city = city.replace(province, '')
        entity_data['province'] = province
        entity_data['city'] = city

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
