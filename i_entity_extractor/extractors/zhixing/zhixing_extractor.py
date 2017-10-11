# coding=utf-8
# 执行信息实体解析

import json
import sys

sys.path.append("..")
sys.path.append("../../")

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor

import copy



class ZhixingExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)

    def format_extract_data(self, extract_data, topic_id):
        '''解析数据'''
        entity_data = copy.deepcopy(extract_data)
        exec_money  = entity_data.get("exec_money","")
        money_unit  = entity_data.get("money_unit",u"元")
        court       = entity_data.get("court", "")
        province    = self.parser_tool.province_parser.get_province(court)

        entity_data["province"]   = province
        entity_data["money_unit"] = money_unit
        entity_data["max_money"]  = exec_money
        entity_data["sum_moeny"]  = exec_money
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

    topic_id = 42
    from entity_extractor_route import EntityExtractorRoute

    route = EntityExtractorRoute()
    topic_info = route.all_topics.get(topic_id, None)
    obj = ZhixingExtractor(topic_info, common.log)
    extract_data = {
    "case_date": "2011年04月15日",
    "case_id": "(2011)三法执字第00171号",
    "case_state": "1",
    "court": "三原县人民法院",
    "exec_money": "5000",
    "i_name": "三原县纸箱纸盒厂",
    "party_card_num": "5620332-4",
    "unique_id": "33680590"
}
    src_url = ""
    data = json.dumps(extract_data)
    extract_info = ExtractInfo(ex_status=2, extract_data=data)
    base_info = BaseInfo(url=src_url)
    parser_info = PageParseInfo(base_info=base_info, extract_info=extract_info)
    entity_data = obj.entity_extract(parser_info, extract_data)
    data = obj.after_extract(src_url, entity_data, extract_data)
    print src_url

    print "--------------------------"
    for key, value in data.items():
        if isinstance(value, list):
            for i in value:
                print key, ":", i
        elif isinstance(value, dict):
            for key2, value2 in value.items():
                print key2, ":", value2
        else:
            print key, ":", value
