# coding=utf-8
# 开庭公告实体解析

import json
import sys
sys.path.append("..")
sys.path.append("../../")

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from common_parser import CommonParser
from i_entity_extractor.common_parser_lib import toolsutil

class KtggExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.seps = [',', ':', '\t']
        self.parser_obj = CommonParser(self.parser_tool, log)

    def format_extract_data(self, extract_data, topic_id):
        entity_data_list = []
        extract_data_list = self.parser_obj.before_parser(extract_data)
        for extract_data in extract_data_list:
            content = extract_data.get("content", "")
            if content:
                entity_data = self.parser_obj.get_data_from_content(extract_data)
            else:
                entity_data = self.parser_obj.get_data_from_litigants(extract_data)

            entity_data_list.append(entity_data)

        return entity_data_list



if __name__ == '__main__':
    import pytoml
    import sys
    import time

    sys.path.append('../../')
    from i_entity_extractor.conf import get_config
    from bdp.i_crawler.i_extractor.ttypes import BaseInfo, CrawlInfo, ExtractInfo, PageParseInfo

    with open('../../entity.toml', 'rb') as config:
        config = pytoml.load(config)
    conf = get_config(config)

    topic_id = 34
    from i_entity_extractor.entity_extractor_route import EntityExtractorRoute
    from i_entity_extractor.common_parser_lib.mongo import MongDb
    import json
    from common.log import log

    route = EntityExtractorRoute(conf)
    topic_info = route.all_topics.get(topic_id, None)
    begin_time = time.time()
    obj = KtggExtractor(topic_info, log)


    extract_data = {
        "content": "我院定于二〇一六年八月一日 上午九时三十分，在本院三区第1法庭依法公开开庭审理王博因其他不服2015年海行初字第01392号行政裁定(或判决）上诉一案。",
        "doc_id": ""
    }

    entity_data = obj.format_extract_data(extract_data,topic_id)
    entity_data = obj.after_process(entity_data[0])
    print "-----------------------------"
    for key, value in entity_data.items():
        if isinstance(value, list):
            for i in value:
                print key, ":", i
        elif isinstance(value, dict):
            for key2, value2 in value.items():
                print key2, ":", value2
        else:
            print key, ":", value

    print "time_cost:", time.time() - begin_time
