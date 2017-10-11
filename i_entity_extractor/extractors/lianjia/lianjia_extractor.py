# coding=utf-8
# 链家实体解析
import sys

sys.path.append("..")
sys.path.append("../../")
import json

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor


class LianjiaExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)


    def format_extract_data(self, extract_data,topic_id):
        '''实体解析抽取数据'''


        return extract_data


if __name__ == "__main__":

    import sys

    sys.path.append('../../')
    topic_id = 99

    import pytoml
    from conf import get_config
    from bdp.i_crawler.i_extractor.ttypes import BaseInfo, CrawlInfo, ExtractInfo, PageParseInfo

    with open('../../entity.toml', 'rb') as config:
        config = pytoml.load(config)
    conf = get_config(config)
    import common
    from entity_extractor_route import entity_route_obj

    topic_info = entity_route_obj.read_topics()[topic_id]
    obj = LianjiaExtractor(topic_info, common.log)
    extract_data = {
        "cnt_lease": "16",
        "cnt_sale": "48",
        "deal_90d": "20",
        "id": "2411050506921",
        "subway": ""
    }
    src_url = "www.baidu.com"
    data = json.dumps(extract_data)
    extract_info = ExtractInfo(ex_status=2, extract_data=data)
    base_info = BaseInfo(url=src_url)
    parser_info = PageParseInfo(base_info=base_info, extract_info=extract_info)
    data = obj.entity_extract(parser_info, extract_data)
    print src_url

    for key, value in data.items():
        if isinstance(value, list):
            for i in value:
                print key, ":", i
        elif isinstance(value, dict):
            for key2, value2 in value.items():
                print key2, ":", value2
        else:
            print key, ":", value
