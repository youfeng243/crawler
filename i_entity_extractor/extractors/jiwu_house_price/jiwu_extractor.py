# coding=utf-8
# 吉屋网实体解析
import sys

sys.path.append("..")
sys.path.append("../../")
import traceback
import json

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor



class JiWuExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)

    def format_extract_data(self, extract_data, topic_id):
        '''实体解析抽取数据'''
        result = extract_data

        city = result.get("city", "")
        province = self.parser_tool.province_parser.get_province(city)
        region = result.get("region", "")
        new_house_price = 0.0
        second_house_price = 0.0
        try:
            new_house_s = result.get("new_house_price", '0.0')
            new_house_price = round(float(new_house_s), 2)
        except:
            new_house_price = 0.0
        try:
            second_house_s = result.get("second_house_price", '0.0')
            second_house_price = round(float(second_house_s), 2)
        except:
            second_house_price = 0.0
        entity_data = {
            "province": province,
            "city": city,
            "region": region,
            "new_house_price": new_house_price,
            "second_house_price": second_house_price,
        }

        return entity_data


if __name__ == "__main__":
    import conf
    from pymongo import MongoClient
    from bdp.i_crawler.i_extractor.ttypes import BaseInfo, CrawlInfo, ExtractInfo, PageParseInfo
    from common_parser_lib.parser_tool import ParserTool
    from entity_extractor_route import EntityExtractorRoute

    topic_id = 65
    parser_tool = ParserTool(conf)
    route = EntityExtractorRoute(conf, parser_tool)
    topic_info = route.all_topics.get(topic_id, None)

    extract_data = {

    }
    src_url = "www.baidu.com"
    parser_tool = ParserTool(conf)
    obj = JiWuExtractor(conf.log, topic_info, parser_tool)
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
