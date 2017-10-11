# coding=utf-8
# 年龄指数实体解析
import sys

sys.path.append("..")
sys.path.append("../../")
import time
import traceback
import json

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor


class BaiDuIndexExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)

    def format_extract_data(self, extract_data, topic_id):
        '''实体解析抽取数据'''
        tmp_age_list = extract_data.get("age", "")

        rate_list = []
        if isinstance(tmp_age_list, list) and len(tmp_age_list) == 5:
            age_list = map(float, tmp_age_list)
            sum_age = sum(age_list)
            for age in age_list:
                rate = (age * 100) / sum_age
                rate_list.append(rate)
        else:
            return {}

        tmp_sex_list = extract_data.get("sex", "")
        sex_list = map(float, tmp_sex_list)
        if isinstance(sex_list, list) and len(sex_list) == 4:
            male_rate = (sex_list[1] * 100) / sex_list[0]
            female_rate = (sex_list[3] * 100) / sex_list[2]

            if (male_rate + female_rate - 100) < -0.0001 and (male_rate + female_rate - 100) > 0.0001:
                if (female_rate - male_rate) > 0:
                    male_rate = 100 - female_rate
                else:
                    female_rate = 100 - male_rate
        else:
            self.log.error("ret[%s]" % traceback.format_exc())
            return {}

        index_name = extract_data.get("index_name", "")

        entity_data = {
            "19-": rate_list[0],
            "20-29": rate_list[1],
            "30-39": rate_list[2],
            "40-49": rate_list[3],
            "50+": rate_list[4],
            "index_name": index_name,
            "sex_male": male_rate,
            "sex_female": female_rate,
            "province_rank": extract_data.get("province_rank"),
        }

        return entity_data


if __name__ == "__main__":
    import conf
    from bdp.i_crawler.i_extractor.ttypes import BaseInfo, CrawlInfo, ExtractInfo, PageParseInfo
    from common_parser_lib.parser_tool import ParserTool
    from entity_extractor_route import EntityExtractorRoute

    topic_id = 56
    parser_tool = ParserTool(conf)
    route = EntityExtractorRoute(conf, parser_tool)
    topic_info = route.all_topics.get(topic_id, None)
    parser_tool = ParserTool(conf)
    obj = BaiDuIndexExtractor(conf.log, topic_info, parser_tool)
    extract_data = {}
    src_url = "www.baidu.com"
    data = json.dumps({})
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
