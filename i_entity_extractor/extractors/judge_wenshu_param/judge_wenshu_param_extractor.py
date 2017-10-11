# coding=utf8
import sys
import json
import copy

sys.path.append('../../')
from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib import toolsutil


class JudgeWenshuParamExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)

    def format_extract_data(self, extract_data, topic_id):
        '''实体解析抽取数据'''
        entity_data = copy.deepcopy(extract_data)
        court       = extract_data.get("court", "")
        if not court:
            return {}

        province = self.parser_tool.province_parser.get_province(court)
        cur_date = toolsutil.get_cur_date()
        yes_date = toolsutil.get_yes_date()
        #yes_date = '2016-11-21'
        if province in ['北京', '上海', '重庆', '天津']:
            province_str = "市,中级法院:"
        else:
            province_str = "省,中级法院:"
        Param = "上传日期:" + yes_date + " TO " + cur_date + ",法院地域:" + province + province_str + court

        entity_data["province"] = province
        entity_data["court"]    = court
        entity_data["Param"]    = Param

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

    topic_id = 83
    from entity_extractor_route import EntityExtractorRoute

    route = EntityExtractorRoute()
    topic_info = route.all_topics.get(topic_id, None)
    obj = JudgeWenshuParamExtractor(topic_info, common.log)
    extract_data = {
        "court": "山东省高级人民法院",
        "sum":"df",
    }
    src_url = "www.baidu.com"
    data = json.dumps(extract_data)
    extract_info = ExtractInfo(ex_status=2, extract_data=data)
    base_info = BaseInfo(url=src_url)
    parser_info = PageParseInfo(base_info=base_info, extract_info=extract_info)
    data = obj.entity_extract(parser_info, extract_data)
    entity_data = obj.entity_extract(parser_info, extract_data)
    entity_data = obj.after_extract(src_url, entity_data, extract_data)
    print src_url

    for key, value in entity_data.items():
        if isinstance(value, list):
            for i in value:
                print key, ":", i
        elif isinstance(value, dict):
            for key2, value2 in value.items():
                print key2, ":", value2
        else:
            print key, ":", value
