# coding=utf-8
# 行政处罚实体解析

import json
import sys

sys.path.append("..")
sys.path.append("../../")

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
import os
import requests
import traceback
import penalty_conf
from i_entity_extractor.common_parser_lib import toolsutil
import copy


class PenaltyExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)

    def format_extract_data(self, extract_data, topic_id):
        '''解析数据'''
        if extract_data.has_key("excel_urls"):
            excel_urls = extract_data["excel_urls"]
            excel_data_list = self.get_data_from_excelurl(excel_urls)
            excel_data_list = self.replace_field(excel_data_list, extract_data)
            return excel_data_list
        else:
            return self.format_extract_data_inner(extract_data)

    def format_extract_data_inner(self, extract_data):
        entity_data = copy.deepcopy(extract_data)
        title = unicode(extract_data.get("title", ""))
        province = extract_data.get("province", "")

        pos = title.find(u'局行政处罚')
        if pos != -1:
            execute_authority = title[:pos] + u'局'
            if execute_authority[0] == u'市':
                execute_authority = province + execute_authority
            entity_data["execute_authority"] = execute_authority
        return entity_data

    def get_data_from_excelurl(self, excel_url):
        '''从excel链接中提取数据'''
        excel_data_list = []
        if not excel_url:
            return excel_data_list

        url_agent = 'Mozilla/5.0 (X11; Linux i86_64) AppleWebKit/537.36 ' + '(KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36'
        headers = {'User-Agent': url_agent}
        current_path = os.getcwd()

        try:
            file = excel_url.split('/')[-1].strip()
            filename_list = excel_url.split('/')[-1].split('.')
            filename = filename_list[0].strip() + "." + filename_list[-1].strip()
            if not os.path.exists(current_path + '/' + file):
                r = requests.get(excel_url.strip(), headers=headers, stream=True)
                with open(filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
                            f.flush()
                self.log.info("download [%s] ok !" % filename)

            excel_data_list = self.parser_tool.excel_parser.read_excel(filename)
            os.remove(filename)
            return excel_data_list

        except:
            self.log.error("read from excel error, ret[%s]" % traceback.format_exc())
            return []

    def replace_field(self, data_list, extract_data):
        '''映射字段名'''
        province = extract_data.get('province')
        publish_time = extract_data.get('publish_time')
        entity_data_list = []
        sep_list = ['\t', ' ', ',', '　', '，', ':', "："]
        for data in data_list:
            entity_data = {}
            for key, value in data.items():
                for sep in sep_list:
                    key = unicode(key).replace(sep, '')
                if penalty_conf.name_replace_map.has_key(key):
                    print key, type(key)
                    real_key = penalty_conf.name_replace_map[key]
                    if real_key == 'execute_authority_time':
                        temp_list = toolsutil.my_split(value, sep_list)
                        if len(temp_list) == 2:
                            entity_data['execute_authority'] = temp_list[0]
                            entity_data['penalty_time'] = temp_list[1]
                        else:
                            entity_data['execute_authority'] = value[:value.find(u'局') + 1]
                            entity_data['penalty_time'] = self.parser_tool.date_parser.get_date_list(value)
                    else:
                        entity_data[real_key] = value
            entity_data['province'] = province
            entity_data['publish_time'] = publish_time
            entity_data_list.append(entity_data)
        return entity_data_list


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

    topic_id = 68
    from entity_extractor_route import EntityExtractorRoute

    route = EntityExtractorRoute()
    topic_info = route.all_topics.get(topic_id, None)
    obj = PenaltyExtractor(topic_info, common.log)
    extract_data = {
        "province": "北京",
        "title":"市工商局行政处罚",
    }
    src_url = ""
    data = json.dumps(extract_data)
    extract_info = ExtractInfo(ex_status=2, extract_data=data)
    base_info = BaseInfo(url=src_url)
    parser_info = PageParseInfo(base_info=base_info, extract_info=extract_info)
    entity_data = obj.entity_extract(parser_info, extract_data)

    data = obj.after_extract(src_url, entity_data, extract_data)
    for key, value in data.items():
        if isinstance(value, list):
            for i in value:
                print key, ":", i
        elif isinstance(value, dict):
            for key2, value2 in value.items():
                print key2, ":", value2
        else:
            print key, ":", value
