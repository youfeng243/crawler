# coding=utf-8
# 上市公告实体解析

import json
import time
import sys

sys.path.append('../')
sys.path.append('../../')

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib import toolsutil
import copy


class SsggExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.src_table = 'stock_info'

    def format_extract_data(self, extract_data, topic_id):
        '''实体解析抽取数据'''
        entity_data = copy.deepcopy(extract_data)
        tmp_code    = extract_data.get("code", "").strip()
        code        = toolsutil.re_find_one("\d+", tmp_code)
        if not code:
            code = ""
            self.log.warning("cann't extract code from [%s]" % tmp_code)

        publish_time = extract_data.get("publish_time")

        try:
            tmp_publish_time = int(publish_time[:-3])
            publish_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tmp_publish_time))
        except:
            pass

        parser_info = {}
        if entity_data.has_key("content"):
            content      = entity_data.get("content","").strip()
            notice_id    = self.get_notice_id(content)
            abstract     = content[0:256]
            parser_info["notice_id"]    = notice_id
            parser_info["abstract"]     = abstract


        entity_data["notice_id"]    = parser_info.get("notice_id")
        entity_data["abstract"]     = parser_info.get("abstract")
        entity_data["publish_time"] = publish_time
        entity_data["code"]         = code

        return entity_data

    def get_notice_id(self, content):
        '''获取上市公告编号'''
        content = unicode(content)
        pos = content.find(u'公告编号：')
        result = ''
        if pos != -1:
            pos += len(u'公告编号：')
            result = content[pos:].split(' ')[0]
            if not result:
                result = ''
        return result



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

    topic_id = 36
    from entity_extractor_route import EntityExtractorRoute

    route = EntityExtractorRoute()
    topic_info = route.all_topics.get(topic_id, None)
    obj = SsggExtractor(topic_info, common.log)
    src_url = ""
    extract_data = {
        "code": "df000783",
        "pdf_url": "http://www.cninfo.com.cnfinalpage/2016-11-11/1202824201.PDF",
        "publish_time": "1478880000000",
        "shares_referred": "长江证券",
        "title": "关于中国证监会核准新理益集团有限公司持有公司5%以上股权的股东资格的公告",
        "type": "其它重大事项",
        "content":"公告编号：4324"
    }

    data = json.dumps(extract_data)
    extract_info = ExtractInfo(ex_status=2, extract_data=data)
    base_info = BaseInfo(url=src_url)
    parser_info = PageParseInfo(base_info=base_info, extract_info=extract_info)
    entity_data = obj.entity_extract(parser_info, extract_data)

    entity_data = obj.after_extract(base_info.url, entity_data, extract_data)

    for key, value in entity_data.items():
        if isinstance(value, list):
            for i in value:
                print key, ":", i
        elif isinstance(value, dict):
            for key2, value2 in value.items():
                print key2, ":", value2
        else:
            print key, ":", value
