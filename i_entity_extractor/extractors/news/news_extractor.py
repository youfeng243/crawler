# coding=utf8
import sys

sys.path.append("..")
sys.path.append("../../")
import json

import time
import esm
from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib import toolsutil


class NewsExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.time_map = {u"小时": 3600, u"分钟": 60, u"秒": 1, }
        negative_word_conf = self.basic_path + 'i_entity_extractor/dict/negative_word.conf'
        self.negative_word_list = open(negative_word_conf).read().split('\n')[:-1]
        self.negative_word_index = esm.Index()
        for negative_word in self.negative_word_list:
            if negative_word:
                self.negative_word_index.enter(negative_word)
        self.negative_word_index.fix()

    def format_extract_data(self, extract_data, topic_id):

        '''新闻实体解析入口'''
        for key,value in extract_data.items():
            if value == None:
                extract_data.pop(key)


        title = extract_data.get('title', '')
        ret = self.negative_word_index.query(title)
        if ret:
            extract_data['sentiment'] = "负面"
        else:
            extract_data['sentiment'] = "正面"

        return extract_data


def main(obj):
    from pymongo import MongoClient
    import traceback
    host = '101.201.102.37'
    port = 28019
    database = 'final_data'
    coll = 'baidu_news'
    client = MongoClient(host, port)
    db = client[database][coll]
    cursor = db.find()
    num = 0
    for item in cursor:
        try:
            num += 1
            item.pop('_id')

            src_url = item.get('_src')[0]['url']

            extract_data = item
            data = json.dumps(extract_data)
            extract_info = ExtractInfo(ex_status=2, extract_data=data)
            base_info = BaseInfo(url=src_url)
            crawl_info = CrawlInfo(download_time=1474547589)
            parser_info = PageParseInfo(base_info=base_info, extract_info=extract_info, crawl_info=crawl_info)
            data = obj.do_merge(parser_info, item)
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
            if num % 100 == 0:
                break
        except:
            print traceback.format_exc()


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

    topic_id = 32
    from entity_extractor_route import EntityExtractorRoute

    route = EntityExtractorRoute()
    topic_info = route.all_topics.get(topic_id, None)
    obj = NewsExtractor(topic_info, common.log)
    main(obj)
