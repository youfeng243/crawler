# coding=utf-8
# 欠税公告实体解析
import os
import sys
sys.path.append('../')
sys.path.append('../../')

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor


class OwxExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)

    def format_extract_data(self, extract_data, topic_id):
        '''实体解析抽取数据'''

        return extract_data

