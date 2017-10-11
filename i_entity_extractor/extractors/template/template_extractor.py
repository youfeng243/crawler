# coding=utf-8
# 模板实体解析


from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor


class TemplateExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)

    def format_extract_data(self, extract_data, topid_id):
        return {}