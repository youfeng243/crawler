# -*- coding: utf-8 -*-

import json
import copy
import common
import datetime

class DataSaveTask(object):
    def __init__(self, entity_extractor_info, merger_names = ['default'], validator_names = ['jsonschema']):
        self.accept_time = datetime.datetime.now()

        self.original_entity_extractor_info = entity_extractor_info

        self.entity_source = entity_extractor_info.entity_source
        self.topic_id = entity_extractor_info.topic_id
        self.update_time = entity_extractor_info.update_time
        self.new_entity_data = json.loads(entity_extractor_info.entity_data)
        self.record_id = self.new_entity_data.get(common.FIELDNAME_RECORD_ID, None)     # TODO: 将来考虑放到thrift的包里面？

        self.old_entity_data = None
        self.current_entity_data = None

        self.merger_names = copy.deepcopy(merger_names)
        self.merger_names.insert(0, 'meta')     # 在最前面增加一个meta数据的合并器
        self.validator_names = copy.deepcopy(validator_names)
        self.validator_names.insert(0, 'meta')  # 在最前面增加一个meta数据的校验器
