# -*- coding: utf-8 -*-

import json
import os
import urlparse

import jsonschema
from jsonschema import Draft4Validator, RefResolver

from common import log, get_mysql_conn
from validate_manager import ValidatorBase, ValidateError, AlwaysFailValidator

BUILTIN_SCHEMA_DIR = os.path.join(os.path.dirname(__file__), 'builtin_schema')

class HaizhiRefSolver(object):
    def __init__(self, data_saver):
        pass

    def resolve_url(self, url):
        url_seg = urlparse.urlsplit(url)
        assert url_seg.scheme == 'haizhi', 'url scheme is not "haizhi"!'
        if url_seg.netloc == 'base_schema':
            mysql_conn = get_mysql_conn()
            cur = mysql_conn.cursor()
            cur.execute('select `id`, `name`, `content` from base_schema where `name` = %s', 
                (url_seg.path.lstrip('/'), ))     # 这是传了两个参数, MySQLdb里会自动做转义, 不会有SQL注入问题
            schema_obj = json.loads(cur.fetchone()[2])
            log.debug('ref-schema url[%s] loaded' % url)
            return schema_obj
        elif url_seg.netloc == 'builtin_schema':
            schema_obj = json.load(open(os.path.join(BUILTIN_SCHEMA_DIR, url_seg.path.lstrip('/')+'.json')))
            return schema_obj
        else:
            raise Exception('Unknown netloc[%s]' % url_seg.netloc)

class JsonSchemaValidator(ValidatorBase):
    def __init__(self, data_saver):
        self._topic_manager = data_saver.topic_manager
        self._topic_validator_dict = {}
        self._haizhi_ref_resolver = HaizhiRefSolver(self._data_saver)
        self.reload()

    def reload(self, reload_id = None):
        try:
            reload_id = int(reload_id)
            if reload_id <= 0:reload_id = None
        except Exception as e:
            reload_id = None
        #如果没有传入reload_id则重载所有的topic
        if not reload_id or not self._topic_validator_dict:
            for topic_id, topic_info in self._topic_manager.topic_dict.iteritems():
                # 对topic_manage里面的每个topic, 校验其Schema本身是否正确.
                # 如果不正确, 则给一个AlwaysFailValidator, 相当于让这个topic不可写入
                schema_obj = topic_info['schema_obj']
                validator_to_use = None
                try:
                    Draft4Validator.check_schema(schema_obj)
                    validator_to_use = Draft4Validator(schema_obj, resolver =
                        RefResolver.from_schema(schema_obj, handlers =
                            {
                                'haizhi': self._haizhi_ref_resolver.resolve_url
                            }
                        )
                    )
                    log.info('JsonSchemaValidator.init: Schema loaded successfully! topic.id[%s], topic.name[%s]' % (topic_id, topic_info['name'].encode('utf8')))
                except jsonschema.SchemaError, se:
                    validator_to_use = AlwaysFailValidator(
                        reason = 'Schema is invalid!! topic.id[%s], topic.name[%s]' % (topic_id, topic_info['name'])
                    )
                    log.error('JsonSchemaValidator.init: Invalid Schema! topic.id[%s], topic.name[%s], schema[%s]' %
                        (topic_id, topic_info['name'].encode('utf8'), topic_info['schema'].encode('utf8')))
                    # TODO: 这里是否应该继续抛出异常, 使得程序退出?
                self._topic_validator_dict[topic_id] = validator_to_use
        else:
            #如果校验器存在该主题ID而主题管理器没有,则说明是删除的
            if not self._topic_manager.topic_dict.has_key(reload_id):
                if self._topic_validator_dict.has_key(reload_id):
                    del self._topic_validator_dict[reload_id]
            else:
                validator_to_use = None
                try:
                    schema_obj = self._topic_manager.topic_dict[reload_id]['schema_obj']
                    Draft4Validator.check_schema(schema_obj)
                    validator_to_use = Draft4Validator(schema_obj, resolver=
                    RefResolver.from_schema(schema_obj, handlers={
                        'haizhi': self._haizhi_ref_resolver.resolve_url
                    })
                    )
                    log.info('JsonSchemaValidator.init: Schema loaded successfully! topic.id[%s], topic.name[%s]' % (reload_id, self._topic_manager.topic_dict[reload_id]['name'].encode('utf8')))

                except jsonschema.SchemaError, se:
                    validator_to_use = AlwaysFailValidator(
                        reason='Schema is invalid!! topic.id[%s], topic.name[%s]' % (reload_id, self._topic_manager.topic_dict[reload_id])
                    )
                    # TODO: 这里是否应该继续抛出异常, 使得程序退出?

                self._topic_validator_dict[reload_id] = validator_to_use


    def validate(self, task):
        # 取出task.topic_id对应的validator, 并校验task.current_entity_data
        validator_to_use = self._topic_validator_dict[task.topic_id]
        try:
            validator_to_use.validate(task.current_entity_data)
        except jsonschema.ValidationError, jve:
            raise ValidateError('jsonschema validate error! message[%s] detail[%s]' % (jve.message, vars(jve)))
