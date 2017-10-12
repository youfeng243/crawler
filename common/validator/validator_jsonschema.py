# -*- coding: utf-8 -*-

import json
import os
import urlparse
import copy

import jsonschema
from jsonschema import Draft4Validator, RefResolver

from ..log import log
from ..mysql_utils import get_mysql_conn, dbrecord_to_dict
from validator_base import ValidatorBase, ValidateError, AlwaysFailValidator

BUILTIN_SCHEMA_DIR = os.path.join(os.path.dirname(__file__), 'builtin_schema')


class PATCH_TYPE:
    UNREQUIRE = "UNREQUIRE"




class HaizhiRefSolver(object):
    def __init__(self, conf):
        self.conf = conf

    def resolve_url(self, url):
        url_seg = urlparse.urlsplit(url)
        assert url_seg.scheme == 'haizhi', 'url scheme is not "haizhi"!'
        if url_seg.netloc == 'base_schema':
            mysql_conn = get_mysql_conn(self.conf)
            cur = mysql_conn.cursor()
            cur.execute('select `id`, `name`, `content` from base_schema where `name` = %s',
                        (url_seg.path.lstrip('/'),))  # 这是传了两个参数, MySQLdb里会自动做转义, 不会有SQL注入问题
            schema_obj = json.loads(cur.fetchone()[2])
            log.debug('ref-schema url[%s] loaded' % url)
            return schema_obj
        elif url_seg.netloc == 'builtin_schema':
            schema_obj = json.load(open(os.path.join(BUILTIN_SCHEMA_DIR, url_seg.path.lstrip('/') + '.json')))
            return schema_obj
        else:
            raise Exception('Unknown netloc[%s]' % url_seg.netloc)


class SchemaPatch:
    def __init__(self, patch_row):
        # TODO : add schema check
        self.patch_content = json.loads(patch_row['content'])
        self.patch_sites = set(patch_row.get('site', []).split(','))
        self.scope = patch_row.get('scope', None)
        self.type = patch_row.get('type', None)
        self.enabled = patch_row.get('enabled', False)
        self.topic_id = patch_row.get('topic_id', None)
        self.patched_validator = None  # will be filled after main schema is loaded

class ValidatorInfo:
    def __init__(self, validator, validator_json):
        self.validator = validator
        self.validator_json = validator_json
        self.patched_validators = {}

class JsonSchemaValidator(ValidatorBase):
    def __init__(self, topic_manager, conf, server_type, required_check_mode=False):
        super(JsonSchemaValidator, self).__init__()
        self.conf = conf
        self.server_type = server_type
        self._topic_manager = topic_manager
        self._topic_validator_dict = {}
        self._haizhi_ref_resolver = HaizhiRefSolver(conf)
        self.required_check_mode = required_check_mode
        self._schema_cache = {}
        self.reload()

    def create_real_validator(self, schema_obj):
        Draft4Validator.check_schema(schema_obj)
        validator_to_use = Draft4Validator(schema_obj, resolver=
            RefResolver.from_schema(schema_obj, handlers={'haizhi': self._haizhi_ref_resolver.resolve_url})
        )
        return validator_to_use


    def reload(self, reload_id=None):
        try:
            reload_id = int(reload_id)
            if reload_id <= 0: reload_id = None
        except Exception as e:
            reload_id = None
        # self.reload_patch()
        # 如果没有传入reload_id则重载所有的topic
        if not reload_id or not self._topic_validator_dict:
            for topic_id, topic_info in self._topic_manager.topic_dict.iteritems():
                # 对topic_manage里面的每个topic, 校验其Schema本身是否正确.
                # 如果不正确, 则给一个AlwaysFailValidator, 相当于让这个topic不可写入
                schema_obj = topic_info['schema_obj']
                validator_to_use = None
                try:
                    validator_to_use = self.create_real_validator(schema_obj)
                    log.info('JsonSchemaValidator.init: Schema loaded successfully! topic.id[%s], topic.name[%s]' % (
                    topic_id, topic_info['name'].encode('utf8')))
                except jsonschema.SchemaError, se:
                    validator_to_use = AlwaysFailValidator(
                        reason='Schema is invalid!! topic.id[%s], topic.name[%s]' % (topic_id, topic_info['name'])
                    )
                    log.error('JsonSchemaValidator.init: Invalid Schema! topic.id[%s], topic.name[%s], schema[%s]' %
                              (topic_id, topic_info['name'].encode('utf8'), topic_info['schema'].encode('utf8')))
                    # TODO: 这里是否应该继续抛出异常, 使得程序退出?
                self._topic_validator_dict[topic_id] = ValidatorInfo(validator_to_use, schema_obj)
        else:
            # 如果校验器存在该主题ID而主题管理器没有,则说明是删除的
            if not self._topic_manager.topic_dict.has_key(reload_id):
                if self._topic_validator_dict.has_key(reload_id):
                    del self._topic_validator_dict[reload_id]
            else:
                validator_to_use = None
                schema_obj = None
                try:
                    schema_obj = self._topic_manager.topic_dict[reload_id]['schema_obj']
                    validator_to_use = self.create_real_validator(schema_obj)
                    log.info('JsonSchemaValidator.init: Schema loaded successfully! topic.id[%s], topic.name[%s]' % (
                    reload_id, self._topic_manager.topic_dict[reload_id]['name'].encode('utf8')))

                except jsonschema.SchemaError, se:
                    validator_to_use = AlwaysFailValidator(
                        reason='Schema is invalid!! topic.id[%s], topic.name[%s]' % (
                        reload_id, self._topic_manager.topic_dict[reload_id])
                    )
                    # TODO: 这里是否应该继续抛出异常, 使得程序退出?

                self._topic_validator_dict[reload_id] = ValidatorInfo(validator_to_use, schema_obj)

    def validate(self, doc, topic_id, site):
        # 取出task.topic_id对应的validator, 并校验task.current_entity_data
        try:
            validator_to_use = self._topic_validator_dict[topic_id]
            validator_to_use.validator.validate(doc)
        except jsonschema.ValidationError, jve:
            lacking_required = False
            if jve.validator == u'required':
                lacking_required = True
            raise ValidateError('Jsonschema validate error:[{}]\t{}'.format(jve.absolute_path[0], jve.message), vars(jve), is_lacking_required=lacking_required)
