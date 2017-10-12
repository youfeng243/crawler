# -*- coding: utf-8 -*-

from log import log
from validator.validator_base import ValidateError
from i_util.global_defs import MetaFields

class ServerType:
    SINGLE_SRC = 'single'
    MULTI_SRC = 'multi'
    ALL = 'all'

class ValidateManager(object):

    default_validators = ['meta', 'jsonschema']

    def __init__(self, topic_mgr, conf, server_type):
        self.validator_map = None
        self.topic_manager = topic_mgr
        self.conf = conf
        self.server_type = server_type
        self.reload(None)


    @staticmethod
    def get_default_validators():
        return ValidateManager.default_validators

    def reload(self, topic_id = None):
        from validator.validator_jsonschema import JsonSchemaValidator
        from validator.validator_meta import MetaValidator
        from validator.validator_rk import PrimaryKeyValidator
        if not self.validator_map:
            self.validator_map = {
                'meta': MetaValidator(),
                'required_attr': JsonSchemaValidator(self.topic_manager, self.conf, self.server_type, required_check_mode=True),
                'jsonschema': JsonSchemaValidator(self.topic_manager, self.conf, self.server_type),
                'pk': PrimaryKeyValidator(self.topic_manager)
            }
        else:
            try:
                self.validator_map['jsonschema'].reload(topic_id)
            except Exception as e:
                log.info("ValidateManger load {}  failed".format(topic_id))
        log.info('ValidateManager loading finished!')

    def validate(self, doc, topic_id, site, validator_names=default_validators, soft_validating=True):
        try:
            for validator_name in validator_names:
                try:
                    validator_to_use = self.validator_map[validator_name]
                    validator_to_use.validate(doc, topic_id, site)
                except KeyError, ke:
                    log.error('ValidateManager: validator_name[%s] not found!' % validator_name)
                    raise
                except ValidateError, ve:
                    log.error('ValidateManager: Validation Failed when calling validator[%s] by %s!' % (validator_name, ve.message))
                    raise
        except ValidateError, e:
            if soft_validating:
                doc[MetaFields.HAS_SCHEMA_ERROR] = True
                doc[MetaFields.SCHEMA_ERROR_DETAIL] = "{}".format(e.message)
            else:
                raise






