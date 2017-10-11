# -*- coding: utf-8 -*-

import os
import sys
import json
import jsonschema
from jsonschema import Draft4Validator

from conf import config as conf
import common
from common import log
from validate_manager import ValidatorBase, ValidateError, AlwaysFailValidator

META_SCHEMA_FILEPATH = os.path.join(os.path.dirname(__file__), 'meta-schema.json')

class MetaValidator(ValidatorBase):
    def __init__(self):
        self.reload()

    def reload(self):
        try:
            schema_obj = json.load(open(META_SCHEMA_FILEPATH))
            Draft4Validator.check_schema(schema_obj)
            self._validator = Draft4Validator(schema_obj)
        except jsonschema.SchemaError, se:
            log.fatal('MetaValidator.init: Invalid meta-schema.json !!!')
            sys.exit(1)

    def validate(self, task):
        try:
            self._validator.validate(task.current_entity_data)
        except jsonschema.ValidationError, jve:
            raise ValidateError('meta validate error! message[%s] detail[%s]' % (jve.message, vars(jve)))
