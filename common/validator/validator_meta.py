# -*- coding: utf-8 -*-

import os
import sys
import json
import jsonschema
from jsonschema import Draft4Validator

from ..log import log
from validator_base import ValidatorBase, ValidateError

META_SCHEMA_FILEPATH = os.path.join(os.path.dirname(__file__), 'meta-schema.json')

class MetaValidator(ValidatorBase):
    def __init__(self):
        super(MetaValidator, self).__init__()
        self.reload()

    def reload(self):
        try:
            schema_obj = json.load(open(META_SCHEMA_FILEPATH))
            Draft4Validator.check_schema(schema_obj)
            self._validator = Draft4Validator(schema_obj)
        except jsonschema.SchemaError, se:
            log.fatal('MetaValidator.init: Invalid meta-schema.json !!!')
            sys.exit(1)

    def validate(self, doc, topic_id, site):
        try:
            self._validator.validate(doc)
        except jsonschema.ValidationError, jve:
            raise ValidateError('meta validate error! message[%s] detail[%s]' % (jve.message, vars(jve)))
