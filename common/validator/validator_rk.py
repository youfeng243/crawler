# -*- coding: utf-8 -*-

import os
import sys
import json
import jsonschema
from jsonschema import Draft4Validator

from ..log import log
from validator_base import ValidatorBase, ValidateError
from i_util.tools import lookup_dict_path

META_SCHEMA_FILEPATH = os.path.join(os.path.dirname(__file__), 'meta-schema.json')

class PrimaryKeyValidator(ValidatorBase):
    def __init__(self, topic_mgr):
        super(PrimaryKeyValidator, self).__init__()
        self.topic_manager = topic_mgr
        self.reload()

    def validate(self, doc, topic_id, site):
        pks = self.topic_manager.get_primary_keys_by_id(topic_id)
        # pks is a list consisting of the columns of pk, url will be used as pk
        # if this list is empty, so no need to check here (Every doc fragment MUST has url)
        fail = False
        lack_pks = []
        if len(pks) > 0:
            for col in pks:

                value, exists = lookup_dict_path(doc, col)
                if not exists:
                    lack_pks.append(col)
                    fail = True
                elif value is None or str(value).strip() == '':
                    lack_pks.append(col)
                    fail = True
        if fail:
            raise ValidateError('Primary key validate error:Lack of %s' % (str(lack_pks)), lack_pks, is_lacking_required=True)
