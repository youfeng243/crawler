# -*- coding: utf-8 -*-

from common import log

class ValidateError(Exception):
    def __init__(self, message, detail = None):
        super(ValidateError, self).__init__(message, detail)
        self.message = message
        self.detail = detail

class ValidatorBase(object):
    def __init__(self):
        pass
    def reload(self):
        pass
    def validate(self, task):
        pass

class AlwaysFailValidator(ValidatorBase):
    def __init__(self, reason):
        self.reason = reason
    def check(self, task):
        raise ValidateError(self.reason)

class ValidateManager(object):
    def __init__(self, data_saver):
        self._data_saver = data_saver
        self.validator_map = None

        self.reload(None)

    def reload(self, topic_id = None):
        from validator.validator_jsonschema import JsonSchemaValidator
        from validator.validator_meta import MetaValidator
        if not self.validator_map:
            self.validator_map = {
                'meta': MetaValidator(),
                'jsonschema': JsonSchemaValidator(self._data_saver)
                # Other validators [validator-name: validator-instance]
            }
        else:
            try:
                self.validator_map['jsonschema'].reload(topic_id)
            except Exception as e:
                log.info("ValidateManger load {}  failed".format(topic_id))
        log.info('ValidateManager loading finished!')

    def validate(self, task):
        for validator_name in task.validator_names:
            try:
                validator_to_use = self.validator_map[validator_name]
                validator_to_use.validate(task)
            except KeyError, ke:
                log.error('ValidateManager: validator_name[%s] not found!' % validator_name)
                raise
            except ValidateError, ve:
                log.error('ValidateManager: Validation Failed when calling validator[%s]!' % (validator_name))
                raise


# validate_manager = ValidateManager()
