class ValidateError(Exception):
    def __init__(self, message, detail = None, is_lacking_required=False):
        super(ValidateError, self).__init__(message, detail)
        self.message = message
        self.detail = detail
        self.is_lacking_required = is_lacking_required

class ValidatorBase(object):
    def __init__(self):
        pass
    def reload(self):
        pass
    def validate(self, doc, topic_id, site):
        pass

class AlwaysFailValidator(ValidatorBase):
    def __init__(self, reason):
        super(AlwaysFailValidator, self).__init__()
        self.reason = reason
    def check(self, doc):
        raise ValidateError(self.reason)