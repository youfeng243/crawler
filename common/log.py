import logging
from i_util.logs import LogHandler

class LogHolder(object):
    def __init__(self):
        self.log = None

    def init_log(self, conf, level=logging.DEBUG, console_out=False, name=None):
        if isinstance(level, basestring):
            level = level.lower()
        if level == "debug":
            level = logging.DEBUG
        elif level == "info":
            level = logging.INFO
        elif level == "warning":
            level = logging.WARNING
        elif level == "error":
            level = logging.ERROR
        else:
            level = logging.DEBUG
        if name is None:
            name = conf['server']['name']

        self.log = LogHandler(name, level, console_out=console_out)

    def __getattr__(self, item):
        if item == 'init_log':
            return self.init_log
        else:
            if self.log is None:
                raise Exception("Logging is not initialized")
            return getattr(self.log, item)

log = LogHolder()
