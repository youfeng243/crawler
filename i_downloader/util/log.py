#coding=utf-8
__author__ = 'fengoupeng'

import logging
import os
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler

LOG_DIRECTORY = '/var/tmp/'
CRAWLER_PATH=os.getenv('CRAWLER_PATH')
if CRAWLER_PATH:
    LOG_DIRECTORY = '/%s/logs/' % CRAWLER_PATH

def LogHandler(name, loglevel=logging.INFO, console_out=False):
    if not os.path.exists(LOG_DIRECTORY):
        os.makedirs(LOG_DIRECTORY)

    LOG_FILE = LOG_DIRECTORY + "%s.log" % name
    fmt = "%(asctime)s - %(processName)s - %(filename)s[line:%(lineno)d] %(levelname)s - %(message)s"
    formatter = logging.Formatter(fmt)
    handler = TimedRotatingFileHandler(LOG_FILE, when='MIDNIGHT', interval=1, backupCount=5)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(handler)

    if console_out == True:
        console_handler = logging.StreamHandler()
        formatter = logging.Formatter(fmt)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    logger.setLevel(loglevel)
    return logger

