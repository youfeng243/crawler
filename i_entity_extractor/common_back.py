# -*- coding: utf-8 -*-
from conf import config

# logging是线程安全的
import logging
# logging.basicConfig(level = logging.DEBUG)
from i_util.logs import LogHandler

log = LogHandler(config.server['name']+str(config.server['port']), logging.INFO)
import MySQLdb


def get_mysqldb():
    # 1 mysql init
    mysql_db = MySQLdb.connect(
        host=config.MYSQL['host'],
        port=config.MYSQL['port'],
        user=config.MYSQL['username'],
        passwd=config.MYSQL['password'],
        db=config.MYSQL['database'],
        charset='utf8'
    )
    return mysql_db


def dbrecord_to_dict(descriptions, record):
    '''mysql元组记录转换为字典'''
    return {
        desc[0]: field_value
        for desc, field_value in zip(descriptions, record)
        }
