# coding=utf-8
import sys
sys.path.append('..')
from ..common.conf import config

def get_config(dict):
    config.host=dict.get('host')
    config.port=dict.get('port')
    config.server_thread_num = dict.get('server_thread_num')
    config.process_thread_num = dict.get('process_thread_num')
    config.beanstalk_conf=dict.get('beanstalk_conf')
    config.MYSQL=dict.get('MYSQL')
    config.STATISTICS_COLLECTION_NAME = dict.get('STATISTICS_COLLECTION_NAME')
    config.MONGODB = dict.get('MONGODB')
    config.logname=dict.get('logname')
    config.server=dict.get('server')
    config.backend=dict.get('backend')
    config.kafka=dict.get('kafka_server')
    return config
