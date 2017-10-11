#!/usr/bin/env
#-*- coding:utf-8 -*-
import sys
sys.path.append('..')
import logging

THRIFT_DOWNLOADER_CONFIG = {
    "port":12200,
    "host":"127.0.0.1"
}
THRIFT_EXTRACTOR_CONFIG = {
    "port":12300,
    "host":"127.0.0.1"
}

THRIFT_SCHEDULER_CONFIG = {
    "port":12100,
    "host":"127.0.0.1"
}

THRIFT_ENTITY_EXTRACTOR_CONFIG = {
    "port":12500,
    "host":"127.0.0.1"
}

THRIFT_CRAWLERMERGE_CONFIG = {
    "port":12400,
    "host":"127.0.0.1"
}

THRIFT_DATA_SAVER_CONFIG = {
    "port":12600,
    "host":"127.0.0.1"
}
BEANSTALKD_CONFIG = { 
    "host":"127.0.0.1",
    "port":11300,
    'download_rsp_tube':'download_rsp'
}
KAFKA_CONFIG = { 
     "kafka_host":'172.18.180.223:9092,172.18.180.222:9092,172.18.180.225:9092',
     "zk_host":'172.18.180.223:2181,172.18.180.222:2181,172.18.180.225:2181',
     'download_rsp_tube':'realtime_download_rsp'
 }
MYSQL = "mysql://work:haizhi@)@127.0.0.1:3306/cmb_crawl?charset=utf8"

REDIS = {
    'host': '127.0.0.1',  # 线上redisIP
    'password': 'haizhi@)',
    'port': 6379,
    'database':8,
    'proxy_name':'proxies',
    'proxy_test_available':24 #hours
}

SERVER = {
    'port':8571,
    'host':'0.0.0.0',
    'debug':False
}

MONGODB={
    'host':'172.17.1.119',
    'port':40042,
    'database':'task_collect',
    'username':'work',
    'password':'haizhi'
}

final_data={
    'host':'172.17.1.119',
    'port':40042,
    'name':'app_data',
    'username':'work',
    'password':'haizhi'
}
realtime_crawl = {
    'api':'http://172.18.180.225:9823/gsxt_info',
    'query':'company'
}

from i_util.logs import LogHandler
log = LogHandler("manager", loglevel=logging.INFO)
