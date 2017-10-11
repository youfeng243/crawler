#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author  gikieng.li
# @Date    2017-05-17 15:56

import json
import sys
import threading
import time
from functools import wraps
from beanstalkc import SocketError
sys.path.append("..")
sys.path.append("../..")
import pymongo
import queue
from i_util.logs import LogHandler
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer
import traceback
from bdp.i_crawler.i_downloader.ttypes import DownLoadRsp,DownLoadReq
from i_util.pybeanstalk import PyBeanstalk

"""
struct DownLoadRsp
{
    1:required string                   url,                   #必选字段，下载的url
    2:optional string                   redirect_url,          #必选字段，跳转url，无跳转则为跳转前url
    3:optional string                   src_type,              #可选字段，url来源
    4:required CrawlStatus              status,                #必须字段，下载状态
    5:required i32                      http_code,             #必须字段，http返回码
    6:optional i32                      download_time,         #可选字段，下载时间
    7:optional i32                      elapsed,               #可选字段，下载耗时
    8:list<Page>                        pages,                 #可选字段，下载的页面历史
    9:optional string                   content_type,          #可选字段，下载的类型
    10:optional string                  content,               #可选字段，网页的内容
    11:optional i32                     page_size,             #可选字段，网页的大小
    12:optional string                  scheduler,             #可选属性，调度相关信息
    13:optional string                  parse_extends,         #可选属性, 解析相关信息
    14:optional string                  data_extends,          #可选属性，扩展属性
    15:optional map<string, string>     info            = {},  #可选属性，额外的信息
}
struct
"""
def after_proccess_document(func):
    @wraps(func)
    def wrapper(self, ori_obj):
        for obj in func(self, ori_obj):
            if obj:
                self.output_queue.put(obj)
        return None

    return wrapper


def after_get_data(func):
    @wraps(func)
    def wrapper(self):
        ret = func(self)
        self.finish = True
        return ret

    return wrapper


class BaseTransfer(object):
    def __init__(self):
        """
        self.beanstalk = PyBeanstalk("101.201.100.58", 11300)
        self.output_tube = 'offline_download_rsp'
        self.output_queue = queue.Queue(maxsize = 1024)
        self.mongo_client = pymongo.MongoClient('101.201.100.58', 27017)

        """
        raise NotImplementedError()

    def __new__(cls, *args, **kwargs):
        obj = object.__new__(cls, *args, **kwargs)
        obj.mongo_thread = threading.Thread(target=obj.get_data_from_mongo)
        obj.mongo_thread.daemon = True
        obj.beanstalk_thread = threading.Thread(target=obj.transfer_data_to_beanstalk)
        obj.beanstalk_thread.daemon = True
        obj.finish = False
        return obj


    def prepare_download_rsp(self, url, content_type="html/text"):
        obj = DownLoadRsp()
        obj.url = url
        obj.elapsed = 50
        obj.content_type = content_type
        obj.status = 0
        obj.http_code = 200
        obj.download_time = int(time.time())
        return obj

    def start(self):
        self.mongo_thread.start()
        self.beanstalk_thread.start()
        while self.mongo_thread.isAlive() \
            and \
            self.beanstalk_thread.isAlive():
            time.sleep(20)

    @after_get_data
    def get_data_from_mongo(self):
        raise NotImplemented

    @after_proccess_document
    def process_docment(self, ori_obj):
        try:
            base_info_obj = self.prepare_download_rsp(ori_obj['base_info_url'])
            base_info_obj.content = ori_obj['base_info'].encode('utf-8')
            base_info_obj.page_size = len(base_info_obj.content)
            yield base_info_obj
        except Exception as e:
            print e.message
    def to_string(self, obj):
        str_parse = None
        try:
            tMemory_b = TMemoryBuffer()
            tBinaryProtocol_b = TBinaryProtocol(tMemory_b)
            obj.write(tBinaryProtocol_b)
            str_parse = tMemory_b.getvalue()
        except EOFError as e:
            print e.message
            pass
        return str_parse

    def transfer_data_to_beanstalk(self):
        cnt = 0
        print "start beanstalk"
        while not self.output_queue.empty() or not self.finish:
            try:
                download_rsp = self.output_queue.get(block=True, timeout=25)
                download_rsp_str = self.to_string(download_rsp)
                if len(download_rsp_str) > 300000:
                    print len(download_rsp_str)
                    continue
                while True:
                    try:
                        self.beanstalk.put(self.output_tube, download_rsp_str)
                        break
                    except SocketError as e:
                        time.sleep(2 * 60)
                        self.beanstalk.reconnect()
                        print "reconnect beanstalk"
                cnt += 1
                if cnt % 500 == 0:
                    print "get\t%d" %cnt
                    #time.sleep(5)
            except Exception as e:
                print traceback.format_exc()
                time.sleep(10)

class Transfer(BaseTransfer):
    def __init__(self, site, url_pattern=".*", test=True, parser_id=None):
        global logger
        self.beanstalk = PyBeanstalk("Crawler-Downloader1:Crawler-Downloader2", 11300)
        self.output_tube = 'download_rsp'
        self.output_queue = queue.Queue(maxsize = 1000)
        self.mongo_client = pymongo.MongoClient('Crawler-DataServer1', 40042)
        self.site = site
        self.url_pattern = url_pattern
        self.test = test
        self.parser_id = parser_id


    @after_proccess_document
    def process_docment(self, ori_obj):
        try:
            src_url = ori_obj['url']
            if isinstance(src_url, list): src_url = src_url[0]
            obj = self.prepare_download_rsp(src_url)
            obj.content = ori_obj['content'].encode('utf-8')
            obj.page_size = len(obj.content)
            obj.parse_extends = '{"parser_id": %d}' % self.parser_id \
                                if self.parser_id \
                                else ori_obj.get("parse_extends")
            yield obj
        except Exception as e:
            logger.info(traceback.format_exc())


    @after_get_data
    def get_data_from_mongo(self):
        crawl_merge_webpage = self.mongo_client['crawl_merge_webpage']
        crawl_merge_webpage.authenticate('work', 'haizhi')
        db = crawl_merge_webpage[self.site]
        cursor = None
        if self.test:
            cursor = db.find({"url":{"$regex":self.url_pattern}}).limit(10)
        else:
            cursor = db.find({"url":{"$regex":self.url_pattern}})
        for item in cursor:
            try:
                if not item or len(item['content']) < 10:continue
                print item['url']
                self.process_docment(item)
            except Exception as e:
                logger.info(traceback.format_exc())

import click
@click.command()
@click.option("-s", "--site", required=True)
@click.option('-u', "--url_pattern",required=False, default=".*")
@click.option('-t', "--test",required=False, default=True,type=bool)
@click.option('-p', "--parser_id",required=False,type=int)
def cli(**option):
    Transfer(**option).start()

if __name__ == "__main__":
    cli()
