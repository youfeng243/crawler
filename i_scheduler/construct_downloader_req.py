# -*- coding:utf-8 -*-

import pymongo
import logging
import json
import time
import traceback
import threading
import sys
sys.path.append('..')
from bdp.i_crawler.i_downloader.ttypes import DownLoadReq
from bdp.i_crawler.i_downloader.ttypes import SessionCommit
from Queue import PriorityQueue, Empty
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer
from i_util.pybeanstalk import PyBeanstalk

index_queue = PriorityQueue()


class MongoDB(object):
    def __init__(self, host, port, database, collection):
        try:
            self.conn = pymongo.MongoClient(host, port)
            db_auth = self.conn.admin
            db_auth.authenticate("root", "Bf^8x8U")
            self.database = self.conn['%s' % database]
            self.collection = self.database['%s' % collection]
        except Exception, e:
            print "connect mongodb fail:%s" % e.message
            exit(1)

    def find(self):
        try:
             return self.collection.find({"url": {'$regex': 'http:\/\/www.pedata.cn\/ep\/\d+\.html'}}, {"url": 1, "_id": 0})
        except Exception, e:
            logging.error("insert data fail:%s" % e.message)


def req_to_string(req):
    str_req = ""
    try:
        tMemory_b = TMemoryBuffer()
        tBinaryProtocol_b = TBinaryProtocol(tMemory_b)
        req.write(tBinaryProtocol_b)
        str_req = tMemory_b.getvalue()
    except:
        self.log.error('crawled_failt\terror:%s' % (traceback.format_exc()))
    return str_req


def construct_downloader_req(urls):
    i = 0

    for url in urls:
        i = i + 1
        print i
        # print url
        download_req = DownLoadReq()
        download_req.method = 'get'
        download_req.url = url['url']
        download_req.http_header = {}
        download_req.session_commit = SessionCommit()
        download_req.session_commit.refer_url = ""
        download_req.session_commit.identifying_code_url = ""
        download_req.session_commit.identifying_code_check_url = ""
        download_req.session_commit.check_body = ""
        download_req.session_commit.check_body_not = ""
        download_req.session_commit.session_msg = {}
        download_req.session_commit.need_identifying = False
        download_req.session_commit.need_identifying = False
        scheduler_info = {}
        download_req.scheduler = json.dumps(scheduler_info)
        download_req.use_proxy = False
        download_req.src_type = "seed"
        if download_req.url is not None:
            download_req.download_type = 'simple'
            priority_key = str(time.time())
            index_queue.put((priority_key, download_req))


def deliver_req():
    out_beanstalk = PyBeanstalk('172.18.180.223', 11300)
    while True:
        try:
            priority, reqs = index_queue.get_nowait()
            req_str = req_to_string(reqs)
            out_beanstalk.put('online_download_req', req_str)
        except Empty:
            continue
            time.sleep(6)


def main():
    threads = []
    db = MongoDB('172.16.215.2', 40042, 'crawl_merge_linkattr', 'pedata.cn')
    urls = db.find()
    # urls = list(urls)
    # exit(0)
    t1 = threading.Thread(target=construct_downloader_req, args=(urls,))
    threads.append(t1)
    t2 = threading.Thread(target=deliver_req)
    threads.append(t2)

    for t in threads:
        t.setDaemon(True)
        t.start()

    for t in threads:
        t.join()


if __name__ == '__main__':
    main()










