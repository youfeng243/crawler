# coding=utf8

import traceback
import sys

sys.path.append('..')
sys.path.append('../..')
sys.path.append('../../..')
from i_entity_extractor.common_parser_lib.mongo import MongDb
import re
from multiprocessing import Pool, Process, Queue
from bson.objectid import ObjectId
import multiprocessing
from i_entity_extractor.common_parser_lib import toolsutil

mongo_conf_insert = {
    'host': 'Crawler-DataServer2',
    'port': 40042,
    'final_db': 'final_data',
    'username': "work",
    'password': "haizhi",
}


mongo_conf = {
    'host': 'Crawler-DataServer2',
    'port': 40042,
    'final_db': 'final_data',
    'username': "work",
    'password': "haizhi",
}

class CleanAcquirerEvent:
    def __init__(self):
        self.db_insert = MongDb(mongo_conf_insert['host'], mongo_conf_insert['port'], mongo_conf_insert['final_db'],
                                mongo_conf_insert['username'],
                                mongo_conf_insert['password'])
        self.db = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
                         mongo_conf['username'],
                         mongo_conf['password'])
        self.db_company = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
                         mongo_conf['username'],
                         mongo_conf['password'])




    def do_clean(self, item):
        src = item.get("_src",[])
        new_src = []
        if len(src) > 0:
            if not src[0].get("site"):
                for each in src:
                    site = each.get("url","").split("//")[-1].split("/")[0]
                    each["site"] = site
                    new_src.append(each)
                item["_src"] = new_src
                new_src = []
                q_data.put(item)

    def insert_info_batch(self, table, lst, is_order=False, insert=False):
        if lst != None and len(lst) == 0: return
        dbtemp = self.db_insert.db[table]
        bulk = dbtemp.initialize_ordered_bulk_op() if is_order else dbtemp.initialize_unordered_bulk_op()
        for item in lst:
            _record_id = item.get("_record_id", "")
            print _record_id
            item["_utime"] = toolsutil.get_now_time()
            if insert:
                bulk.insert(item)
            else:
                _id = item.pop('_id')
                bulk.find({'_id': _id}).update({'$set': item})
        try:
            bulk.execute({'w': 0})
            print ('insert_logs:' + str(len(lst)))

        except:
            print traceback.format_exc()


obj = CleanAcquirerEvent()


def prox(item):
    obj.do_clean(item)


if __name__ == "__main__":
    import time

    print "start"
    begin_time = time.time()

    manager = multiprocessing.Manager()
    q_data = manager.Queue()
    q_lock = manager.Lock()
    num = 0
    data_list = []
    insert_data_list = []
    pool = Pool(4)
    table_list = [
        "penalty", "ssgs_baseinfo", "baidu_news", "baidu_tieba", "bulletin", "enterprise_owing_tax", "judge_process",
        "news", "patent"
    ]

    for src_table in table_list:
        cursor = obj.db.select(src_table, {})
        for item in cursor:
            try:
                num += 1
                # obj.do_clean(item)
                data_list.append(item)
                if len(data_list) >= 1000:
                    ret = pool.map(prox, data_list)
                    del data_list[:]
                    for i in range(q_data.qsize()):
                        insert_data_list.append(q_data.get())
                    obj.insert_info_batch(src_table, insert_data_list)
                    del insert_data_list[:]

                if num % 10000 == 0:
                    print "sum_num:", num, len(data_list),src_table, "time_cost:", time.time() - begin_time

            except Exception as e:
                print traceback.format_exc()

        ret = pool.map(prox, data_list)
        del data_list[:]
        for i in range(q_data.qsize()):
            insert_data_list.append(q_data.get())
        obj.insert_info_batch(src_table, insert_data_list)
        del insert_data_list[:]

        print "finish %s, time_cost:%s:"%(src_table,time.time() - begin_time)
    print 'clean finished!!!'
