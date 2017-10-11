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

mongo_conf_remove = {
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
        self.db_remove = MongDb(mongo_conf_remove['host'], mongo_conf_remove['port'], mongo_conf_remove['final_db'],
                                mongo_conf_remove['username'],
                                mongo_conf_remove['password'])
        self.db = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
                         mongo_conf['username'],
                         mongo_conf['password'])
        self.sourceTable = 'acquirer_event'
        self.targetTable = 'acquirer_event'


    def do_judge(self,item):
        """判断item是否除了系统字段外其他的字段的数据是否全部是空值，若确实空值返回True, 否则返回False"""
        src_keys = ['_id', 'source_site', '_src', '_record_id', '_in_time', '_utime', 'url']
        keys = set(item.keys()).difference(set(src_keys))
        flag = True
        for key in keys:
            if item.get(key, ''):
                flag = False
                break
        q_data.put((flag, item))

    def remove_info_batch(self, table, lst, is_order=False):
        if lst != None and len(lst) == 0: return
        dbtemp = self.db_remove.db[table]
        bulk = dbtemp.initialize_ordered_bulk_op() if is_order else dbtemp.initialize_unordered_bulk_op()
        for item in lst:
            _record_id = item.get("_record_id", "")
            bulk.find({'_record_id': _record_id}).remove_one()
            print _record_id
        try:
            bulk.execute({'w': 0})
            print ('remove_logs:' + str(len(lst)))

        except:
            print traceback.format_exc()


obj = CleanAcquirerEvent()


def prox(item):
    obj.do_judge(item)


if __name__ == "__main__":
    import time

    print "start"
    begin_time = time.time()
    cursor = obj.db.select(obj.sourceTable, {})
    manager = multiprocessing.Manager()
    q_data = manager.Queue()
    q_lock = manager.Lock()
    num = 0
    data_list = []
    remove_data_list = []
    pool = Pool(4)
    for item in cursor:
        try:
            num += 1
            # obj.do_judge(item)
            data_list.append(item)
            if len(data_list) >= 1000:
                ret = pool.map(prox, data_list)
                del data_list[:]
                for i in range(q_data.qsize()):
                    data = q_data.get()
                    if data[0]:
                        remove_data_list.append(data[1])
                obj.remove_info_batch(obj.targetTable, remove_data_list)
                del remove_data_list[:]

            if num % 10000 == 0:
                print "sum_num:", num, len(data_list), "time_cost:", time.time() - begin_time

        except Exception as e:
            print traceback.format_exc()

    ret = pool.map(prox, data_list)
    del data_list[:]
    for i in range(q_data.qsize()):
        data = q_data.get()
        if data[0]:
            remove_data_list.append(data[1])
    obj.remove_info_batch(obj.targetTable, remove_data_list)
    del remove_data_list[:]
    print "time_cost:", time.time() - begin_time
    print 'remove finished!!!'
