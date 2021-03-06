#coding=utf8

import traceback
import sys
sys.path.append('..')
sys.path.append('../..')
sys.path.append('../../..')
from i_entity_extractor.common_parser_lib.mongo import MongDb
import re
from multiprocessing import Pool, Process, Queue
from i_util import tools
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



class CleanJudgeProcess:
    def __init__(self):
        self.db_insert = MongDb(mongo_conf_insert['host'], mongo_conf_insert['port'], mongo_conf_insert['final_db'],
                           mongo_conf_insert['username'],
                           mongo_conf_insert['password'])
        self.db = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
                         mongo_conf['username'],
                         mongo_conf['password'])
        self.sourceTable = 'judge_process'
        self.targetTable = 'judge_process'
        self.time_regex  = re.compile("\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")

    def deal_date(self,date_str):
        norm_date = toolsutil.norm_date_time(date_str)
        if not norm_date:
            return date_str

        if self.time_regex.findall(norm_date):
            return norm_date.split()[0]
        else:
            return date_str




    def do_clean(self,item):

        primary_key_str = '|' + item.get("court","") + '|' + item.get("litigants","") + '|' + item.get("case_id","")
        new_record_id   = tools.get_md5(primary_key_str)
        old_record_id   = item.get("_record_id","")
        print("old:{},new:{}".format(old_record_id,new_record_id))
        if old_record_id != new_record_id:
            print ("okold:{},new:{}".format(old_record_id,new_record_id))
            item["_record_id"] = new_record_id
            q_data.put(item)




    def _insert_info_batch(self, table, lst, is_order=False, insert=False):
        if lst != None and len(lst) == 0: return
        dbtemp = self.db_insert.db[table]
        bulk = dbtemp.initialize_ordered_bulk_op() if is_order else dbtemp.initialize_unordered_bulk_op()
        for item in lst:
            _id = item.get("_id","")
            print _id
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





obj = CleanJudgeProcess()
def prox(item):
    obj.do_clean(item)


if __name__ == "__main__":
    import time
    print "start"
    begin_time = time.time()
    cursor = obj.db.select(obj.sourceTable, {})
    manager = multiprocessing.Manager()
    q_data = manager.Queue()
    q_lock = manager.Lock()
    num  = 0
    data_list = []
    insert_data_list = []
    pool = Pool(4)
    for item in cursor:
        try:
            num += 1

            data_list.append(item)
            if len(data_list)>=1000:
                ret = pool.map(prox,data_list)
                del data_list[:]
                for i in range(q_data.qsize()):
                    insert_data_list.append(q_data.get())
                obj._insert_info_batch(obj.targetTable, insert_data_list)
                del insert_data_list[:]

            if num%10000==0:
                print "sum_num:",num, len(data_list),"time_cost:", time.time() - begin_time

        except Exception as e:
            print traceback.format_exc()

    ret = pool.map(prox, data_list)
    del data_list[:]
    for i in range(q_data.qsize()):
        insert_data_list.append(q_data.get())
    obj._insert_info_batch(obj.targetTable, insert_data_list)
    del insert_data_list[:]

    print "time_cost:",time.time() - begin_time
