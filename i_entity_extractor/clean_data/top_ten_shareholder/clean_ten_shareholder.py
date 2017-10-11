#coding=utf8

import traceback
import sys
sys.path.append('..')
sys.path.append('../..')
sys.path.append('../../..')
from i_entity_extractor.common_parser_lib.mongo import MongDb
import json
from multiprocessing import Pool, Process, Queue
from bson.objectid import ObjectId
import multiprocessing
from i_entity_extractor.common_parser_lib import toolsutil

mongo_conf_insert = {
    'host': '172.16.215.2',
    'port': 40042,
    'final_db': 'final_data',
    'username': "work",
    'password': "haizhi",
}

mongo_conf = {
    'host': '172.16.215.2',
    'port': 40042,
    'final_db': 'final_data',
    'username': "work",
    'password': "haizhi",
}

class CleanTenShareholder:
    def __init__(self):
        self.db_insert = MongDb(mongo_conf_insert['host'], mongo_conf_insert['port'], mongo_conf_insert['final_db'],
                           mongo_conf_insert['username'],
                           mongo_conf_insert['password'])
        self.db = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
                         mongo_conf['username'],
                         mongo_conf['password'])
        self.sourceTable = 'top_ten_shareholder'
        self.targetTable = 'top_ten_shareholder'

    def remove_duplicate(self, dict_list):
        seen = set()
        new_dict_list = []
        for dict in dict_list:
            t_dict = {'per_capita_float': dict['per_capita_float'], 'year_quarter': dict['year_quarter']}
            t_tup = tuple(t_dict.items())
            if t_tup not in seen:
                seen.add(t_tup)
                new_dict_list.append(dict)
        return new_dict_list

    def do_clean(self,item):

        shareholder_number_table = item.get("shareholder_number_table")
        if shareholder_number_table:
            new_shareholder_number_table = self.remove_duplicate(shareholder_number_table)
            item["shareholder_number_table"] = new_shareholder_number_table
            #print json.dumps(new_shareholder_number_table,ensure_ascii=False,encoding='utf8')
            q_data.put(item)








    def _insert_info_batch(self, table, lst, is_order=False, insert=False):
        if lst != None and len(lst) == 0: return
        dbtemp = self.db_insert.db[table]
        bulk = dbtemp.initialize_ordered_bulk_op() if is_order else dbtemp.initialize_unordered_bulk_op()
        for item in lst:
            _record_id = item.get("_record_id","")
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





obj = CleanTenShareholder()
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
            # new_item = obj.do_clean(item)
            # insert_data_list.append(new_item)
            #
            # if len(insert_data_list) >= 100:
            #     obj._insert_info_batch(obj.targetTable,insert_data_list)
            #     del insert_data_list[:]
            data_list.append(item)
            if len(data_list)>=500:
                ret = pool.map(prox,data_list)
                del data_list[:]
                for i in range(q_data.qsize()):
                    insert_data_list.append(q_data.get())
                obj._insert_info_batch(obj.targetTable, insert_data_list)
                del insert_data_list[:]

            if num%1000==0:
                print "sum_num:",num, len(data_list),"time_cost:", time.time() - begin_time

            break


        except Exception as e:
            print traceback.format_exc()

    for i in range(q_data.qsize()):
        insert_data_list.append(q_data.get())
    obj._insert_info_batch(obj.targetTable, insert_data_list)
    del insert_data_list[:]

    print "time_cost:",time.time() - begin_time

    url = "http://www.landchina.com/default.aspx?tabid=386&comname=default&wmguid=75c72564-ffd9-426a-954b-8ac2df0903b7&recorderguid=43d2dda6-1a14-448c-b506-8e85cbb4a3bc"

    from i_util import tools

    print tools.get_md5('|'+ url)