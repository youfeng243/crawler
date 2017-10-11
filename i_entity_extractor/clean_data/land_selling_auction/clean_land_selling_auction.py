#coding=utf8

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


class CleanLandSellingAuction:
    def __init__(self):
        self.db_insert = MongDb(mongo_conf_insert['host'], mongo_conf_insert['port'], mongo_conf_insert['final_db'],
                           mongo_conf_insert['username'],
                           mongo_conf_insert['password'])
        self.db = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
                         mongo_conf['username'],
                         mongo_conf['password'])
        self.sourceTable = 'land_selling_auction'
        self.targetTable = 'land_selling_auction'
        self.time_regex  = re.compile("\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")

    def deal_date(self,date_str):
        norm_date = toolsutil.norm_date_time(date_str)
        if not norm_date:
            return date_str

        if self.time_regex.findall(norm_date) == norm_date:
            return norm_date.split[0]
        else:
            return date_str

    def do_clean(self,item):
        # if not item.get("code") and len(item.keys())<20:
        #     land_basic_info = item.get("land_basic_info",[])
        #     for land in land_basic_info:
        #         for key,value in land.items():
        #             item[key] = value
        #         break
        #
        #
        #     q_data.put(item)

        # 面积单位
        acreage = item.get('acreage', '')
        acreage_unit = re.sub(u'\d|\.', '', acreage)
        item['acreage_unites'] = acreage_unit

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





obj = CleanLandSellingAuction()
def prox(item):
    obj.do_clean(item)


if __name__ == "__main__":
    import time
    print "start"
    begin_time = time.time()
    cursor = obj.db.select(obj.sourceTable, {"code":{"$eq":None}})
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
