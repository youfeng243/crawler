#coding=utf8

import traceback
import sys
sys.path.append('..')
sys.path.append('../..')
sys.path.append('../../..')
from crawler.i_entity_extractor.common_parser_lib.mongo import MongDb
import re
from multiprocessing import Pool, Process, Queue
from bson.objectid import ObjectId
from crawler.i_entity_extractor.extractors.fygg.fygg_extractor import FyggExtractor
import multiprocessing
from crawler.i_entity_extractor.common_parser_lib import toolsutil

import pytoml
import time
from common.log import log

with open('../../entity.toml', 'rb') as config:
    conf = pytoml.load(config)
log.init_log(conf, console_out=conf['logger']['console'])
conf['log'] = log
topic_id = 33
from crawler.i_entity_extractor.entity_extractor_route import EntityExtractorRoute
import json
route = EntityExtractorRoute(conf)
topic_info = route.all_topics.get(topic_id, None)
begin_time = time.time()
fygg_obj = FyggExtractor(topic_info, log)

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


class CleanFygg:
    def __init__(self):
        self.db_insert = MongDb(mongo_conf_insert['host'], mongo_conf_insert['port'], mongo_conf_insert['final_db'],
                           mongo_conf_insert['username'],
                           mongo_conf_insert['password'])
        self.db = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
                         mongo_conf['username'],
                         mongo_conf['password'])
        self.sourceTable = 'bulletin'
        self.targetTable = 'bulletin'
        self.time_regex  = re.compile("\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")


    def do_clean(self,item):
        if not item.get("bulletin_content"):
            item["bulletin_content"] = item.get("bulletin_type","") + "\t\t" + item.get("norm_bulletin_content","")

        entity_data = fygg_obj.format_extract_data(item,33)
        bulletin_date = entity_data.get("bulletin_date","")
        flag = False
        norm_date_time = toolsutil.norm_date_time(bulletin_date)

        if bulletin_date != norm_date_time:
            entity_data["bulletin_date"] = norm_date_time.split()[0]
            flag = True
        else:
            if self.time_regex.findall(norm_date_time):
                entity_data["bulletin_date"] = bulletin_date.split()[0]
                flag = True



        q_data.put(entity_data)




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





obj = CleanFygg()
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
    #pool = Pool(4)
    for item in cursor:
        try:
            num += 1
            obj.do_clean(item)
            # data_list.append(item)
            # if len(data_list)>=1000:
            #     ret = pool.map(prox,data_list)
            #     del data_list[:]
            #     for i in range(q_data.qsize()):
            #         insert_data_list.append(q_data.get())
            #     obj._insert_info_batch(obj.targetTable, insert_data_list)
            #     del insert_data_list[:]

            if num%10000==0:
                print "sum_num:",num, len(data_list),"time_cost:", time.time() - begin_time
                

        except Exception as e:
            print traceback.format_exc()

    # ret = pool.map(prox, data_list)
    # del data_list[:]
    # for i in range(q_data.qsize()):
    #     insert_data_list.append(q_data.get())
    # obj._insert_info_batch(obj.targetTable, insert_data_list)
    # del insert_data_list[:]

    print "time_cost:",time.time() - begin_time
