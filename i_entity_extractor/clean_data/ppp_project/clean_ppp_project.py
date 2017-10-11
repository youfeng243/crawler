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



class CleanPppProject:
    def __init__(self):
        self.db_insert = MongDb(mongo_conf_insert['host'], mongo_conf_insert['port'], mongo_conf_insert['final_db'],
                           mongo_conf_insert['username'],
                           mongo_conf_insert['password'])
        self.db = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
                         mongo_conf['username'],
                         mongo_conf['password'])
        self.sourceTable = 'ppp_project'
        self.targetTable = 'ppp_project'
        self.time_regex = re.compile("\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")

    def deal_time(self,src_time):
        norm_time = toolsutil.norm_date_time(src_time)
        if not norm_time:
            return src_time

        if self.time_regex.findall(norm_time):
            return norm_time
        else:
            return src_time



    def do_clean(self, item):
        item["launch_time"] = self.deal_time(item.get("launch_time",""))
        source_site = item.get("_src")[-1]["site"]

        project_type = u""  # 网站来源和content的关键字来确定project_type
        if source_site == u'www.chinappp.cn':
            content = item.get(u'content', u'')
            item["project_type"] = u'财政部项目' if u'财政部项目' in content else u'发改委项目'
        elif source_site == u'www.cpppc.org':
            item["project_type"] = u"财政部项目"

        q_data.put(item)

    def insert_info_batch(self, table, lst, is_order=False, insert=False):
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


obj = CleanPppProject()


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
    num = 0
    data_list = []
    insert_data_list = []
    pool = Pool(4)
    for item in cursor:
        try:
            num += 1
            # obj.do_clean(item)
            data_list.append(item)
            if len(data_list)>=1000:
                ret = pool.map(prox,data_list)
                del data_list[:]
                for i in range(q_data.qsize()):
                    insert_data_list.append(q_data.get())
                obj.insert_info_batch(obj.targetTable, insert_data_list)
                del insert_data_list[:]

            if num%10000==0:
                print "sum_num:",num, len(data_list),"time_cost:", time.time() - begin_time


        except Exception as e:
            print traceback.format_exc()

    ret = pool.map(prox, data_list)
    del data_list[:]
    for i in range(q_data.qsize()):
        insert_data_list.append(q_data.get())
    obj.insert_info_batch(obj.targetTable, insert_data_list)
    del insert_data_list[:]

    print "time_cost:",time.time() - begin_time
