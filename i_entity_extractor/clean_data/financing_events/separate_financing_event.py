#coding=utf8

import traceback
import sys
sys.path.append('..')
sys.path.append('../..')
sys.path.append('../../..')
sys.path.append('../../../..')
from crawler.i_entity_extractor.common_parser_lib.mongo import MongDb
from crawler.i_entity_extractor.common_parser_lib import toolsutil

mongo_conf = {
    'host': '172.16.215.16',
    'port': 40042,
    'final_db': 'app_data',
    'username': "work",
    'password': "haizhi",
}


class SeparateInvestmentEvent:
    def __init__(self):
        self.db_insert = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
                                mongo_conf['username'],
                                mongo_conf['password'])
        self.db = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
                         mongo_conf['username'],
                         mongo_conf['password'])
        self.sourceTable = 'financing_events'
        self.targetTable = 'financing_events_innotree'

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

obj = SeparateInvestmentEvent()

if __name__ == "__main__":
    import time
    print "start"
    begin_time = time.time()
    cursor = obj.db.select(obj.sourceTable, {})
    num = 0
    data_list = []
    insert_data_list = []

    for item in cursor:
        try:
            num += 1
            site = item.get('_src', [{}])[0].get('site')
            if site == 'www.innotree.cn':
                data_list.append(item)
            if len(data_list) >= 1000:
                obj.insert_info_batch(obj.targetTable, data_list, insert=True)
                del data_list[:]

            if num % 1000 == 0:
                print "sum_num:",num, len(data_list),"time_cost:", time.time() - begin_time

        except Exception as e:
            print traceback.format_exc()

    obj.insert_info_batch(obj.targetTable, data_list, insert=True)
    del data_list[:]
    print "time_cost:",time.time() - begin_time
    print 'remove finished!'
    print 'start delete ...'

    cursor_delete = obj.db.select(obj.targetTable, {})
    delete_num = 0
    for item in cursor_delete:
        delete_num += 1
        _id = item.get('_id', '')
        obj.db.db[obj.sourceTable].delete_one({'_id': _id}, )
    print 'separate finished, delete num %d' % delete_num
