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

mongo_conf_company = {
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
        self.sourceTable = 'acquirer_event'
        self.targetTable = 'acquirer_event'
        self.company_table = 'company_information_pedata'
        self.time_regex = re.compile("\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
        self.company_data=self._init_data()

    def _init_data(self):
        print "company_data loading..."
        company_data = {}
        cursor_company = self.db_company.db[self.company_table].find({}).batch_size(1000)
        for item0 in cursor_company:
            company_name = item0.get('company_short_name_cn')
            if company_name:
                company_data[company_name] = item0.get(u'company_name', u'')
        print "company_data load completed"
        return company_data

    def deal_full_name(self, short_name):
        """公司全称"""
        full_company_name = self.company_data.get(short_name, u'')
        if full_company_name != u'':
            full_company_name = str(full_company_name).strip()
        return full_company_name

    def deal_date(self, src_date):
        norm_date_time = toolsutil.norm_date_time(src_date)

        if src_date != norm_date_time:
            result_date = norm_date_time.split()[0]
        else:
            if self.time_regex.findall(norm_date_time):
                result_date = src_date.split()[0]
            else:
                return src_date
        return result_date

    def do_clean(self, item):
        begin_date = item.get("begin_date", "")
        end_date = item.get("end_date", "")
        acquirered_short_name = item.get('acquirered_short_name', '')
        acquirered_full_name = item.get('acquirered_full_name', '')
        acquirer_full_name = item.get('acquirer_full_name', '')
        acquirer_short_name = item.get('acquirer_short_name', '')

        if not acquirered_full_name:
            item['acquirered_full_name'] = self.deal_full_name(acquirered_short_name)

        if not acquirer_full_name:
            item['acquirer_full_name'] = self.deal_full_name(acquirer_short_name)

        if begin_date:
            item["begin_date"] = self.deal_date(begin_date)
        else:
            item["begin_date"] = ""
        if end_date:
            item["end_date"] = self.deal_date(end_date)
        else:
            item["end_date"] = ""

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
            if len(data_list) >= 1000:
                ret = pool.map(prox, data_list)
                del data_list[:]
                for i in range(q_data.qsize()):
                    insert_data_list.append(q_data.get())
                obj.insert_info_batch(obj.targetTable, insert_data_list)
                del insert_data_list[:]

            if num % 10000 == 0:
                print "sum_num:", num, len(data_list), "time_cost:", time.time() - begin_time

        except Exception as e:
            print traceback.format_exc()

    ret = pool.map(prox, data_list)
    del data_list[:]
    for i in range(q_data.qsize()):
        insert_data_list.append(q_data.get())
    obj.insert_info_batch(obj.targetTable, insert_data_list)
    del insert_data_list[:]

    print "time_cost:", time.time() - begin_time
    print 'clean finished!!!'
