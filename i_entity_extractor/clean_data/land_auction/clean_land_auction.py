#coding=utf8

import traceback
import sys
sys.path.append('..')
sys.path.append('../..')
sys.path.append('../../..')
from multiprocessing import Pool, Process, Queue
import multiprocessing
from i_entity_extractor.common_parser_lib.mongo import MongDb
from i_entity_extractor.common_parser_lib.province_parser import ProvinceParser
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


province_city = '../../../i_entity_extractor/dict/province_city.conf'
phone_city = '../../../i_entity_extractor/dict/phonenum_city.conf'
region_city = '../../../i_entity_extractor/dict/region_city.conf'
city_city = '../../../i_entity_extractor/dict/city.conf'


class CleanLandAuction:
    def __init__(self):
        self.db_insert = MongDb(mongo_conf_insert['host'], mongo_conf_insert['port'], mongo_conf_insert['final_db'],
                           mongo_conf_insert['username'],
                           mongo_conf_insert['password'])
        self.db = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
                         mongo_conf['username'],
                         mongo_conf['password'])
        self.province_parser = ProvinceParser(province_city, phone_city, region_city, city_city)
        self.sourceTable = 'land_auction'
        self.targetTable = 'land_auction'

    def do_clean(self, entity_data):

        # 省分和城市
        text = entity_data.get('approved_unit', '') + entity_data.get('district', '')
        province = entity_data.get('province', '')
        city = entity_data.get('city', '')
        if not province:
            province = self.province_parser.get_province(text, 1)
        if not city:
            city = self.province_parser.get_region(text, 1)
        if province in city:
            city = city.replace(province, '')
        entity_data['province'] = province
        entity_data['city'] = city

        q_data.put(entity_data)

    def insert_info_batch(self, table, lst, is_order=False, insert=False):
        if lst != None and len(lst) == 0:
            return
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


obj = CleanLandAuction()


def proxy(extract_item):
    obj.do_clean(extract_item)


if __name__ == "__main__":
    import time
    print "start"
    begin_time = time.time()
    cursor = obj.db.select(obj.sourceTable, {})
    manager = multiprocessing.Manager()
    q_data = manager.Queue()
    q_lock = manager.Lock()
    pool = Pool(4)

    data_list = []
    insert_data_list = []
    num = 0
    del_num = 0

    for item in cursor:
        try:
            num += 1
            site = item['_src'][0]['site']
            if site == 'land.fang.com':
                obj.db.delete(obj.targetTable, item)
                del_num += 1
                continue

            data_list.append(item)

            if len(data_list) >= 1000:
                pool.map(proxy, data_list)
                del data_list[:]
                for i in range(q_data.qsize()):
                    insert_data_list.append(q_data.get())
                obj.insert_info_batch(obj.targetTable, insert_data_list)
                del insert_data_list[:]

            if num % 10000 == 0:
                print "sum_num: %d/%d, time_cost: %f" % (del_num, num, time.time() - begin_time)

        except Exception as e:
            print traceback.format_exc()

    pool.map(proxy, data_list)
    del data_list[:]
    for i in range(q_data.qsize()):
        insert_data_list.append(q_data.get())
    obj.insert_info_batch(obj.targetTable, insert_data_list)
    del insert_data_list[:]

    print "del_sum/sum_num: %d/%d, time_cost: %f" % (del_num, num, time.time() - begin_time)
    print 'clean finished!!!'
