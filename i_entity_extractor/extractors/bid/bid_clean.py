# encoding=utf-8
import sys
import time
import traceback

import copy

sys.path.append('..')
sys.path.append('../..')
sys.path.append('../../..')

from common_parser_lib.parser_tool import parser_tool
from multiprocessing import Pool, Process, Queue
import multiprocessing
from common_parser_lib.mongo import MongDb
from pymongo.errors import BulkWriteError

reload(sys)
sys.setdefaultencoding('utf8')
manager = multiprocessing.Manager()
q_data = manager.Queue()
q_lock = manager.Lock()

mongo_conf37 = {
    'host': '101.201.102.37',
    'port': 28019,
    'final_db': 'final_data',
    'username': None,
    'password': None,
}

mongo_conf = {
    'host': '101.201.102.37',
    'port': 28019,
    'final_db': 'final_data',
    'username': None,
    'password': None,
}
db37 = MongDb(mongo_conf37['host'], mongo_conf37['port'], mongo_conf37['final_db'],
              mongo_conf37['username'],
              mongo_conf37['password'])
db = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
            mongo_conf['username'],
            mongo_conf['password'])

sourceTable = 'bid_detail'
targetTable = 'bid_detail'


def _insert_info_batch(table, lst, is_order=True, insert=True):
    if lst != None and len(lst) == 0: return
    dbtemp = db37.db[table]
    bulk = dbtemp.initialize_ordered_bulk_op() if is_order else dbtemp.initialize_unordered_bulk_op()
    for item in lst:
        id = item.get('_id')
        if insert:
            bulk.insert(item)
        else:
            print "id:", id
            _id = item.pop('_id')
            bulk.find({'_id': _id}).update({'$set': item})
    try:
        bulk.execute({'w': 0})
        print ('insert_logs:' + str(len(lst)))
    except BulkWriteError as bwe:
        print bwe.details


def multi_do_clean():
    begin_time = time.time()
    cursor = db.select(sourceTable, {})
    pool = Pool(8)
    result_list = []
    insert_data_list = []
    num = 0

    for item in cursor:
        try:
            num += 1
            content = item.get('bid_content')
            if not content:
                continue
            result_list.append(item)

            if len(result_list) == 10:
                ret = pool.map(update_data, result_list)
                print "update_num:%s \t time_cost:%s" % (len(result_list), (time.time() - begin_time))
                result_list = []
                for i in range(q_data.qsize()):
                    insert_data_list.append(q_data.get())
                _insert_info_batch(targetTable, insert_data_list, insert=False)
                del insert_data_list[:]

        except:
            print traceback.format_exc()

    for i in range(q_data.qsize()):
        insert_data_list.append(q_data.get())
    _insert_info_batch(targetTable, insert_data_list, insert=False)
    del insert_data_list[:]
    print "finish_num:%s \t time_cost:%s" % (num, (time.time() - begin_time))


def update_data(item):
    try:
        model = copy.deepcopy(item)
        src_url = item.get("_src")[0]['url']
        content = item.get("bid_content", "")
        title = item.get("title", "")
        id = item.get("_id", "")

        if not content:
            return
        city = parser_tool.bid_region_parser.do_parser(content)
        closing_time = parser_tool.bid_close_time_parser.do_parser(content)
        (money_list, budget_money_list) = parser_tool.bid_money_parser.do_parser(content)
        result_data = parser_tool.bid_company_parser.do_parser(content, title)
        if not city and result_data.get("zhaobiao"):
            city = parser_tool.province_parser.get_region(result_data.get("zhaobiao"))
        model["city"] = city
        model["closing_time"] = closing_time
        model["bid_money_list"] = money_list
        model["bid_budget_list"] = budget_money_list
        model["bid_money"] = ','.join(money_list)
        model["bid_budget"] = ','.join(budget_money_list)
        model["win_bid_company"] = result_data.get("zhongbiao")
        model["candicate_win_bid_company"] = result_data.get("houxuan_zhongbiao")
        model["public_bid_company"] = [result_data.get("zhaobiao")]
        model["agent"] = [result_data.get("agent")]

        for key, value in model.items():
            if key in ['bid_money', 'bid_budget']:
                continue
            if not value:
                model.pop(key)
        q_data.put(model)


    except:
        print traceback.format_exc()


if __name__ == '__main__':
    multi_do_clean()
