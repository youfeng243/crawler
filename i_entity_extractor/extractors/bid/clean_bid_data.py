# encoding=utf-8
import sys
import time
import traceback

import copy
sys.path.append('..')
sys.path.append('../..')
sys.path.append('../../..')

from common_parser_lib.parser_tool import parser_tool
from multiprocessing import Pool
from pymongo import MongoClient

reload(sys)
sys.setdefaultencoding('utf8')

mongo_conf37 = {
    'host': '101.201.102.37',
    'port': 28019,
    'final_db': 'final_data',
    'username': '',
    'password': '',
}

mongo_conf = {
    'host': '172.16.215.1',
    'port': 40042,
    'final_db': 'final_data',
    'username': "readme",
    'password': "readme",
}

src_table = "bid_detail"
target_table = "bid_detail2"

client1 = MongoClient(mongo_conf37['host'], mongo_conf37['port'], connect=False)
db37 = client1[mongo_conf37['final_db']]
# db37.authenticate(mongo_conf37['username'], mongo_conf37['password'])

client2 = MongoClient(mongo_conf['host'], mongo_conf['port'], connect=False)
db = client2[mongo_conf['final_db']]
db.authenticate(mongo_conf['username'], mongo_conf['password'])


def upsert(db, table, data):
    try:
        query = {'_id': data['_id']}
        if not db[table].find_one(query):
            db[table].insert(data)
        else:
            data.pop('_id')
            db[table].update(query, {'$set': data})
    except Exception:
        print traceback.format_exc()


def multi_do_clean():
    begin_time = time.time()
    cursor = db[src_table].find()
    pool = Pool(8)
    result_list = []
    num = 0

    for item in cursor:
        try:
            num += 1
            result_list.append(item)
            if len(result_list) == 100:
                ret = pool.map(update_data, result_list)
                print "update_num:%s \t time_cost:%s" % (len(result_list), (time.time() - begin_time))
                result_list = []

        except:
            print traceback.format_exc()

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

        upsert(db37, target_table, model)
        print "update_success id:%s\turl:%s" % (id, src_url)
    except:
        print traceback.format_exc()


if __name__ == '__main__':
    multi_do_clean()