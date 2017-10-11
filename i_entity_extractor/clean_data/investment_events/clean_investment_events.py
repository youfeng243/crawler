# coding=utf8

import traceback
import sys
import time
from datetime import datetime
from datetime import timedelta

sys.path.append('..')
sys.path.append('../..')
sys.path.append('../../..')
sys.path.append('../../../..')

from crawler.i_entity_extractor.common_parser_lib.mongo import MongDb


mongo_conf_insert = {
    'host': '172.16.215.16',
    'port': 40042,
    'final_db': 'app_data',
    'username': "work",
    'password': "haizhi",
}

mongo_conf = {
    'host': '172.16.215.16',
    'port': 40042,
    'final_db': 'app_data',
    'username': "work",
    'password': "haizhi",
}


class CleanInvestmentEvents:
    def __init__(self):
        self.db_insert = MongDb(mongo_conf_insert['host'], mongo_conf_insert['port'], mongo_conf_insert['final_db'],
                                mongo_conf_insert['username'],
                                mongo_conf_insert['password'])
        self.db = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
                         mongo_conf['username'],
                         mongo_conf['password'])
        self.sourceTable = 'investment_events'
        self.targetTable = 'investment_events'


obj = CleanInvestmentEvents()


if __name__ == "__main__":
    import time
    import copy
    import hashlib
    print "start"
    begin_time = time.time()
    cursor = obj.db.select(obj.sourceTable, {})
    num = 0
    data_list = []
    data_dict = {}

    for item in cursor:
        num += 1
        content = item.get('profile', '') + item.get('pull_full_name', '') + item.get('title', '')
        if content:
            tmp = {'_record_id': item['_record_id'], '_utime': item['_utime']}
            content_md5 = hashlib.md5(content.encode('utf8')).hexdigest()
            if data_dict.has_key(content_md5):
                data_dict[content_md5].append(tmp)
            else:
                data_dict[content_md5] = [tmp]

        if num % 500 == 0:
            print 'num: ', num

    multi_data = []
    for profile in data_dict.keys():
        value = data_dict[profile]
        if len(value) > 2:
            print '-'*30
            print profile, len(value), len(multi_data)
            print value

            tmp_v = value[0]
            utime = datetime.strptime(tmp_v['_utime'], '%Y-%m-%d %H:%M:%S')

            for v in value[1:]:
                utime1 = datetime.strptime(v['_utime'], '%Y-%m-%d %H:%M:%S')
                if utime1 < utime:
                    multi_data.append(v)
                elif utime1 > utime:
                    multi_data.append(tmp_v)
                    tmp_v = copy.deepcopy(v)
                    utime = utime1
                print multi_data[-1]

    for item in multi_data:
        obj.db.delete(obj.targetTable, item)
        obj.db.db[obj.targetTable].remove(item)
    print 'finished, remove %d' % len(multi_data)
