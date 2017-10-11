# coding=utf8

import traceback
import sys
import time
import copy
import json
import hashlib
from datetime import datetime

sys.path.append('..')
sys.path.append('../..')
sys.path.append('../../..')
sys.path.append('../../../..')

from crawler.i_entity_extractor.common_parser_lib.mongo import MongDb

mongo_conf = {
    'host': '172.16.215.16',
    'port': 40042,
    'final_db': 'app_data',
    'username': "work",
    'password': "haizhi",
}


class RemoveRepeatedData:
    def __init__(self, table, keys):
        self.db_insert = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
                                mongo_conf['username'],
                                mongo_conf['password'])
        self.db = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
                         mongo_conf['username'],
                         mongo_conf['password'])
        self.table = table
        self.keys = keys

    def get_repeated_data(self):
        print 'loading data......'
        begin_time = time.time()
        cursor = self.db.select(obj.table, {})
        num = 0
        data_dict = {}
        for item in cursor:
            content = ''
            num += 1
            if num % 500 == 0:
                print 'loading num: %d, cost time: %f' % (num, time.time() - begin_time)
            for k in self.keys:
                content += json.dumps(item.get(k, ''))

            tmp = {'_record_id': item['_record_id'], '_utime': item['_utime']}
            content_md5 = hashlib.md5(content.encode('utf8')).hexdigest()
            if content_md5 in data_dict.keys():
                data_dict[content_md5].append(tmp)
            else:
                data_dict[content_md5] = [tmp]
        print 'loading num: ', num
        print 'finished loading data'
        return data_dict

    def get_older_data(self):
        data_dict = self.get_repeated_data()
        multi_data = []
        for key in data_dict.keys():
            value = data_dict[key]
            if len(value) > 2:
                print '-' * 30
                print key, len(value), len(multi_data)
                print value

                tmp_v = value[0]
                utime = datetime.strptime(tmp_v['_utime'], '%Y-%m-%d %H:%M:%S')

                for v in value[1:]:
                    utime1 = datetime.strptime(v['_utime'], '%Y-%m-%d %H:%M:%S')
                    if utime1 < utime:
                        multi_data.append(v)
                    else:
                        multi_data.append(tmp_v)
                        tmp_v = copy.deepcopy(v)
                        utime = utime1
                    print multi_data[-1]
        return multi_data

    def remove(self):
        multi_data = self.get_older_data()
        for item in multi_data:
            self.db.delete(self.table, item)
            print item.get('_record_id', '')
        print 'finished, remove %d' % len(multi_data)


if __name__ == "__main__":
    print "start"
    obj = RemoveRepeatedData('acquirer_event', ['describe', 'acquirer_full_name', 'acquirered_full_name', 'acquirer_short_name', 'acquirered_short_name'])
    obj.get_older_data()
