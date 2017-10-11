#coding=utf8

from i_entity_extractor.common_parser_lib.mongo import MongDb
from i_entity_extractor.common_parser_lib import toolsutil
from pymongo.errors import BulkWriteError
import traceback
import time

class Dbprocess:
    def __init__(self,conf):
        mongo_conf = conf.get('mongo',{})
        self.db    = MongDb(mongo_conf.get('host',''), mongo_conf.get('port',''), mongo_conf.get('db_name',''),mongo_conf.get('username',''),mongo_conf.get('passwd',''))
        self.log   = conf.get('log')
        self.collection_names = self.db.db.collection_names()

    def insert_info_batch(self, table, lst, is_order=False, insert=False):
        '''批量插入数据'''
        if lst != None and len(lst) == 0: return
        collection_names = self.db.db.collection_names()
        print table,collection_names
        if table not in collection_names:
            ret = self.create_table(table)
            if not ret:
                return

        try:
            dbtemp  = self.db.db[table]
            bulk    = dbtemp.initialize_ordered_bulk_op() if is_order else dbtemp.initialize_unordered_bulk_op()
            for item in lst:
                _record_id = item.get("_record_id","")
                item["_utime"] = toolsutil.get_now_time()
                self.log.info("_record_id:%s"%_record_id)
                if insert:
                    bulk.insert(item)
                else:
                    _id = item.pop('_id')
                    bulk.find({'_id': _id}).upsert().update({'$set': item})

            bulk.execute({'w': 0})
            self.log.info ('insert_logs:' + str(len(lst)))
        except BulkWriteError as bwe:
            self.log.error("msg:%s\tbwemsg:%s"%(traceback.format_exc(),bwe.details))

    def create_table(self,table_name):
        try:
            if table_name not in self.collection_names:
                self.db.db.create_collection(table_name)
                self.db.db[table_name].ensure_index("_record_id", unique=True)
                self.db.db[table_name].ensure_index("_in_time", unique=False)
                self.db.db[table_name].ensure_index("_utime", unique=False)
                self.log.info("create_table_success,table_name:%s" % table_name)
            else:
                self.log.warning("table_name:%s is exist" % table_name)
            return True
        except Exception as e:
            self.log.info("create_table_error,table_name:%s\tmsg:%s"%(table_name,traceback.format_exc()))
        return False

    def rename_table(self, src_table, target_table):
        try:
            self.collection_names = self.db.db.collection_names()
            if target_table in self.collection_names:
                tmp_table = target_table + "_tmp"
                if tmp_table in self.collection_names:
                    self.db.db.drop_collection(tmp_table)
                self.db.db[target_table].rename(tmp_table)
                self.log.info("target_table_exist,rename_target_table_to:%s\ttarget_table:%s" % (tmp_table,target_table))

            self.db.db[src_table].rename(target_table)
        except Exception as e:
            self.log.error("rename_table_error,src_table:%s\ttarget_table:%s\tmsg:%s"%(src_table,target_table,traceback.format_exc()))
            return False

        self.log.info("rename_table_success,src_table:%s\ttarget_table:%s"%(src_table,target_table))
        return True

    def cur_table_bak(self, src_table, rename_table):
        '''备份当前表'''

        x = time.localtime(time.time())
        time_str = time.strftime('%Y-%m-%d_%H:%M:%S', x)
        #1 将当前表重命名为临时表
        rename_table = rename_table + "_" + time_str
        ret = self.rename_table(src_table,rename_table)
        if not ret:
            return False

        return True

    def reset_table(self, last_table, cur_table):
        '''清洗回滚'''

        x = time.localtime(time.time())
        time_str = time.strftime('%Y-%m-%d_%H:%M:%S', x)
        # 1 将当前表重命名为临时表
        reset_table = cur_table + "_reset_" + time_str

        ret1 = self.rename_table(cur_table, reset_table)
        if not ret1:
            return False

        # 2 将备份表重命名为当前表
        ret2 = self.rename_table(last_table, cur_table)

        if not ret2:
            return False

        return True

    def get_last_table(self,last_table_str):
        '''获取对应主题最近备份表'''
        self.collection_names = self.db.db.collection_names()

        last_table_map = {}
        for collection in self.collection_names:
            if last_table_str in collection:
                tmp_list = collection.split("_")
                if len(tmp_list) > 2:
                    time_str    = tmp_list[-2] + " " + tmp_list[-1]
                    if not toolsutil.re_find_one("\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}",time_str):
                        continue
                    time_second = time.mktime(time.strptime(time_str,'%Y-%m-%d %H:%M:%S'))
                    last_table_map[collection] = time_second

        for key,value in sorted(last_table_map.items(),lambda x,y:cmp(x[1],y[1]),reverse=True):
            return key

        return ""












