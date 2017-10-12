# -*- coding: utf-8 -*-
import copy
import json

import pymongo
import time

from common.log import log
from common.topic_manager import TopicManager
from pymongo.errors import DuplicateKeyError

from common.validate_manager import ValidateManager
from i_data_saver.data_statistic import DataStatistics
from i_util.global_defs import MetaFields, DOWNLOAD_TIME
from i_util.tools import get_record_id_new
from bdp.i_crawler.i_data_saver.ttypes import DataSaverRsp


def value_cmp(a, b, cmp_func = cmp):
    return cmp_func(a, b)


class MongoUpdator(object):
    def __init__(self, conf, table_names):
        self.conf = conf
        self.mongo_client = pymongo.MongoClient(
            host=conf['host'],
            port=conf['port']
        )
        self.db = self.mongo_client[conf['db']]
        if conf['username'] != '':
            self.db.authenticate(conf['username'],
                                 conf['password'])
        self.init_tables(table_names)

    def fetch(self, table_name, record_id):
        colletion = self.db[table_name]
        existed_doc = colletion.find_one(record_id)
        return existed_doc

    def merge_from_existed_doc(self, existed_doc, doc):
        merged_doc = copy.deepcopy(existed_doc)
        doc_change_flag = False
        for k, v in doc.items():
            # 忽略下横线开头的字段
            if k.startswith("_"):
                continue
            # 如果是新发现的字段,则赋值
            if merged_doc.get(k, None) is None:
                merged_doc[k] = v
                doc_change_flag = True
            # 如果新抽取数据有值,则替换
            elif v:
                merged_doc[k] = v
                doc_change_flag = True
        if doc_change_flag:
            if MetaFields.SRC in merged_doc and MetaFields.SRC in doc:
                src_list = merged_doc[MetaFields.SRC]
                src_existed = False
                for src in src_list:
                    if doc[MetaFields.SRC][0][MetaFields.SITE] == src[MetaFields.SITE]:
                        src_existed = True
                        src[DOWNLOAD_TIME] = int(time.time())
                        break

                if not src_existed:
                    # 更新下最新下载时间，便于知道是何时入库的
                    doc[MetaFields.SRC][0][DOWNLOAD_TIME] = int(time.time())
                    merged_doc[MetaFields.SRC].extend(doc[MetaFields.SRC])
            else:
                # 如果doc里面有src 则合并
                src_list = doc.get(MetaFields.SRC, [])
                if isinstance(src_list, list) and len(src_list) > 0:
                    src_list[0][DOWNLOAD_TIME] = int(time.time())
                    merged_doc[MetaFields.SRC] = src_list

        return merged_doc, doc_change_flag

    #TODO 将入库切出,增加多源融
    def merge(self, table_name, doc):
        collection = self.db[table_name]
        update_doc = doc
        existed_doc = collection.find_one({MetaFields.RECORD_ID: doc[MetaFields.RECORD_ID]})
        doc_change_flag = True
        if existed_doc:
            update_doc, doc_change_flag = self.merge_from_existed_doc(existed_doc, doc)
        if MetaFields.IN_TIME not in update_doc:
            update_doc[MetaFields.IN_TIME] = time.strftime("%Y-%m-%d %H:%M:%S")
        update_doc[MetaFields.UTIME] = time.strftime("%Y-%m-%d %H:%M:%S")
        return update_doc, doc_change_flag

    def sink(self, table_name, update_doc):
            # Workaround for mongo bug #SERVER-14322
        # Do client-side retries on DuplicateKeyError
        retry_count = 1000  # Should be more than enough
        succ = False
        update_result = None
        collection = self.db[table_name]
        while not succ and retry_count > 0:
            retry_count -= 1
            try:
                update_result = collection.update_one(
                    filter={
                        MetaFields.RECORD_ID: update_doc[MetaFields.RECORD_ID]
                    },
                    update={
                        '$set': update_doc
                    },
                    upsert=True
                )
            except DuplicateKeyError, e:
                # if upsert fail with DuplicateKeyError, just retry.
                # if other exceptions occur, no retry
                pass
            else:
                succ = True
        if update_result:
            return update_result.matched_count
        else:
            return -1

    def init_tables(self, target_table_names):
        current_tables = self.list_tables()
        for table_name in target_table_names:
            if table_name not in current_tables:
                self.create_table(table_name)

    def list_tables(self):
        return self.db.collection_names(False)

    def create_table(self, name):
        collection = self.db[name]
        collection.create_index(
            MetaFields.RECORD_ID,
            background=True,
            unique=True
        )
        collection.create_index(
            MetaFields.IN_TIME,
            background=True,
            unique=False
        )
        collection.create_index(
            MetaFields.UTIME,
            background=True,
            unique=False
        )

class DataSaver(object):
    def __init__(self, conf):
        self.topic_manager = TopicManager(conf)
        self.logger = log
        self.validate_manager = ValidateManager(self.topic_manager, conf, 'all')
        self.data_sink = MongoUpdator(conf['data_sink']['mongodb'], self.topic_manager.get_table_names())
        ds_conf = copy.deepcopy(conf['data_sink']['mongodb'])
        ds_conf['collection'] = "__STATISTICS__"
        self.data_stastics = DataStatistics(ds_conf)
        self.data_stastics.start()

    def check_data(self, j, save=False):
        topic_id = j['topic_id']
        data = j['data']
        self.logger.info(
            "haizhi-check{} topic_id:{}, record_id:{}".format(data['_src'][0]['url'], topic_id, data.get("_record_id")))
        for k in data.keys():
            if k.startswith("_") and k not in MetaFields.ALLOW_META_FIELDS:
                data.pop(k)
        self.validate_manager.validate(data, topic_id, None, validator_names=['pk'])
        if MetaFields.HAS_SCHEMA_ERROR not in data:
            data[MetaFields.RECORD_ID] = get_record_id_new(self.topic_manager.get_primary_keys_by_id(topic_id),
                                                           data['_src'][0]['url'], data)
            self.validate_manager.validate(data, topic_id, None, validator_names=['jsonschema'])
            if MetaFields.HAS_SCHEMA_ERROR in data:
                self.logger.error(
                    "NOT_MATCH_SCHEMA: topic_id = {}\turl = {}\terror_info={}".format(topic_id, data['_src'][0]['url'],
                                                                                     str(data[MetaFields.HAS_SCHEMA_ERROR])))
        else:
            self.logger.error(
                "NOT_COMPLETE_PK, topic_id = {}\turl = {}\tlack_pk={}".format(topic_id, data['_src'][0]['url'],
                                                                              str(data[MetaFields.HAS_SCHEMA_ERROR])))
        if save:
            if MetaFields.HAS_SCHEMA_ERROR in data:
                self.data_stastics.inc_failure(topic_id)
                self.logger.error("haizhi- url = {} schema错误".format(data['_src'][0]['url']))
            else:
                table_name = self.topic_manager.get_table_name_by_id(topic_id)
                update_doc, change_flag = self.data_sink.merge(table_name, data)
                self.logger.debug("put to succ table table_name = %s" % table_name)
                #TODO 以后会将merge操作抽离put操作,以便于加入其他合并策略,目前还不清楚合并策略怎么处理,先简单上一版吧
                if change_flag:
                    rets = self.data_sink.sink(table_name, update_doc=update_doc)
                    if rets == 0:
                        self.data_stastics.inc_success_insert(topic_id)
                        self.logger.info("haizhi- url = {} 插入操作,标识为{}".format(data['_src'][0]['url'],
                                                                              update_doc[MetaFields.RECORD_ID]))
                    elif rets > 0:
                        self.data_stastics.inc_success_update(topic_id)
                        self.logger.info("haizhi- url = {} 更新操作,标识符为{}".format(data['_src'][0]['url'],
                                                                               update_doc[MetaFields.RECORD_ID]))
                    else:
                        self.data_stastics.inc_failure(topic_id)
                        self.logger.info("haizhi- url = {} 数据库操作失败".format(data['_src'][0]['url']))
                    self.logger.info(
                        "haizhi-{}数据更新Sink topic_id:{}, record_id:{}, result:{}".format(data['_src'][0]['url'],
                                                                                        topic_id, update_doc[
                                                                                            MetaFields.RECORD_ID],
                                                                                        rets))
                else:
                    self.logger.info("Skip sink topic_id:{}, record_id:{}, data has no change!".format(topic_id, update_doc[MetaFields.RECORD_ID]))
        return DataSaverRsp(status=0, message=None, data=json.dumps(data))

    def reload(self, topic_id):
        try:
            self.topic_manager.reload(topic_id)
            self.validate_manager.reload(topic_id)
            table_name = self.topic_manager.get_table_name_by_id(topic_id)
            if table_name:
                self.data_sink.create_table(table_name)
            return DataSaverRsp(status=0,
                                message="success",
                                data=str(topic_id))
        except Exception, e:
            return DataSaverRsp(status=1,
                                message=str(e),
                                data=None)

    def get_schema(self, topic_id):
        try:
            schema_data = self.topic_manager.topic_dict[topic_id]['schema']
            return DataSaverRsp(
                status=0,
                message='OK',
                data=schema_data.encode('utf8')
            )
        except Exception, e:
            return DataSaverRsp(
                status=1,
                message=repr(e),
                data=None
            )
