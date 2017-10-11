# -*- coding: utf-8 -*-
import json
import threading
import traceback

import common
from common import get_mysql_conn, mongodbs, dbrecord_to_dict, log
from conf import  config as conf


# Topic信息
class TopicManager(object):
    def __init__(self):
        self.topic_dict = {}
        self.reload(-1)

    def reload(self,topic_id):
        #双缓冲方式,减少数据冲突点
        topic_dict_copy = dict(self.topic_dict)
        try:
            #Topic id cannot be 0 ???
            topic_id = int(topic_id)
            if topic_id <= 0:
                topic_id = None
        except Exception as e:
            topic_id = None
        db_topic_res = []
        try:
            mysql_conn = get_mysql_conn()
            sql = 'select `id`, `name`, `description`, `table_name`, `schema`, `utime`, `ctime` from topic'
            if topic_id is not None:
                sql = "%s where id = %d;" % (sql, topic_id)
            cur = mysql_conn.cursor()
            cur.execute(sql)
            db_topic_res = cur.fetchall()
            cur.close()
        except Exception as e:
            log.error(e.message)
        #We requested a topic id but db returned nothing, means this topic(s) is non-existent
        if topic_id and len(db_topic_res) == 0 and topic_dict_copy.has_key(topic_id):
            del topic_dict_copy[topic_id]

        for record in db_topic_res:
            topic_info = dbrecord_to_dict(cur.description, record)
            try:
                # 将record元组转换为dict. (利用cur.description里面的字段名)
                # 解析Schema对象
                try:
                    topic_info['schema_obj'] = json.loads(topic_info['schema'])
                except Exception, e:
                    topic_info['schema_obj'] = None
                    log.warning('schema parse failed[%s], topic[%s] is not writable!' % (str(e), topic_info['name'].encode('utf8')))

                # 给一个默认的db_name.
                # TODO: 将来把db_name也存放在mysql表中, 使得不同的topic可以存在不同的db里面去
                topic_info['db_name'] = conf.MONGODB['default_db']

                # 拿到collection
                db_name = topic_info['db_name']

                table_name = topic_info['table_name']
                collection = mongodbs[db_name][table_name]
                # 确保collection中有_record_id的索引
                # https://docs.mongodb.com/getting-started/python/indexes/
                # ensure_index已经废弃了, create_index也是在index不存在时才进行创建.

                collection.create_index(
                    common.FIELDNAME_RECORD_ID,
                    background = True,
                    unique = True
                )
                collection.create_index(
                    common.FIELDNAME_IN_TIME,
                    background = True,
                    unique = False
                )
                collection.create_index(
                    common.FIELDNAME_UTIME,
                    background = True,
                    unique = False
                )
                topic_info['collection'] = collection

                # 加入self.topic_dict
                topic_dict_copy[topic_info['id']] = topic_info
                log.info('TopicManager loaded: topic.id[%s], topic.name[%s], db_name[%s], table_name[%s] loaded!' %
                    (topic_info['id'], topic_info['name'].encode('utf8'), db_name.encode('utf8'), table_name.encode('utf8')))
            except Exception as e:
                log.info("failed reload schema {}".format(topic_info['id']))
                log.error(str(traceback.format_exc()))

        #将新字典替换原来的
        # old = self.topic_dict
        self.topic_dict = topic_dict_copy
        # old_key = set(old.keys())
        # new_key = set(self.topic_dict.keys())
        # print new_key - old_key
        # print old_key - new_key
        log.info('TopicManager loading finished!')

topic_manager = TopicManager()  # TopicManager单例
if __name__ == "__main__":
    topic_manager.reload(-1)
    pass