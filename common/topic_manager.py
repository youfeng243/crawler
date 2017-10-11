# -*- coding: utf-8 -*-
import json
import traceback

from mysql_utils import get_mysql_conn, mysql_fetch, dbrecord_to_dict
from log import log



def parse_pk(topic_def):
    # Historical reson:
    # if pk column is NULL or empty string, pk is url
    if 'primary_keys' in topic_def:
        if topic_def['primary_keys'] is None:
            return []
        elif topic_def['primary_keys'].strip() == '':
            return []
        else:
            pk_json = json.loads(topic_def['primary_keys'])
            return [i for i in pk_json]
    else:
        return []

class TopicManager(object):
    def __init__(self, conf, testing=False):
        self.testing=testing
        self.conf = conf
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
        mysql_conn = get_mysql_conn(self.conf)
        sql = 'select `id`, `name`, `description`, `table_name`, `schema`, `utime`, `ctime`, `primary_keys` from topic'
        if topic_id is not None:
            sql = "%s where id = %d;" % (sql, topic_id)
        db_topic_res = mysql_fetch(mysql_conn, sql, return_dict=True)
        #We requested a topic id but db returned nothing, means this topic(s) is non-existent
        if topic_id and len(db_topic_res) == 0 and topic_dict_copy.has_key(topic_id):
            del topic_dict_copy[topic_id]
        change_topics = []
        for topic_info in db_topic_res:
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
                # topic_info['db_name'] = conf.MONGODB['default_db']

                # 拿到collection
                # db_name = topic_info['db_name']
                table_name = topic_info['table_name']
                topic_info['collection'] = table_name
                # 加入self.topic_dict
                topic_dict_copy[topic_info['id']] = topic_info
                change_topics.append(topic_info['id'])
                log.info('TopicManager loaded: topic.id[%s], topic.name[%s],  table_name[%s] loaded!' %
                    (topic_info['id'], topic_info['name'].encode('utf8'),  table_name.encode('utf8')))
            except Exception as e:
                log.info("failed reload schema {}".format(topic_info['id']))
                log.error(str(traceback.format_exc()))

        #将新字典替换原来的
        for topic_id in change_topics: #parse primary keys ahead of time, speed up query
            topic_dict_copy[topic_id]['__pk_parsed__'] = parse_pk(topic_dict_copy[topic_id])
        self.topic_dict = topic_dict_copy
        log.info('TopicManager loading finished!')

    def get_table_name_by_id(self, topic_id):
        topic_id = long(topic_id)
        if self.topic_dict.has_key(topic_id):
            return self.topic_dict[topic_id]['table_name']
        else:
            return None

    def get_primary_keys_by_id(self, topic_id):
        # __pk_parsed__ is a list, containing columns of pk.
        # url will be used if this is empty
        return self.topic_dict[topic_id]['__pk_parsed__']

    def get_table_names(self):
        return [topic['table_name'] for topic in self.topic_dict.values()]


# topic_manager = TopicManager()  # TopicManager单例
# if __name__ == "__main__":
#     topic_manager.reload(-1)
#     pass