# coding=utf-8
import traceback, sys
from flask import current_app
from i_manager.background.models_sqlalchemy import Settings
import pymongo


class TopicMongo:
    def __init__(self):
        try:
            self.mongodb_conf = {}
            session = current_app.config['Dsession']()
            records = session.query(Settings).filter_by(item='mongodb').all()
            for record in records:
                self.mongodb_conf['host'] = '172.17.1.119'
                self.mongodb_conf['port'] = record.value['port']
                self.mongodb_conf['database'] = record.value['database']
                break
            session.close()
        except Exception as e:
            self.logger.error(traceback.format_exc())
            return
            # 初始化mongodb
        self.mongo_conn = pymongo.MongoClient(host=self.mongodb_conf['host'],
                                              port=int(self.mongodb_conf['port']))
        self.mongo_db = self.mongo_conn[self.mongodb_conf['database']]

    def get_topic_table_param(self,table_name):
        collection = self.mongo_db[table_name]


