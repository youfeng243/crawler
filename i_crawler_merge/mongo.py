# coding:utf-8

import sys

sys.path.append('..')

from pymongo import MongoClient
from pymongo import ASCENDING
from pymongo import DESCENDING
import re, json
from bson.regex import Regex
import traceback

class PyMongo(object):
    def __init__(self, host, port, db, user='', passwd=''):
        self.mongo_host = host
        self.mongo_port = port
        self.mongo_db = db
        self.mongo_user = user
        self.mongo_passwd = passwd
        self.__conn = MongoClient(self.mongo_host, self.mongo_port)
        self.db = self.__conn[self.mongo_db]
        if self.mongo_user and self.mongo_passwd:
            self.db.authenticate(self.mongo_user, self.mongo_passwd)

    def __del__(self):
        self.__conn.close()

    def get_db(self):
        try:
            return self.db
        except Exception, e:
            raise e

    def create_unique_index(self, collection_name, key):
        try:
            self.db[collection_name].ensure_index([(key, ASCENDING)], unique=True)
        except Exception, e:
            raise e

    def create_asce_index(self, collection_name, key):
        try:
            self.db[collection_name].ensure_index([(key, ASCENDING)])
        except Exception, e:
            raise e

    def create_desc_index(self, collection_name, key):
        try:
            self.db[collection_name].ensure_index([(key, DESCENDING)])
        except Exception, e:
            raise e

    def find(self, collection_name, query_item):
        try:
            item_cursor = self.db[collection_name].find(query_item)
            return item_cursor
        except Exception, e:
            raise e

    def find_first(self, collection_name, query_item):
        try:
            item_cursor = self.db[collection_name].find(query_item).limit(1)
            item_list = list(item_cursor)
            if item_list:
                return item_list[0]
            else:
                return None
        except Exception, e:
            raise e

    def save(self, collection_name, item):
        try:
            self.create_unique_index(collection_name, 'url')
            self.create_desc_index(collection_name, 'download_time')
            self.db[collection_name].save(item)
        except Exception, e:
            raise e

    def insert(self, collection_name, item):
        try:
            self.create_unique_index(collection_name, 'url')
            self.create_desc_index(collection_name, 'download_time')
            self.db[collection_name].insert(item)
        except Exception, e:
            raise e

    def replace_one(self, collection_name, item_old, item_new):
        try:
            self.db[collection_name].replace_one(item_old, item_new)
        except Exception, e:
            raise e

    def delete_by_prefix(self, collection_name, field, prefix):
        try:
            pattern = re.compile('^' + prefix + '.*')
            regex = Regex.from_native(pattern)
            regex.flags ^= re.IGNORECASE
            self.db[collection_name].delete_many({field: {'$regex': regex}})
        except Exception, e:
            raise e

    def delete_by_pattern(self, collection_name, field, pattern):
        try:
            pattern = re.compile(pattern)
            regex = Regex.from_native(pattern)
            regex.flags ^= re.IGNORECASE
            self.db[collection_name].delete_many({field: {'$regex': regex}})
        except Exception, e:
            raise e

    def get_collection_names(self):
        try:
            collection_names = self.db.collection_names()
            if 'system.indexes' in collection_names:
                collection_names.remove('system.indexes')
            return collection_names
        except Exception, e:
            raise e

    def get_collection_count(self, collection_name):
        try:
            return self.db[collection_name].find().count()
        except Exception, e:
            raise e

    def select_by_url_format(self, collection_name, site, url_format, limit=0, start=0, extra_filter='{}'):
        try:
            #pattern = re.compile(url_format)
            #regex = Regex.from_native(pattern)
            #regex.flags ^= re.IGNORECASE
            regex = re.compile(url_format, re.IGNORECASE)
            extra_filter = json.loads(extra_filter)
            extra_filter.update({'url': {'$regex': regex}})
            if limit > 0:
                item_cursor = self.db[collection_name].find(extra_filter).skip(start).limit(limit)
            else:
                item_cursor = self.db[collection_name].find(extra_filter).skip(start);
        except Exception, e:
            raise e
        return item_cursor

    def find_in_url_list(self, collection_name, url_list):
        try:
            return self.db[collection_name].find({'url': {'$in': url_list}})
        except Exception, e:
            raise e
