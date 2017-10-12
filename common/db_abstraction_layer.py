# encoding=utf-8

import pytoml
import pymongo
from common.log import log
from i_util.pyhbase.HBaseConnection import HBaseThrift2Connection
from pymongo.errors import DuplicateKeyError
import json

# Wrap all database backends into unified interface
# For now, we have the following backends:
#   1. HBase
#   2. MongoDB

class BaseBackend(object):
    def __init__(self, conf):
        self.conf = conf
        self.auto_decode_json = False

    def set_auto_decode_json(self, target):
        self.auto_decode_json = target

    def get(self, table, key):
        raise Exception("Not implemented")

    def put(self, table, key, col, value):
        raise Exception("Not implemented")

    def put_multi(self, table, key, col_defs):
        raise Exception("Not implemented")

    def create_table(self, name):
        raise Exception("Not implemented")

    def delete_col(self, table, key, cols):
        raise Exception("Not implemented")

    def delete_row(self, table, key):
        raise Exception("Not implemented")

    def list_table(self):
        raise Exception("Not implemented")

    def drop_table(self, table):
        raise Exception("Not implemented")

    def truncate_table(self, table):
        raise Exception("Not implemented")

class HBaseBackend(BaseBackend):

    HBASE_CF_NAME = 'docs'

    def __init__(self, conf):
        super(HBaseBackend, self).__init__(conf)
        # log.debug("starting db backend " + HBaseBackend.__name__)
        self.conn = HBaseThrift2Connection(conf=conf)

    def create_table(self, name):
        self.conn.create_table(table_name=name)

    def put(self, table, key, col, value):
        self.conn.put(table_name=table, rowkey=key, col_family=HBaseBackend.HBASE_CF_NAME, col=col, value=value)

    def put_multi(self, table, key, col_defs):
        self.conn.put_multi(table_name=table, rowkey=key, col_family=HBaseBackend.HBASE_CF_NAME, col_defs=col_defs)

    def get(self, table, key):
        res = self.conn.get(table_name=table, rowkey=key)
        return res

    def delete_row(self, table, key):
        self.conn.delete_row(table, key)

    def delete_col(self, table, key, cols):
        self.conn.delete_cols(table, key, cols)

    def list_table(self):
        return self.conn.list_table()

    def drop_table(self, table):
        self.conn.drop_table(table)

    def truncate_table(self, table):
        self.conn.truncate_table(table)

    def scan(self, table, rc=100, cols=None):
        return self.conn.scan(table, rc, cols)


class MongoBackend(BaseBackend):

    MONGO_ROW_KEY = '_row_key'

    def __init__(self, conf):
        super(MongoBackend, self).__init__(conf)
        # log.debug("starting db backend " + MongoBackend.__name__)
        self.mongo_client = pymongo.MongoClient(
            host=conf['host'],
            port=conf['port']
        )
        self.db = self.mongo_client[conf['db']]
        if conf['username'] != '':
            self.db.authenticate(conf['username'],
                                 conf['password'])

    def create_table(self, name):
        mongo_collection = self.db[name]

        # 因为是唯一索引，所以要确保不会出错
        try:
            mongo_collection.ensure_index(
                MongoBackend.MONGO_ROW_KEY,
                background=True,
                unique=True)
        except Exception as e:
            log.warn("索引好像已经创建，引起了重建异常:")
            log.exception(e)

    def get(self, table, key):
        mongo_collection = self.db[table]
        doc = mongo_collection.find_one(
            filter={
                MongoBackend.MONGO_ROW_KEY: key
            }
        )
        res_out = {}
        if doc is not None:
            res = doc[HBaseBackend.HBASE_CF_NAME]
            for col in res:
                res_out[self.unescape_dot(col)] = res[col]
        return res_out

    def escape_dot(self, s):
        return s.replace('.', '_#DOT#_')

    def unescape_dot(self, s):
        return s.replace('_#DOT#_', '.')

    def do_put(self, table, key, col_defs):
        mongo_collection = self.db[table]

        # Workaround for mongo bug #SERVER-14322
        # Do client-side retries on DuplicateKeyError
        retry_count = 1000  # Should be more than enough
        succ = False
        while not succ and retry_count > 0:
            retry_count -= 1
            try:
                s = {'$set': {}}
                for col in col_defs:
                    s['$set']['docs.' + self.escape_dot(col)] = col_defs[col]
                mongo_collection.update({MongoBackend.MONGO_ROW_KEY: key}, s, upsert=True)

            except DuplicateKeyError, e:
                # if upsert fail with DuplicateKeyError, just retry.
                # if other exceptions occur, no retry
                pass
            else:
                succ = True

    def do_unset(self, table, key, cols):
        mongo_collection = self.db[table]
        s = {'$unset': {}}
        for col in cols:
            s['$unset']['docs.' + self.escape_dot(col)] = ""
        mongo_collection.update({MongoBackend.MONGO_ROW_KEY: key}, s)

    def put(self, table, key, col, value):
        self.do_put(table, key, {col: value})

    def put_multi(self, table, key, col_defs):
        self.do_put(table, key, col_defs)

    def delete_row(self, table, key):
        mongo_collection = self.db[table]
        mongo_collection.delete_one(filter={
            MongoBackend.MONGO_ROW_KEY: key
        })

    def delete_col(self, table, key, cols):
        self.do_unset(table, key, cols)

    def list_table(self):
        return self.db.collection_names(False)

    def drop_table(self, table):
        mongo_collection = self.db[table]
        mongo_collection.drop()

    def truncate_table(self, table):
        mongo_collection = self.db[table]
        mongo_collection.delete_many({})

    def scan(self, table, rc=100, cols=None):
        mongo_collection = self.db[table]
        cursor = mongo_collection.find(filter={})
        res = []
        for raw_row in cursor:
            cur_row = {}
            for col_name in raw_row['docs']:
                unescaped_col_name = self.unescape_dot(col_name)
                if type(cols) != list or unescaped_col_name in cols:
                    cur_row[unescaped_col_name] = raw_row['docs'][col_name]
                    cur_row[unescaped_col_name] = cur_row[unescaped_col_name]
            res.append(cur_row)
        return res


class DBBackendFactory(object):
    backend_map = {
        'hbase': HBaseBackend,
        'mongodb': MongoBackend
    }

    @staticmethod
    def get_backend(conf, tag='db_backend'):
        assert type(conf) == dict
        backend_type = conf[tag]['type']
        backend_conf = conf[tag][backend_type]
        backend_class = DBBackendFactory.backend_map.get(backend_type, None)
        if backend_class is None:
            raise Exception("Invalid db backend type : " + backend_type)
        return backend_class(backend_conf)


if __name__ == '__main__':
    with open('test.toml') as conf_file:
        config = pytoml.load(conf_file)
        print config

        db = DBBackendFactory.get_backend(config)

        print db

        db.create_table('fff')

        db.put('fff', 'k1', 'c1', 'v1')
        db.put('fff', 'k1', 'c2', 'v2')
        db.put('fff', 'k1', 'c3', 'v3')

        db.put_multi('fff', 'k1', {'c4': 'v4', 'c5': 'v5'})

        print db.get('fff', 'k1')

        db.delete_col('fff', 'k1', ['c2', 'c5'])

        print db.get('fff', 'k1')

        db.delete_col('fff', 'k1', ['c2', 'c5'])

        print db.get('fff', 'k1')

        # db.delete_row('fff', 'k1')
        #
        # print db.get('fff', 'k1')

        db.truncate_table('fff')

        print db.get('fff', 'k1')

        print db.list_table()

        db.drop_table('fff')

        print db.list_table()
