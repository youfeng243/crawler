import pymongo
from pymongo.errors import DuplicateKeyError

from i_util.global_defs import MetaFields

def auto_reconnect(retry_limit=30, retry_delay=0.5):
    from pymongo.errors import AutoReconnect
    import time
    def decorator(func):
        def wrapper(*args, **kwargs):
            tried_times = 0
            while True:
                try:
                    return func(*args)
                except AutoReconnect as r:
                    tried_times += 1
                    if tried_times > retry_limit:
                        raise Exception("pymongo cannot reconnect successfully after %d retries." % retry_limit)
                    time.sleep(retry_delay)
        return wrapper
    return decorator


class SafeIterator(object):

    def __init__(self, iter):
        self.iter = iter

    @auto_reconnect()
    def __iter__(self):
        return self.iter.__iter__()

    @auto_reconnect()
    def __getitem__(self, item):
        return self.iter.__getitem__(item)

    def close(self):
        self.iter.close()



class MongoManager(object):

    default_db = 'final_data'
    def __init__(self, conf, table_names = []):
        self.conf = conf
        self.mongo_client = pymongo.MongoClient(
            host=conf['host'],
            port=conf['port']
        )
        self.db = self.mongo_client[conf['db']]
        if conf['username'] and conf['password']:
            self.db.authenticate(conf['username'],
                                 conf['password'])
        self.init(table_names)

    def close(self):
        self.mongo_client.close()

    @auto_reconnect()
    def scan_iter(self, table_name):
        mongo_collection = self.db[table_name]
        return SafeIterator(mongo_collection.find(filter={}))

    @auto_reconnect()
    def put(self, table_name, doc):
        collection = self.db[table_name]
        # Workaround for mongo bug #SERVER-14322
        # Do client-side retries on DuplicateKeyError
        retry_count = 1000  # Should be more than enough
        succ = False
        update_result = None
        while not succ and retry_count > 0:
            retry_count -= 1
            try:
                update_result = collection.replace_one(
                    filter={
                        MetaFields.RECORD_ID: doc[MetaFields.RECORD_ID]
                    },
                    upsert=True,
                    replacement=doc
                )
            except DuplicateKeyError, e:
                # if upsert fail with DuplicateKeyError, just retry.
                # if other exceptions occur, no retry
                pass
            else:
                succ = True


    def init(self, target_table_names):
        current_tables = self.list_tables()
        for table_name in target_table_names:
            if table_name not in current_tables:
                self.create_table(table_name)

    @auto_reconnect()
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

    @auto_reconnect()
    def fetch_one(self, colleciton_name, query):
        return self.db[colleciton_name].find_one(query)