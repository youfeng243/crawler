import MySQLdb
from common.log import log
from DBUtils import PersistentDB
from threading import Lock
import traceback

MYSQL_CONNECTION_POOL = "mysql_connection_pool"
FETCH_ONE = 0
FETCH_MANY = 1
FETCH_ALL = 2

class PoolHolder(object):
    def __init__(self):
        self.pool = None
        self.lock = Lock()

pool_holder = PoolHolder()

def init_connection_pool(conf):
    global pool_holder
    if pool_holder.pool is not None:
        # raise Exception("Trying to initialize mysql connection pool twice")
        return # Silently success
    try:
        pool_holder.pool = PersistentDB.PersistentDB(MySQLdb, 1000,
                                         host=conf['MYSQL']['host'],
                                         port=conf['MYSQL']['port'],
                                         user=conf['MYSQL']['username'],
                                         passwd=conf['MYSQL']['password'],
                                         db=conf['MYSQL']['dbname'],
                                         charset='utf8',
                                         setsession=['set autocommit = 1'])
    except Exception, e:
        log.error("failed to initialize mysql connection pool, message : %s" % (e.message))
        log.error(traceback.format_exc())

def get_mysql_conn(conf):
    global pool_holder
    with pool_holder.lock:
        init_connection_pool(conf)
        return pool_holder.pool.connection()

def mysql_fetch(conn=None, sql=None, type=FETCH_ALL, rows=1, return_dict=False, conf=None):
    with pool_holder.lock:
        if conn is None:
            conn = get_mysql_conn(conf)

        cur = conn.cursor()
        cur.execute(sql)

        if type == FETCH_ONE:
            raw_res = cur.fetchone()
        elif type == FETCH_MANY:
            raw_res = cur.fetchmany(rows)
        else:
            raw_res = cur.fetchall()

        if return_dict:
            res = [dbrecord_to_dict(cur.description, record) for record in raw_res]
        else:
            res = raw_res

        cur.close()
        conn.close()
        return res

def mysql_execute(conn=None, sql=None, conf=None):
    with pool_holder.lock:
        if conn is None:
            conn = get_mysql_conn(conf)
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()
        affected_rows = cur.rowcount
        conn.close()
        return affected_rows

def mysql_exec_oneshot(conf, sql):
    record_dicts = []
    mysql_conn = get_mysql_conn(conf)
    with pool_holder.lock:
        cur = mysql_conn.cursor()
        cur.execute(sql)
        records = cur.fetchall()
        record_dicts = [dbrecord_to_dict(cur.description, record) for record in records]
        cur.close()
    mysql_conn.close()
    return record_dicts

def dbrecord_to_dict(descriptions, record):
    return {desc[0]: field_value for desc, field_value in zip(descriptions, record)}



