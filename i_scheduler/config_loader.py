# coding=utf-8
import sys
import re
import pymongo
import json
import redis
import pytoml
import getopt
import traceback
import datetime
sys.path.append('..')
from i_util.logs import LogHandler
from i_util.tools import str_obj, unicode_obj, get_md5_i64
from common.mysql_utils import get_mysql_conn, mysql_fetch, mysql_execute


class SchedulerConfigLoader:
    def __init__(self, conf, mongodb_conf, redis_tasks, log):
        self.log = log
        # 初始化mysql
        self.mysql_conn = get_mysql_conn(conf)
        # 初始化mongodb
        self.mongodb_conf = mongodb_conf
        try:
            self.mongo_conn = pymongo.MongoClient(host=mongodb_conf['host'], port=mongodb_conf['port'])
        except Exception, e:
            self.log.error("connect mongodb fail:%s" % traceback.format_exc())
            exit(1)

        try:
            if mongodb_conf['username'] and mongodb_conf['password']:
                self.mongo_conn[mongodb_conf['database']].authenticate(mongodb_conf['username'], mongodb_conf['password'])
        except Exception, e:
            self.log.error("mongodb auth fail:%s" % traceback.format_exc())
            exit(1)

        self.mongo_db = self.mongo_conn[mongodb_conf['database']]
        # 初始化redis
        self.redis_tasks = redis_tasks
        try:
            self.redis_db = redis.Redis(host=self.redis_tasks['host'],
                                        port=self.redis_tasks['port'],
                                        db=self.redis_tasks['database'],
                                        password=self.redis_tasks['password'],
                                        )
        except Exception, e:
            self.log.error("connect redis fail:%s" % traceback.format_exc())
            exit(1)

        # self.seed_keys = self.fetch_mysql('describe seeds')
        self.seed_keys = mysql_fetch(self.mysql_conn, 'describe seeds')

    def load_sites(self, site=''):
        sites = {}
        if site:
            sql_str = 'SELECT site, name, avg_interval, encoding FROM `site` WHERE site="' \
                      + str(site) + '" ORDER BY `id`'
        else:
            sql_str = 'SELECT site, name, avg_interval, encoding FROM `site`  ORDER BY `id`'
        datas = mysql_fetch(self.mysql_conn, sql_str)
        for data in datas:
            task = {}
            site = data[0]
            task['site'] = site
            task['name'] = data[1]
            task['avg_interval'] = data[2]
            task['encoding'] = data[3]
            task['site_id'] = get_md5_i64(site)
            task['cookies'] = []
            cookie_sql = 'SELECT site, user_id, cookie FROM `cookie` where site ="%s" ORDER BY `id`' % site
            # cookies = self.fetch_mysql(cookie_sql)
            cookies = mysql_fetch(self.mysql_conn, cookie_sql)
            for cookie in cookies:
                cookie_dict = {}
                cookie_dict['user_id'] = cookie[1]
                cookie_dict['cookie'] = cookie[2].encode("utf8")
                task['cookies'].append(cookie_dict)
            sites[site] = task
        self.log.info("sites:" + str(len(sites)))
        return sites

    def load_seeds(self, site):
        seeds = {}
        datas = mysql_fetch(self.mysql_conn, 'SELECT * FROM `seeds` WHERE `mode`="on" and `site`="%s" ORDER BY `id`' % site)
        seeds = self.modify_seeds(datas)
        for seed_id, seed in seeds.items():
            mysql_execute(self.mysql_conn, 'UPDATE seeds SET mode="off" WHERE is_once="true" and id = %d' % seed_id)
        return seeds

    def load_seed_by_id(self, seed_id, restart=False):
        seed_info = {}
        if not restart:
            datas = mysql_fetch(self.mysql_conn, 'SELECT * FROM `seeds` WHERE `mode`="on" and `id`="%s" ORDER BY `id`' % seed_id)
        else:
            datas = mysql_fetch(self.mysql_conn, 'SELECT * FROM `seeds` WHERE `id`="%s" ORDER BY `id`' % seed_id)

        seeds = self.modify_seeds(datas)
        for seed_id, seed in seeds.items():
            mysql_execute(self.mysql_conn, 'UPDATE seeds SET mode="off" WHERE is_once="true" and id = %d' % seed_id)
            seed_info = seed
        return seed_info

    def macro_function(self, data):
        if isinstance(data, basestring):
            time_regex = re.compile('macro_time\(([%YmdHMS-][%YmdHMS-]{1,7})\,\s*([-+]\d+)\)')
            result = time_regex.search(data)
            if result is None:
                return data
            else:
                today = datetime.date.today()
                if result.group(2) < 0:
                    today = today - datetime.timedelta(days=int(result.group(2).strip('-')))
                elif result.group(2) > 0:
                    today = today + datetime.timedelta(days=int(result.group(2)))
                else:
                    today = today
                data = re.sub('(macro_time\([%YmdHMS-][%YmdHMS-]{1,7}\,\s*([-+]\d+)\))', str(today), data, 1)
                return self.macro_function(data)

        return data

    def modify_seeds(self, datas):
        seeds = {}
        count = 0
        site = None
        for data in datas:
            task = {}
            seed_id = -1
            count += 1
            task['variable_param_list'] = []
            task['page_turning_rule'] = {}
            task['session_commit'] = {}
            task['data'] = {}
            for i in range(len(data)):
                key = self.seed_keys[i][0]
                if key in ['mode', 'site_name']:
                    continue
                val = data[i]
                if not val and val != 0:
                    continue
                if key == 'id':
                    key = 'seed_id'
                    seed_id = val
                elif key.find('check_body') > -1:
                    val = val.replace('\'', '"')
                key = str_obj(key)
                val = str_obj(val)
                task[key] = self.macro_function(val)

                if key == "site":
                    site = val
                    site_id = get_md5_i64(val)
                if key == 'variable_params':
                    var_conf = json.loads(val)
                    if var_conf:
                        type = var_conf.get('type', "")
                    if type == 'mongo':
                        task['variable_param_list'] = []
                        mongo_conf = var_conf.get('mongo_data', {})
                        if mongo_conf:
                            task['variable_param_list'] = self.load_var_params(mongo_conf)
                    elif type == 'map':
                        task['variable_param_list'] = var_conf.get('map_data', [])
                    elif type == 'json':
                        task['variable_param_list'] = var_conf.get('json_data', [])

                    task.pop('variable_params')
                elif key == 'page_turning_rule':
                    task['page_turning_rule'] = json.loads(val)
                elif key == 'session_commit':
                    task['session_commit'] = json.loads(val)
                elif key == 'data':
                    task['data'] = json.loads(self.macro_function(val))
                elif key == 'http_header':
                    task['http_header'] = json.loads(val)
                elif key == 'config_init_period':
                    task['config_init_period'] = json.loads(val)
    
            #重置翻页参数时需要使用
            #task['origin_url'] = task['url']
            #task['origin_data'] = task['data']
            #task['current_variable_param'] = json.dumps([])
            task['site_id'] = site_id
            seeds[seed_id] = task

        mysql_execute(self.mysql_conn, 'UPDATE seeds SET mode="off" WHERE is_once="true"')
        self.log.info("config_loader\tsite:%s\tseeds:%d" % (site, len(seeds)))
        return seeds

    def load_var_params(self, var_conf):
        if (not var_conf.has_key('database')) or (not var_conf.has_key('mongo')):
            return []
        if var_conf.has_key('database') and var_conf['database'].has_key('host'):
            try:
                mongo_conn = pymongo.MongoClient(host=var_conf['database']['host'],
                                                 port=int(var_conf['database']['port']))
                username = var_conf['database'].get('username', '')
                password = var_conf['database'].get('password', '')
                if (not username) or (not password):
                    username = self.mongodb_conf.get('username', '')
                    password = self.mongodb_conf.get('password', '')
                if username and password:
                    mongo_conn[var_conf['database']['db']].authenticate(username, password)
            except Exception, e:
                self.log.error("load_var_params error:%s\n%s" % (e.message, traceback.format_exc()))
                return []

            mongo_db = mongo_conn[var_conf['database']['db']]
        else:
            mongo_conn = self.mongo_conn
            mongo_db = self.mongo_db
        select_fields = {}
        schema = var_conf['mongo']['select_fields'].split(",")
        for k in schema:
            select_fields[k] = 1
    
        optional_args = {}
        if var_conf['mongo'].has_key('optional'):
            optional_args = var_conf['mongo']['optional']
   
        #通过游标获取数据
        request_param = var_conf['request_param_list']
        table_name = var_conf['mongo']['table_name']
        where_condition = var_conf['mongo']['where_condition']
        if not where_condition:
            where_condition = {}
        else:
            where_condition = json.loads(where_condition)
            where_condition = self.macro_function(where_condition)

        limit = int(var_conf['mongo']['limit'])
        records = mongo_db[table_name].find(where_condition, select_fields).limit(limit)
        results = []
        for item in records:
            ele = {}
            for (key1, key2) in request_param.items():
                keys = key2.split('##')
                if len(keys) == 2:
                    if item.has_key(keys[0]):
                        ele[key1] = item[keys[0]] + self.macro_function(keys[1])
                else:
                    if item.has_key(keys[0]):
                        ele[key1] = item[keys[0]]

            results.append(ele)
        return results

    # def load_scheduler_mata(self):
    #     return {}

    # def load_fail_task(self):
    #     result_fail_tasks = {}
    #     try:
    #         fail_tasks = self.redis_db.lrange(self.redis_tasks['key'], 0, -1)
    #         for fail_task in fail_tasks:
    #             self.log.info('_load_fail_task : %s' % str(fail_task))
    #             json_task = json.loads(fail_task)
    #             url_id = None
    #             url = None
    #             if json_task.has_key('url'):
    #                 url = json_task['url']
    #             if json_task.has_key('url_id'):
    #                 url_id = int(json_task['url_id'])
    #             elif url is not None:
    #                 url_id = get_md5_i64(url)
    #             if url_id is None:
    #                 self.log.warn("load_fail_task\tstatus:failt\tinfo:%s" % (str(fail_task)))
    #             else:
    #                 self.log.info("load_fail_task\tstatus:success\turl_id:%d\turl:%s" % (url_id, url))
    #             task = {}
    #             for key, val in json_task.items():
    #                 key = str_obj(key)
    #                 val = str_obj(val)
    #                 task[key] = val
    #             result_fail_tasks[url_id] = task
    #         #self.redis_db.delete(self.conf.redis_tasks['key'])
    #     except:
    #         self.log.error('_load_fail_task error : ' + traceback.format_exc())
    #     self.log.info("load_fail_task\tsize:%d" % (len(result_fail_tasks)))
    #     return result_fail_tasks


# def usage():
#     pass
#
#
# def test(conf):
#     loader = SchedulerConfigLoader(conf['mysql_conf'], conf['mongodb_conf'], conf['redis_tasks'], conf['log'])
#     #site_dict = loader.load_sites("epub.sipo.gov.cn")
#     #print json.dumps(site_dict)
#     #seeds = loader.load_seeds("epub.sipo.gov.cn")
#     #print json.dumps(seeds)
#     seeds = loader.load_seed_by_id(458)
#     print json.dumps(seeds)
#     #loader.load_fail_task()
#
# if __name__ == '__main__':
#     try:
#         file_path = './scheduler.toml'
#         opt, args = getopt.getopt(sys.argv[1:], 'f:', ['help'])
#         for name, value in opt:
#             if name == "-f":
#                 file_path = value
#             elif name in ("-h", "--help"):
#                 usage()
#                 sys.exit()
#             else:
#                 assert False, "unhandled option"
#
#         with open(file_path, 'rb') as config:
#             conf = pytoml.load(config)
#             conf['log']=LogHandler(conf['server']['name']+str(conf['server']['port']))
#         test(conf)
#
#     except getopt.GetoptError:
#         sys.exit()