# -*- coding:utf-8 -*-
from logging.handlers import RotatingFileHandler

from flask import Flask, request
import logging
import pymongo,time
from redis import StrictRedis
from ast import literal_eval
from i_downloader.conf import redis_proxies
__author__ = "Echo"

# db_proxy = pymongo.MongoClient(mongo_db['host'], mongo_db['port'])[mongo_db['name']]
# if mongo_db['username'] and mongo_db['password']:
#     connected = db_proxy.authenticate(mongo_db['username'], mongo_db['password'])
# else:
#     connected = True

PROXY_TABLE = 'proxies_collection'
PROXY_TABLE_2='tb_proxies_all'

proxies_file = './conf/proxies_400.txt'
logging_file = './proxies_app.log'
logger = logging.getLogger('A')
logger.setLevel(logging.INFO)
logfile = RotatingFileHandler(logging_file, maxBytes=1000000, backupCount=1000)
formatter = logging.Formatter('%(asctime)s%(levelname)s:%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logfile.setFormatter(formatter)
logger.addHandler(logfile)
app = Flask(__name__)
# 通过key,proxy_type来指定要获取的key类型
# proxy_type=1 默认,收费代理
# proxy_type=2 普通免费代理


@app.route('/get_proxy', methods=['GET'])
def get_proxy():

    key = request.args.get('key', 'default')
    proxy_type = int(request.args.get('proxy_type', '1'))
    proxy_addr = proxies.get_proxy(key, proxy_type)
    logger.info('key:{0},proxy:{1}'.format(key, proxy_addr))
    return proxy_addr


@app.route('/remove_proxy', methods=['GET'])
def remove_proxy():
    key = request.args.get('key', 'default')
    proxy_addr = proxies.remove_proxy(key)
    logger.info('remove key:{0},status:{1}'.format(key, proxy_addr))
    return proxy_addr


@app.route('/useless_proxy', methods=['GET'])
def update_proxy_info():
    key = request.args.get('key', None)
    aim_proxy = request.args.get('proxy', None)
    if key is None or aim_proxy is None or key is '':
        return 'args error'
    result = proxies.sub_grade_with_proxy(key, aim_proxy)
    return result


@app.route('/reset_all_grade', methods=['GET'])
def reset_all_grade():
    key = request.args.get('key', None)
    if key is None:
        return 'args error'
    result = proxies.reset_all_grade(key)
    return result


class Proxies:
    def __init__(self, proxies_file_path, default_grade=0, stander_grade=0):
        self.default_grade = default_grade
        self.stander_grade = stander_grade
        self.sub_grade = 1
        self.proxies_file_path = proxies_file_path
        self.redis_connect = StrictRedis(host=redis_proxies['host'], port=redis_proxies['port'],
                                         db=redis_proxies['database'], password=redis_proxies['password'])

    def get_proxy(self, key, proxy_type):
        proxy , grade=self.get_next_item(key,proxy_type)
        return proxy
        # proxy = None
        # grade = self.stander_grade - 1
        # start=time.time()
        # while grade < self.stander_grade:
        #     proxy, grade = self.get_next_item(key, proxy_type)
        # end=time.time()
        # logger.info ("method:{2}:{0}:time[{1}]".format (key , end - start , 'get_proxy'))
        # return proxy

    def sub_grade_with_proxy(self, key, aim_proxy):
        if key not in self.redis_connect.keys():
            return 'key not exit'
        index = self.r_find_index(key, aim_proxy)

        if index is not None:
            self.sub_item_grade(key, index)
            return 'OK'
        return 'proxy not exit'

    def remove_proxy(self, key):
        if key not in self.redis_connect.keys():
            return "key not exit"
        self.redis_connect.delete(key)
        return 'successed'

    def reset_all_grade(self, key):
        if key not in self.redis_connect.keys():
            return "key not exit"
        # tuple数组的redis存储问题
        all_proxies = map(literal_eval, self.redis_connect.lrange(key, 0, -1))
        new_proxies = map(lambda x: (x[0], self.default_grade), all_proxies)
        self.redis_connect.delete(key)
        self.redis_connect.rpush(key, *new_proxies)
        return 'OK'

    # 根据key来获取proxy,key不存在就初始化key
    def get_next_item(self, key, proxy_type):
        start=time.time()
        grade = self.stander_grade - 1
        proxy=None
        while grade<self.stander_grade:
            proxy_with_grade = self.redis_connect.lpop(key)
            if proxy_with_grade is None:
                self.init_proxies(key=key, proxy_type=proxy_type)
                proxy_with_grade = self.redis_connect.lpop(key)
            proxy, grade = literal_eval(proxy_with_grade)
        self.redis_connect.rpush(key, (proxy, grade))
        end=time.time()
        logger.info("method:{2}:{0}:time[{1}]".format(key,end-start,'get_next_item'))
        return proxy, grade

    def get_item(self, index, key):
        proxy_with_grade = self.redis_connect.lindex(key, index)
        proxy, grade = literal_eval(proxy_with_grade)
        return proxy, grade

    def sub_item_grade(self, key, index):
        proxy, old_grade = self.get_item(index, key)
        new_grade = old_grade - self.sub_grade
        self.redis_connect.lset(key, index, (proxy, new_grade))

    # value -> index
    def r_find_index(self, key, aim_proxy):
        start_index = -1
        proxies_len = self.redis_connect.llen(key)
        for i in range(proxies_len):
            index = start_index - i
            proxy, grade = self.get_item(index, key)
            if proxy == aim_proxy:
                return index
        return None

    # 初始化一个key
    def init_proxies(self, key='default', proxy_type=1):
        proxies_with_grade = self._create_proxies_list(proxy_type)
        # 清除老的proxies
        self.redis_connect.delete(key)
        self.redis_connect.rpush(key, *proxies_with_grade)
        self.redis_connect.expire(key, 3600 * 6)
        logger.info('init proxies for key:{0}'.format(key))
        return True

    # def _create_proxies_list(self, proxy_type=1):
    #     proxies_with_grade = []
    #     # 默认使用收费IP
    #     with open(self.proxies_file_path) as fr:
    #         proxies = fr.read().splitlines()
    #     if proxy_type == 2:
    #         del proxies[:]
    #         cur_result = db_proxy[PROXY_TABLE].find({})
    #         for item in cur_result:
    #             proxies.append(item['_id'])
    #     if proxy_type==3:
    #         del proxies[:]
    #         cur_result = db_proxy[PROXY_TABLE_2].find({})
    #         for item in cur_result:
    #             proxies.append(item['_id'])
    #
    #     proxies_with_grade = map(lambda x: ('http://{}'.format(x), self.default_grade), proxies)
    #     return proxies_with_grade


if __name__ == '__main__':
    proxies = Proxies(proxies_file_path=proxies_file)
    app.run(host="0.0.0.0", debug=False, use_reloader=False,
            port=9300, threaded=True)
