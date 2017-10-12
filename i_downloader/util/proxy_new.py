# -*- coding:utf-8    -*-
import json
import os
import time

import requests
from redis import StrictRedis

proxies_file = os.path.join(os.path.dirname(__file__), 'proxies_400.txt')


class Proxies(object):
    def __init__(self, log, config, default_status=1, start_index=0, proxy_faile_mark=0):
        self.log = log
        self.redis_conf = config
        self.proxy_faile_mark = proxy_faile_mark
        self.default_status = default_status
        self.start_index = start_index
        self.proxy_name = self.redis_conf['proxy_name']
        self.redis_connect = StrictRedis(host=self.redis_conf['host'], port=self.redis_conf['port'],
                                         db=self.redis_conf['database'], password=self.redis_conf['password'])

    def init_proxy(self):
        try:
            now_time = json.loads(self.redis_connect.lindex(self.proxy_name, 0))['in_time']
        except Exception as e:
            now_time = -1
            self.log.exception(e)
        if time.time() - now_time < 3600:
            return
        try:
            self.redis_connect.flushdb()
            self.read_file()
        except Exception as e:
            self.log.error("加载静态代理信息失败: ")
            self.log.exception(e)

    def get_dynamic_proxy(self):
        proxy_url = 'http://101.132.128.78:18585/proxy'

        user_config = {
            'username': 'beihai',
            'password': 'beihai',
        }
        for _ in xrange(3):

            try:
                r = requests.post(proxy_url, json=user_config, timeout=10)
                if r.status_code != 200:
                    continue
                json_data = json.loads(r.text)
                is_success = json_data.get('success')
                if not is_success:
                    continue

                proxy = json_data.get('proxy')
                if proxy is None:
                    continue

                self.log.info("获取远程代理成功: proxy = {}".format(proxy))
                return proxy
            except Exception as e:
                self.log.error('获取代理异常:')
                self.log.exception(e)

        return None

    def read_file(self):
        with open(proxies_file) as fr:
            proxies = fr.read().splitlines()
            for i in proxies:
                proxy_dict = self.file_to_dict(i)
                proxy_dict["status"] = self.default_status
                proxy_str = self.dict2str(proxy_dict)
                self.redis_connect.rpush(self.proxy_name, proxy_str)

    # 根据站点轮询获取代理
    def get_proxy(self, site):

        proxy = self.get_dynamic_proxy()
        if proxy is not None:
            return proxy

        p = self.redis_connect.pipeline(transaction=True)
        flag = 0
        proxy = 0
        while flag == 0:
            index = self.get_index(site)
            proxy_str = self.get_item(index)
            proxy_dict = self.str2dict(proxy_str)
            flag = proxy_dict["status"]
            proxy = self.dict_to_proxy(proxy_dict)
            if flag == 0:
                continue
            if self.is_exit_index(self.site_to_key(site, proxy)):
                flag = 0
                continue
        p.execute(raise_on_error=True)
        self.log.warn("获取动态代理失败，需要用静态代理: {}".format(proxy))
        return proxy

    # 文件格式转化
    def file_to_dict(self, proxy_str):
        proxy_dict = {"type": "http"}
        user = proxy_str.split('@')[0]
        ip = proxy_str.split('@')[1]
        proxy_dict["user"] = user.split(':')[0]
        proxy_dict["password"] = user.split(':')[1]
        proxy_dict["ip"] = ip.split(':')[0]
        proxy_dict["port"] = int(ip.split(':')[1])
        proxy_dict["in_time"] = time.time()
        proxy_dict["check_time"] = time.time()
        return proxy_dict

    # 6个方法各种转化
    def proxy_to_dict(self, proxy_str):
        proxy_dict = {}
        proxy_type = proxy_str.split('://')[0]
        user = proxy_str.split('://')[1].split('@')[0]
        ip = proxy_str.split('://')[1].split('@')[1]
        proxy_dict["user"] = user.split(':')[0]
        proxy_dict["password"] = user.split(':')[1]
        proxy_dict["ip"] = ip.split(':')[0]
        proxy_dict["port"] = int(ip.split(':')[1])
        proxy_dict["type"] = proxy_type
        return proxy_dict

    def dict_to_proxy(self, dict):
        proxy_type = dict.get("type")
        ip = dict.get("ip")
        port = dict.get("port")
        user = dict.get("user")
        password = dict.get("password")
        proxy = proxy_type + "://" + user + ":" + password + "@" + ip + ":" + str(port)
        return proxy

    def dict2str(self, dict):
        return json.dumps(dict)

    def str2dict(self, str):
        return json.loads(str)

    def dictlist2strlist(self, dictlist):
        strlist = []
        for i in dictlist:
            strlist.append(self.dict2str(i))
        return strlist

    def strlist2dictlist(self, strlist):
        dictlist = []
        for i in strlist:
            dictlist.append(self.str2dict(i))
        return dictlist

    # 判断key是否存在
    def is_exit_index(self, site):
        return self.redis_connect.exists(site)

    # 获取总代理数
    def get_proxy_llen(self):
        return self.redis_connect.llen(self.proxy_name)

    # 获取站点轮询的index
    def get_index(self, site):
        index = 0
        if self.is_exit_index(site):
            index = self.redis_connect.get(site)
        else:
            self.redis_connect.set(site, self.start_index)
            index = self.start_index
        lens = self.get_proxy_llen()
        self.redis_connect.set(site, (int(index) + 1) % lens)
        return index

    # 将站点转换为redis中的key
    def site_to_key(self, site, proxy):
        return '%s#%s' % (site, proxy)

    # 查找所有的代理及状态
    def findall(self):
        try:
            return self.redis_connect.lrange(self.proxy_name, 0, -1)

        except Exception as e:
            self.log.info("findall\tfail\treason:{}".format(e.message))

    # 由index获取代理
    def get_item(self, index):
        return self.redis_connect.lindex(self.proxy_name, index)
