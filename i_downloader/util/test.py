# -*- coding:utf-8    -*-
import copy
import json
import os
import time
import traceback
import threading
import requests
import schedule
from redis import StrictRedis
from requests.exceptions import ProxyError
REDIS = {
    'host': '127.0.0.1',  # 线上redisIP
    'password': 'haizhi@)',
    'port': 6379,
    'database':8,
    'proxy_name':'proxies',
    'proxy_test_available':24 #hours
}
proxies_file='./proxy.txt'

class Proxies():
    def __init__(self,config,default_status=1, start_index=0,proxy_faile_mark=0):
        self.redis_conf=config
        self.proxy_faile_mark=proxy_faile_mark
        self.default_status = default_status
        self.start_index = start_index
        self.proxy_name=self.redis_conf['proxy_name']
        self.redis_connect = StrictRedis(host=self.redis_conf['host'], port=self.redis_conf['port'],db=self.redis_conf['database'], password=self.redis_conf['password'])

    def init_proxy(self):
        try:
            self.redis_connect.flushdb()
            self.read_file()
        except Exception as e:
            pass
    def read_file(self):
        with open(proxies_file) as fr:
            proxies = fr.read().splitlines()
            for i in proxies:
                dict = self.file2dict(i)
                dict["status"] = self.default_status
                proxy_str = self.dict2str(dict)
                print proxy_str
                self.redis_connect.rpush(self.proxy_name, proxy_str)
    # 测试更新全量代理
    def updateall_proxy(self):
        strlist = self.findall()
        dictlist = self.strlist2dictlist(strlist)
        for olddict in dictlist:
            try:
                res = self.testOne_proxy(olddict)
                newdict = copy.deepcopy(olddict)
                newdict["status"] = res
                newdict["check_time"]=time.time()
                oldproxy = self.dict2str(olddict)
                newproxy = self.dict2str(newdict)
                self.update_proxy(oldproxy, newproxy)
                self.log.info("update\t{0}\tto\t{1}success".format(oldproxy,newproxy))
            except Exception :
                self.log.error("update\t{0}\tto\t{1}fail\t{2}".format(oldproxy, newproxy,traceback.print_stack()))

    # 根据站点轮询获取代理
    def get_proxy(self, site):
        p = self.redis_connect.pipeline(transaction=True)
        flag = 0
        proxy = 0
        while flag == 0:
            index = self.get_index(site)
            str = self.get_item(index)
            dict = self.str2dict(str)
            flag = dict["status"]
            proxy = self.dict2proxy(dict)
            if flag == 0:
                continue
            if self.is_exit_index(self.site_to_key(site, proxy)):
                flag = 0
                continue
        p.execute(raise_on_error=True)
        return proxy

    def findbyip(self,ip):
        strlist=self.findall()
        dictlist=self.strlist2dictlist(strlist)
        for i in dictlist:
            if ip==i.get("ip"):
                return i

    def proxy_cannot_ping(self,aim_proxy):
        dict=self.proxy2dict(aim_proxy)
        old_proxy=self.dict2str(self.findbyip(dict.get("ip")))
        new_dict=self.str2dict(old_proxy)
        new_dict["status"]=0
        new_dict["check_time"]=time.time()
        new_proxy=self.dict2str(new_dict)
        self.update_proxy(old_proxy,new_proxy)
    #文件格式转化
    def file2dict(self,str):
        dict={}
        dict["type"]="http"
        user = str.split('@')[0]
        ip = str.split('@')[1]
        dict["user"] = user.split(':')[0]
        dict["password"] = user.split(':')[1]
        dict["ip"] = ip.split(':')[0]
        dict["port"] = int(ip.split(':')[1])
        dict["in_time"]=time.time()
        dict["check_time"]=time.time()
        return  dict
    #6个方法各种转化
    def proxy2dict(self,proxys):
        dict={}
        type=proxys.split('://')[0]
        user = proxys.split('://')[1].split('@')[0]
        ip = proxys.split('://')[1].split('@')[1]
        dict["user"] = user.split(':')[0]
        dict["password"] = user.split(':')[1]
        dict["ip"] = ip.split(':')[0]
        dict["port"] = int(ip.split(':')[1])
        dict["type"] = type
        return  dict
    def dict2proxy(self,dict):
        type=dict.get("type")
        ip = dict.get("ip")
        port = dict.get("port")
        user = dict.get("user")
        password = dict.get("password")
        proxy = type+"://"+user + ":" + password + "@" + ip + ":" + str(port)
        return proxy
    def dict2str(self,dict):
        return json.dumps(dict)
    def str2dict(self,str):
        return json.loads(str)
    def dictlist2strlist(self,dictlist):
        strlist=[]
        for i in dictlist:
            strlist.append(self.dict2str(i))
        return strlist
    def strlist2dictlist(self,strlist):
        dictlist=[]
        for i in strlist:
            dictlist.append(self.str2dict(i))
        return dictlist

    def get_utime(self):

        pass
    #定期测试全量代理并更新状态
    def update_proxy_time(self,proxy_test_available):
        while True:
            time.sleep(200)
            now_time = json.loads(self.redis_connect.lindex(self.proxy_name,0))['in_time']
            if time.time()-now_time>proxy_test_available:
                now_time=time.time()
                try:
                    self.init_proxy()
                except Exception as e:
                    pass


    #判断key是否存在
    def is_exit_index(self,site):
        return self.redis_connect.exists(site)
    #获取总代理数
    def get_proxy_llen(self):
        return self.redis_connect.llen(self.proxy_name)
    #获取站点轮询的index
    def get_index(self,site):
        index =0
        if self.is_exit_index(site):
            index= self.redis_connect.get(site)
        else:
            self.redis_connect.set(site,self.start_index)
            index =self.start_index
        lens=self.get_proxy_llen()
        self.redis_connect.set(site,(int(index)+1)%lens)
        return index
    #站点和代理合并
    def site_proxy_mark(self,site,proxy):
        mark='%s#%s'%(site,proxy)
        self.redis_connect.set(mark,self.proxy_faile_mark,ex=6*3600)

    #将站点转换为redis中的key
    def site_to_key(self,site,proxy):
        return '%s#%s'%(site,proxy)
    #查找所有的代理及状态
    def findall(self):
        try:
            return self.redis_connect.lrange(self.proxy_name,0,-1)

        except Exception as e:
            self.log.info("findall\tfail\treason:{}".format(e.message))

    #添加代理
    def add_one_proxy(self,dict):
        try:
            dict['type']='http'
            dict['in_time']=time.time()
            dict['check_time']=time.time()
            dict['status']=1
            str=self.dict2str(dict)
            self.redis_connect.rpush(self.proxy_name,str)
            return 1
        except Exception as e:
            return 0

    #把代理映射成index
    def find_index(self,aim_proxy):
        dict_list=self.findall()
        return dict_list.index(aim_proxy)

    #更新代理tuple
    def update_proxy(self,oldproxy,newproxy):
        p = self.redis_connect.pipeline(transaction=True)
        index=self.find_index(oldproxy)
        self.redis_connect.lset(self.proxy_name,index=index,value=newproxy)
        p.execute(raise_on_error=True)

    #删除全部代理
    def remove_all(self):
        self.redis_connect.delete(self.proxy_name)
    #根据ip删除代理
    def removebyip(self,ip):
        try:
            dict=self.findbyip(ip)
            str=self.dict2str(dict)
            self.redis_connect.lrem(self.proxy_name,0,str)
            return 1
        except Exception as e:
            return 0
    #删除代理list
    def remove_proxy(self,list):
        try:
            for i in list:
                self.redis_connect.lrem(self.proxy_name,0,i)
            return 1
        except Exception as e:
            self.log.error("remove\tproxy\tfail\treason:{}".format(e.message))
            return 0

    #由index获取代理
    def get_item(self, index):
        return self.redis_connect.lindex(self.proxy_name, index)
    def get_proxy_detail(self):
        strlist=self.findall()
        dictlist=self.strlist2dictlist(strlist)
        fail_num=0
        success_num=0
        for i in dictlist:
            if i.get("status")==0:
                fail_num+=1
            else:
                success_num+=1
        return dictlist,fail_num,success_num

    def testlist_proxy(self):
        for i in self.strlist2dictlist(self.findall()):
            self.testOne_proxy(i)

        pass


    def get_all_ip(self):
        strlist=self.findall()
        dictlist=self.strlist2dictlist(strlist)
        iplist=[]
        for i in dictlist:
            iplist.append(i.get('ip'))
        return iplist





    #测试代理是否可用
    def testOne_proxy(self,dict):
        try:
            ip=dict.get("ip")
            port=dict.get("port")
            user = dict.get("user")
            password = dict.get("password")
            url='http://www.baidu.com'
            proxy=user+":"+password+"@"+ip+":"+str(port)
            kw={}
            kw['proxies']={
                'http':'http://%s'%(proxy),
                'https':'https://%s'%(proxy)
            }
            res=requests.get(url=url,timeout=10,**kw)
            if res.status_code==200:
                return 1
            else:
                return 0
        except ProxyError as e:
            return 0
        except Exception as e:
            return 1

if __name__ == '__main__':
    Proxies=Proxies(config=REDIS)
    Proxies.init_proxy()

