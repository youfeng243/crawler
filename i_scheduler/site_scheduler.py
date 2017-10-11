# coding=utf-8
import json
import sys
import threading
import getopt
import time
import traceback
import functools
import copy
import urllib
import re
import random
import pytoml
from redis import StrictRedis
from Queue import PriorityQueue, Queue, Empty
from selector import Selector
from config_loader import SchedulerConfigLoader
#thrift 相关api
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer
from thrift.transport.TTransport import TMemoryBuffer
sys.path.append('..')
from bdp.i_crawler.i_downloader.ttypes import DownLoadReq
from bdp.i_crawler.i_downloader.ttypes import SessionCommit
from bdp.i_crawler.i_downloader.ttypes import Proxy
from bdp.i_crawler.i_crawler_merge.ttypes import LinkAttr
from bdp.i_crawler.i_extractor.ttypes import LinkType
from i_util.tools import str_obj, unicode_obj, get_md5_i64, url_query_decode, build_hzpost_url, extract_hzpost_url
from i_util.logs import LogHandler


DOC_TYPES = ['seed', 'index', 'item']
# DOC_TYPES = ['index', 'item']


def with_after_effect(after):
    def invoker(method):
        @functools.wraps(method)
        def wrapper(self, *args, **kwargs):
            result = method(self, *args, **kwargs)
            getattr(self, after)()
            return result
        return wrapper
    return invoker


class SiteScheduler(object):

    def __init__(self, site_info, redis_conf, log, site_statistic, seed_statistic):
        self.log = log
        self.site_statistic = site_statistic
        self.seed_statistic = seed_statistic
        # 初始化站点信息
        self.site_info = site_info
        self.site_name = site_info['name']
        self.site_id = site_info['site_id']
        self.site = site_info['site']
        self.cookies = self.site_info.get('cookies', [])
        self.avg_interval = self.site_info.get('avg_interval', 10)
        self.seed_interval = 3600*4
        self.level_interval = [600, 1200, 3600, 14400, 21600, 43200, 86400]
        # self.period_interval = [-1, 360, 86400, 604800]
        self.period_interval = [3600, 14400, 21600, 43200, 86400, 172800, 604800]
        self.page_per = 100
        # 初始化redis-client
        self.redis_conf = redis_conf
        self.redis = StrictRedis(host=self.redis_conf['host'],
                                 port=self.redis_conf['port'],
                                 password=self.redis_conf['password']
                                 )

        # 初始化调度队列
        self.seed_queue = PriorityQueue()
        self.item_queue = PriorityQueue()
        self.index_queue = PriorityQueue()
        self.seeds_meta = {}
        self.seeds_info = {}
        self.seeds_last_start_time = {}
        self.fails_meta = {}
        self.index_urls_dict = {}
        self.content_urls_dict = {}
        self.dynamic_urls = {}
        # 初始化调度器状态
        self.counter = 0
        self.stopped = False
        self.stop_time = 0
        self._get_status()
        self.wlock = threading.Lock()
        #if not self.stopped:
        #    self._resume_tasks(); # 历史抓取url
        #    self._resume_seed();  # 种子抓取历史
        #    self._resume_fail();  # 下载失败任务
        #    self._report_status()
        #编码模式:
        self.encoding = self.site_info.get('encoding', 'utf8')
        # 计算调度器时间
        self.next_dispatch_time = time.time() + self.avg_interval

    def clear_one_site_cache(self):
        self.log.warning("clear site cache\tsite:%s" % self.site)
        self.index_urls_dict.clear()
        self.content_urls_dict.clear()


    def reload_site(self, site_info):
        self.site_info = site_info
        self.site_id = site_info['site_id']
        self.site = site_info['site']
        self.cookies = self.site_info.get('cookies', [])
        self.avg_interval = self.site_info.get('avg_interval', 10)
        self.encoding = self.site_info.get('encoding', 'utf8')

    def get_schedule_seeds(self):
        seed_ids = []
        for seed_id, seed in self.seeds_info.items():
            last_start_time = seed.get('last_start_time', None)
            config_init_period = seed.get('config_init_period', None)
            if config_init_period and config_init_period.get('is_period',
                                                             None) == 'false' or config_init_period is None:
                continue

            period = int(config_init_period.get('period', 0))
            interval = 0
            if period in [1, 2, 3, 4, 5, 6, 7]:
                interval = self.period_interval[period-1]

            if period == 0:
                interval = config_init_period.get('custom', 3600)

            if (last_start_time is not None) and interval > 0:
                next_start_time = interval + last_start_time
                if next_start_time < time.time():
                    seed['last_start_time'] = time.time()
                    seed_ids.append(seed_id)
        return seed_ids

    def string_to_meta(self, body):
        meta_info = LinkAttr()
        try:
            tMemory_o = TMemoryBuffer(body)
            tBinaryProtocol_o = TBinaryProtocol(tMemory_o)
            meta_info.read(tBinaryProtocol_o)
            return meta_info
        except EOFError, e:
            self.log.warning("cann't read DownLoadReq from string")
            return None

    def meta_to_string(self, meta):
        str_meta = None
        try:
            tMemory_b = TMemoryBuffer()
            tBinaryProtocol_b = TBinaryProtocol(tMemory_b)
            meta.write(tBinaryProtocol_b)
            str_meta = tMemory_b.getvalue()
        except EOFError, e:
            self.log.warning("cann't write meta to string")
        return str_meta

    def string_to_req(self, body):
        req_info = DownLoadReq()
        try:
            tMemory_o = TMemoryBuffer(body)
            tBinaryProtocol_o = TBinaryProtocol(tMemory_o)
            req_info.read(tBinaryProtocol_o)
            return req_info
        except EOFError, e:
            self.log.warning("cann't read DownLoadReq from string")
            return None

    def req_to_string(self, download_req):
        str_req = None
        try:
            tMemory_b = TMemoryBuffer()
            tBinaryProtocol_b = TBinaryProtocol(tMemory_b)
            download_req.write(tBinaryProtocol_b)
            str_req = tMemory_b.getvalue()
        except EOFError, e:
            self.log.warning("cann't write DownLoadReq to string")
        return str_req;        

    def _report_status(self):
        key = 'site:%s' % self.site
        self.redis.hset(key, 'status', 'stopped' if self.stopped else 'running')
        self.redis.hset(key, 'seed', self.seed_queue.qsize())
        self.redis.hset(key, 'item', self.item_queue.qsize())
        self.redis.hset(key, 'index', self.index_queue.qsize())
        self.redis.hset(key, 'counter', self.counter)

    @with_after_effect('_report_status')
    def select_seed(self):
        if len(self.seeds_meta) > 0:
            self.log.info("select_seed\tsite:%s\tsize:%d" % (self.site, len(self.seeds_meta)));
        for url_id, meta in self.seeds_meta.items():
            priority = 6
            if self.seeds_info.has_key(meta.seed_id):
                seed_info = self.seeds_info[meta.seed_id]
                priority = int(seed_info.get("priority", 6) if str(seed_info.get("priority", 6)) else 6)
            if priority > 6 or priority < 0:
                priority = 6
            seed_interval = self.level_interval[priority] 
            if meta.crawl_info and meta.crawl_info.download_time + seed_interval <= int(time.time()):
                if meta.crawl_info.schedule_time + seed_interval <= int(time.time()):
                    req = self.create_download_req(meta)
                    priority_key = str(time.time())
                    self.seed_queue.put((priority_key, req))
                    meta.crawl_info.schedule_time = int(time.time())
                    self.log.info("select_seed\turl:%s" % str(req.url))
        return True

    def _get_status(self):
        key = 'site:%s' % self.site
        result = self.redis.hget(key, 'status')
        if result == None or result == 'stopped':
            self.stopped = True

    def _deposit_task(self, task, doc_type='item'):
        if task:
            req_str = self.req_to_string(task)
            result = self.redis.rpush('resume:%s:%s' % (doc_type, self.site), req_str)
            # if task.url in self.urls_dict:
            #     del self.urls_dict[task.url]
            self.log.info("deposit_task\tsite:%s\tdoc_type:%s\turl:%s\tdownload_type:%s" % (self.site, doc_type,
                                                                                            task.url,
                                                                                            task.download_type))
            return result
        else:
            return False

    def _deposit_meta(self, meta, doc_type='seed'):
        if meta:
            req_str = self.meta_to_string(meta)
            result = self.redis.rpush('%s:%s' % (doc_type, self.site), req_str)
            self.log.info("deposit_meta\tsite:%s\tdoc_type:%s\turl:%s" % (self.site, doc_type, meta.url))
            return result
        else:
            return False

    def _deposit_seed_last_start_time(self):
        self.log.info("deposit_seed_last_start_time...")
        seeds_last_start_time = {}
        for seed_id, seed in self.seeds_info.items():
            last_start_time = seed.get('last_start_time', None)
            seeds_last_start_time[seed_id] = last_start_time
            self.log.info("seed_id:%s, last_start_time:%s" % (seed_id, last_start_time))

        try:
            self.redis.set("seeds:%s:last_start_time" % self.site, json.dumps(seeds_last_start_time))
            self.log.debug("deposit_seed_last_start_time\tsite:%s\tlast_start_time:%s" %
                           (self.site, seeds_last_start_time))
        except Exception, e:
            self.log.error("deposit_seed_last_start_time_error:%s" % e.message)

        self.log.info("deposit_seed_last_start_time end")

    def _resume_tasks(self):
        all_counter = 0
        urls_static = ""
        for doc_type in DOC_TYPES:
            resume_counter = 0
            key = 'resume:%s:%s' % (doc_type, self.site)
            for task in self.redis.lrange(key, 0, -1):
                req = self.string_to_req(task)
                self.log.debug(req)
                if req is None:
                    continue

                if doc_type == 'index':
                    if req.url in self.index_urls_dict:
                        continue
                    else:
                        priority_key = str(time.time())
                        self.index_urls_dict[req.url] = time.time()

                if doc_type == 'item':
                    if req.url in self.content_urls_dict:
                        continue
                    else:
                        priority_key = str(time.time())
                        self.content_urls_dict[req.url] = time.time()

                if len(self.cookies) > 0:
                    req.http_header = {}
                    cookie_index = random.randint(0, len(self.cookies)-1)
                    req.http_header['Cookie'] = self.cookies[cookie_index].get('cookie', "")

                if doc_type == 'seed':
                    self.seed_queue.put((priority_key, req))
                elif doc_type == 'index':
                    self.index_queue.put((priority_key, req))
                elif doc_type == 'item':
                    self.item_queue.put((priority_key, req))
                resume_counter += 1
                self.log.debug('resume_url\tsite:%s\turl:%s\tdownload_type:%s' %
                              (self.site, req.url, req.download_type))
            self.redis.delete(key)
            urls_static += "\t%s:%d" % (doc_type, resume_counter)
            all_counter += resume_counter
        self.log.info('resume_site\tsite:%s\turls:%d%s' % (self.site, all_counter, urls_static))

    def _resume_seed(self):
        key = 'seed:%s' % self.site
        for task in self.redis.lrange(key, 0, -1):
            meta = self.string_to_meta(task)
            if meta is None:
                continue
            self.seeds_meta[meta.url_id] = meta
            his_num = 0
            if meta.normal_crawl_his:
                his_num = len(meta.normal_crawl_his)
            priority = 6
            if self.seeds_info.has_key(meta.seed_id):
                seed_info = self.seeds_info[meta.seed_id]
                priority = int(seed_info.get("priority", 6) if seed_info.get("priority", 6) else 6)
            self.log.info('resume_seed\tsite:%s\turl:%s\this_num:%s\tpriority:%s' % (self.site, meta.url, his_num,
                                                                                     priority))
        self.redis.delete(key)
        self.log.info('resume_seed\tsite:%s\turls:%d' % (self.site, len(self.seeds_meta)))

    def _resume_seed_last_start_time(self):
        self.log.info("resume seeds last start time, site:%s" % self.site)
        key = 'seeds:%s:last_start_time' % self.site
        try:
            self.seeds_last_start_time = self.redis.get(key)
            self.seeds_last_start_time = json.loads(self.seeds_last_start_time)
            self.redis.delete(key)
        except Exception, e:
            self.log.info("resume seeds:%s last start time error:%s" % (self.site, e.message))

    def _resume_fail(self):
        key = 'fail:%s' % self.site
        for task in self.redis.lrange(key, 0, -1):
            meta = self.string_to_meta(task)
            if meta is None:
                continue
            his_num = 0
            if meta.normal_crawl_his:
                his_num = len(meta.normal_crawl_his)
            self.fails_meta[meta.url_id] = meta
            self.log.info('resume_fail\tsite:%s\turl:%s\this_num:%s' % (self.site, meta.url, his_num))

    def _resume_counter(self):
        key = 'site:%s' % self.site
        try:
            self.counter = int(self.redis.hget(key, "counter"))
        except Exception, e:
            self.log.error("resume counter fail:%s" % e.message)
        
    def ensure_stopped(self):
        return self.stopped and time.time() - self.stop_time > 60

    @with_after_effect('_report_status')
    def save_status(self):
        self.log.info("save %s status to redis" % self.site)
        with threading.Lock():
            for task in self.index_queue.queue:
                self._deposit_task(task[1], 'index')
            self.index_queue = PriorityQueue()
            for task in self.item_queue.queue:
                self._deposit_task(task[1], 'item')
            self.item_queue = PriorityQueue()
            for url_id, meta in self.seeds_meta.items():
                self._deposit_meta(meta, 'seed')
            self.seed_queue = PriorityQueue()
            # 保存种子上次启动时间，在异常退出后重新启动根据period决定是否重新加载种子配置
            self._deposit_seed_last_start_time()
            self.log.info("save  %s status to redis completely" % self.site)

    @with_after_effect('_report_status')
    def stop(self):
        self.log.info("stop_site\tsite:%s\tstopping" % self.site)
        self.stopped = True
        self.stop_time = time.time()
        with self.wlock:
            #for task in self.index_queue.queue:
            #    self._deposit_task(task[1], 'index')
            self.index_queue = PriorityQueue()

            for task in self.item_queue.queue:
                self._deposit_task(task[1], 'item')
            self.item_queue = PriorityQueue()

            # for url_id, meta in self.seeds_meta.items():
            #     self._deposit_meta(meta, 'seed')
            self.seed_queue = PriorityQueue()

            # self._deposit_seed_last_start_time()

        self.log.info("stop_site\tsite:%s\tstopped" % self.site)
        return True

    @with_after_effect('_report_status')
    def resume(self, seeds):
        self.log.info("resume_site\tsite:%s\tstart" % self.site)
        self._resume_seed_last_start_time()
        self.log.info("seeds last start time resume info: %s" % self.seeds_last_start_time)
        with self.wlock:
            if not self.stopped:
                for (seed_id, seed) in seeds.items():
                    self.seeds_info[seed_id] = seed
                    if self.seeds_last_start_time == {} or self.seeds_last_start_time is None:
                        self.seeds_last_start_time = {}
                        self.log.warning("resume seeds last start time: null")
                        # self.add_seed_json(seed)

                    last_start_time = self.seeds_last_start_time.get(str(seed_id), None)
                    seed['last_start_time'] = last_start_time
                    if last_start_time is None:
                        self.add_seed_json(seed)

                    config_init_period = seed.get('config_init_period', None)
                    if config_init_period and config_init_period.get('is_period',
                                                                     None) == 'false' or config_init_period is None:
                        continue

                    period = int(config_init_period.get('period', 0))
                    interval = 0
                    if period in [1, 2, 3, 4, 5, 6, 7]:
                        interval = self.period_interval[period - 1]

                    if period == 0:
                        interval = config_init_period.get('custom', 3600)

                    if (last_start_time is not None) and interval > 0:
                        next_load_time = interval + last_start_time
                        if next_load_time < time.time():
                            self.add_seed_json(seed)

                self._resume_tasks()  # 历史抓取url
                self._resume_seed()  # 种子抓取历史
                self._resume_fail()  # 下载失败任务
                self._resume_counter()  # 恢复站点已抓取

        self.log.info("resume_site\tsite:%s\tfinish" % self.site)
        return True

    @with_after_effect('_report_status')
    def start(self, seeds):
        self.log.info("start_site\tsite:%s\tstarting" % self.site)
        # self._resume_seed_last_start_time()
        self.wlock.acquire()
        if self.stopped:
            self.stopped = False
            for (seed_id, seed) in seeds.items():
                self.add_seed_json(seed)

            # self._resume_tasks()  # 历史抓取url
            # self._resume_seed()  # 种子抓取历史
            # self._resume_fail()  # 下载失败任务
        self.wlock.release()
        self.log.info("start_site\tsite:%s\tfinish" % self.site)
        return True

    def _construct_turn_urls(self, seed, url):
        req_url_index = url.find('?')
        path_url = url
        params_dict = {}
        post_data = seed.get('data', {})
        if req_url_index > 0:
            path_url = url[0:req_url_index]
            params_url = url[req_url_index+1:]
            params_dict = url_query_decode(params_url)

        turn_page = False
        turn_urls = []
        if seed.has_key('page_turning_rule'):
            page_turning_rule = seed['page_turning_rule']
            page_turning_type = page_turning_rule.get('type', "")
            page_turning_data = page_turning_rule.get('data', {})
            count_param = page_turning_data.get('count_param', "")
            if page_turning_data and len(count_param) > 0:
                start_param = int(page_turning_data.get('start_param', "-1"))
                count_interval = int(page_turning_data.get('count_interval', "1"))
                end_condition = int(page_turning_data.get('end_condition', "-1"))
                while start_param <= end_condition:
                    if page_turning_type == "get_count":
                        turn_dict = copy.copy(params_dict)
                        turn_dict[count_param] = start_param
                        params_str = urllib.urlencode(turn_dict)
                        turn_url = "%s?%s" % (path_url, params_str)
                        turn_urls.append((turn_url, post_data))
                        turn_page = True
                    elif page_turning_type == "post_count":
                        turn_post = copy.copy(post_data)
                        turn_post[count_param] = start_param
                        turn_url = url + "\t" + str(start_param)
                        turn_urls.append((turn_url, turn_post))
                        turn_page = True
                    elif page_turning_type == "reg_count":
                        regex = re.compile(count_param)
                        items = regex.findall(url)
                        if items is not None and len(items) > 0:
                            this_item = items[0][0]
                            this_num = items[0][1]
                            next_item = this_item.replace(this_num, str(start_param))
                            turn_url = url.replace(this_item, next_item)
                            turn_urls.append((turn_url, post_data))
                            turn_page = True
                    start_param += count_interval
                    # if len(turn_urls) > 2000:
                    #     break

        if not turn_page:
            turn_urls.append((url, post_data))

        return turn_urls

    def _construct_download_req(self, seed, url):
        seed_id = int(seed.get('seed_id', "0") if seed.get('seed_id', "0") else "0")
        method = seed.get('method', 'get')

        download_req = DownLoadReq()
        download_req.method = method
        download_req.url = url
        if seed.has_key('http_header'):
            download_req.http_header = seed['http_header']
        if len(self.cookies) > 0:
            if not download_req.http_header:
                download_req.http_header = {}
            cookie_index = random.randint(0, len(self.cookies) - 1)
            download_req.http_header['Cookie'] = self.cookies[cookie_index].get('cookie', "")
        if seed.has_key('session_commit') and seed['session_commit']:
            download_req.session_commit = SessionCommit()
            download_req.session_commit.refer_url = seed['session_commit'].get("refer_url", "")
            download_req.session_commit.identifying_code_url = seed['session_commit'].get("identifying_code_url", "")
            download_req.session_commit.identifying_code_check_url = seed['session_commit'].get(
                "identifying_code_check_url", "")
            download_req.session_commit.check_body = seed['session_commit'].get("check_body", "")
            download_req.session_commit.check_body_not = seed['session_commit'].get("check_body_not", "")
            download_req.session_commit.session_msg = seed['session_commit'].get("session_msg", {})
            download_req.session_commit.need_identifying = False
            need_identifying = seed['session_commit'].get("need_identifying", "False")
            if need_identifying == "True":
                download_req.session_commit.need_identifying = True

        scheduler_info = {}
        scheduler_info["schedule_time"] = time.time()
        scheduler_info["seed_id"] = seed_id
        download_req.scheduler = json.dumps(scheduler_info)
        if seed.has_key('proxy_time') and seed['proxy_time']:
            try:
                proxy_time = int(seed['proxy_time'])
                if proxy_time <= 0:
                    download_req.use_proxy = False
            except:
                self.log.error("proxy_time must is int")

        return download_req

    @with_after_effect('_report_status')
    def add_seed_json(self, seed):
        url = seed.get('url', "")
        seed_id = int(seed.get('seed_id', "0") if seed.get('seed_id', "0") else "0")
        seed_name = seed.get('name')
        seed['last_start_time'] = time.time()
        if len(url) <= 0 or seed_id <= 0:
            self.log.error("add_seed\turl:empty\tseed:%s" % (json.dumps(seed)))
            return False
        self.seeds_info[seed_id] = seed
        seed['start_time'] = time.time()
        is_seed = False
        if len(seed['variable_param_list']) <= 10:
            is_seed = True

        method = seed.get('method', 'get')
        # 对url进行切割，得出path_url 与 params_dict
        try: 
            turn_urls = self._construct_turn_urls(seed, url)
        except Exception, e:
            self.log.error("construct turn urls fail:%s" % traceback.format_exc())
            turn_urls = []

        if len(seed['variable_param_list']) == 0:
            seed['variable_param_list'].append({})

        turn_url_index = 0
        for (turn_url, post_value) in turn_urls:
            turn_url_index += 1
            if turn_url_index > 3:
                is_seed = False

            pars = turn_url.split("\t")
            url = pars[0]
            for variable_param in seed['variable_param_list']:
                data = copy.copy(post_value)
                try:
                    for (key, value) in variable_param.items():
                        if value is None or len(value) <= 0:
                            continue
                        if self.encoding and method == "get":
                            data[key] = value.encode(self.encoding)
                        else:
                            data[key] = value.encode('utf8')
                except Exception, e:
                    self.log.error("variable_param\terror:%s" % (str(e)))
                    continue

                download_req = self._construct_download_req(seed, url)

                download_req.src_type = "seed"
                if method == "get":
                    req_url_index = download_req.url.find('?')
                    if req_url_index > 0:
                        params_url = download_req.url[req_url_index + 1:]
                        path_url = download_req.url[0:req_url_index]
                        params_dict = url_query_decode(params_url)
                        for (key, value) in params_dict.items():
                            if not data.has_key(key):
                                data[key] = value

                        params_str = urllib.urlencode(data)
                        req_url = "%s?%s" % (path_url, params_str)
                        download_req.url = req_url

                elif method == "post":
                    download_req.post_data = data

                if download_req.url is not None:
                    if method == "post":
                        download_req.url = build_hzpost_url(download_req.url, download_req.post_data)
                        download_req.post_data = {}
                    # print download_req
                    download_req.download_type = seed.get('download_type', 'simple')
                    priority_key = str(time.time())
                    self.index_urls_dict[download_req.url] = time.time()
                    if is_seed:
                        meta = self.add_seed(download_req.url, seed_id);
                        self.seed_queue.put((priority_key, self.create_download_req(meta)))
                    else:
                        self.index_queue.put((priority_key, download_req))

                    self.seed_statistic.inc_download_count(self.site, seed_id, seed_name)
                    self.log.info("seed_config\turl:%s\tseed:%s\tdownload_type:%s" %
                                  (download_req.url, is_seed, download_req.download_type))

        # seed.pop('url')
        # if seed.has_key("origin_url"):
        #     seed.pop('origin_url')
        # 日志输出
        self.log.info('seed_config' +
                      '\tsite:' + str(self.site) +
                      '\tseed_id:' + str(seed_id) +
                      '\tseeds:' + str(len(self.seeds_meta)) +
                      '\tseed_query:' + str(self.seed_queue.qsize()) +
                      '\tindex_query:' + str(self.index_queue.qsize()) +
                      '\titem_query:' + str(self.item_queue.qsize()))

        return True

    def add_seed(self, url, seed_id):
        url_id = get_md5_i64(url)
        link_info = LinkAttr()
        link_info.url = url
        link_info.url_id = url_id
        link_info.src_type = "seed"
        link_info.found_time = time.time()
        link_info.depth = 1
        link_info.seed_id = seed_id
        if self.seeds_meta.has_key(link_info.url_id):
            meta = self.seeds_meta[url_id]
            meta.src_type = link_info.src_type
            meta.depth = link_info.depth
            meta.seed_id = link_info.seed_id
        else:
            self.seeds_meta[url_id] = link_info
            meta = self.seeds_meta[url_id]
        return meta
 
    def create_download_req(self, meta, post_data={}):
        download_req = DownLoadReq()
        download_req.url = meta.url
        download_req.post_data = {}
        download_req.src_type = meta.src_type
        scheduler_info = {}
        scheduler_info["schedule_time"] = time.time()
        scheduler_info["seed_id"] = meta.seed_id
        download_req.scheduler = json.dumps(scheduler_info)
        if self.seeds_info.has_key(meta.seed_id):
            seed = self.seeds_info[meta.seed_id]
            download_req.method = seed.get('method', 'get')
            download_req.download_type = seed.get('download_type', 'simple')
            if seed.has_key('http_header'):
                download_req.http_header = seed['http_header']
            if seed.has_key('session_commit') and seed['session_commit']:
                download_req.session_commit = SessionCommit()
                download_req.session_commit.refer_url = seed['session_commit'].get("refer_url", "")
                download_req.session_commit.identifying_code_url = seed['session_commit'].get("identifying_code_url", "")
                download_req.session_commit.identifying_code_check_url = seed['session_commit'].get("identifying_code_check_url", "")
                download_req.session_commit.check_body = seed['session_commit'].get("check_body", "")
                download_req.session_commit.check_body_not = seed['session_commit'].get("check_body_not", "")
                download_req.session_commit.session_msg = seed['session_commit'].get("session_msg", {})
                download_req.session_commit.need_identifying = False
                need_identifying = seed['session_commit'].get("need_identifying", "False")
                if need_identifying == "True":
                    download_req.session_commit.need_identifying = True
            if seed. has_key('proxy_time') and seed['proxy_time']:
                try:
                    proxy_time = int(seed['proxy_time'])
                    if proxy_time <= 0:
                        download_req.use_proxy = False
                except:
                    self.log.error("proxy_time must is int")
            if download_req.method == "post":
                for (key, value) in post_data.items():
                    download_req.post_data[key.encode("utf-8")] = value.encode('utf-8')
                if download_req.post_data:
                    download_req.url = build_hzpost_url(download_req.url, download_req.post_data)
                #download_req.post_data = {}
        if len(self.cookies) > 0:
            if download_req.http_header:
                download_req.http_header = {}
                cookie_index = random.randint(0, len(self.cookies)-1)
                download_req.http_header['Cookie'] = self.cookies[cookie_index].get('cookie', "")
                
        return download_req

    @with_after_effect('_report_status')
    def schedule(self, task):
        if self.stopped:
            self.log.info('site_schedule\tsite:%s\tstatus:stopped\turl:%s' % (self.site, task.url))
            return True

        self.site_statistic.inc_response_count(self.site, self.site_name)
        self.wlock.acquire()
        try:
            link_num = 0
            if task.crawl_info and task.seed_id == None:
                task.seed_id = task.crawl_info.seed_id
            if task.sub_links:
                link_num = len(task.sub_links)
            self.log.info("start_schedule\turl:%s\tstatus:%d\tsub:%d\tseed_id:%s" %
                          (task.url, task.crawl_info.status_code, link_num, task.crawl_info.seed_id))

            if task.crawl_info and task.crawl_info.content_type == "content_link":
                if task.seed_id:
                    seed_id = task.seed_id
                    seed_name = self.seeds_info[seed_id]["name"]
                    self.seed_statistic.inc_download_content_success_count(self.site, seed_id, seed_name)

            queue = None
            meta = None
            folow_download_type = 'simple'
            if self.seeds_info.has_key(task.seed_id):
                seed = self.seeds_info[task.seed_id]
                folow_download_type = seed.get('item_download_type', 'simple')

            if self.seeds_meta.has_key(task.url_id):
                meta = self.seeds_meta[task.url_id]
                self.merge_seed_meta(meta, task, 'seed')
                if task.crawl_info.status_code != 0:
                    fail_num_hours = 0
                    if meta.normal_crawl_his:
                        for his in meta.normal_crawl_his:
                            if his.status_code != 0 and his.download_time > time.time() - 3600:
                                fail_num_hours += 1
                    if meta.normal_crawl_his and fail_num_hours > 3 or task.crawl_info.status_code == 5 and \
                                    task.crawl_info.content_type is not None and \
                            not 0 >= task.crawl_info.content_type.find('json'):
                        self.log.info("site_schedule\turl:%s\tdoc_type:seed\tfail_num_hours:%s\t"
                                      "his_num:%d\tnot_crawl" %
                                      (task.url, fail_num_hours, len(meta.normal_crawl_his)))
                    else:
                        priority_key = str(time.time())
                        req = self.create_download_req(meta)
                        self.seed_queue.put((priority_key, req))
                        self.log.info("site_schedule\turl:%s\tdoc_type:%s\tto_crawl" % (task.url, "seed"))
            elif task.crawl_info.status_code != 0:
                # self.site_statistic.inc_fail_count(self.site, self.site_name)
                if not self.fails_meta.has_key(task.url_id):
                    self.fails_meta[task.url_id] = task
                fail_meta = self.fails_meta[task.url_id]
                fail_num_hours = 0
                if fail_meta.normal_crawl_his:
                    for his in fail_meta.normal_crawl_his:
                        if his.status_code != 0 and his.download_time > time.time() - 3600:
                            fail_num_hours += 1

                if fail_num_hours < 3:
                    priority_key = str(time.time())
                    self.merge_seed_meta(fail_meta, task, 'fail')
                    req = self.create_download_req(fail_meta)
                    self.index_queue.put((priority_key, req))
                    self.log.info("site_schedule\turl:%s\tfail_num_hours:%d\this_num:%s\tto_crawl" %
                                  (task.url, fail_num_hours, len(fail_meta.normal_crawl_his)))
                else:
                    self.log.info("site_schedule\turl:%s\tfail_num_hours:%d\this_num:%s\tnot_crawl" %
                                  (task.url, fail_num_hours, len(fail_meta.normal_crawl_his)))
            # 对子链接进行判断
            sub_link_num = 0
            content_link_num = 0
            download_link_num = 0
            index_link_num = 0
            not_type_link = 0
            if task.crawl_info.status_code == 0:
                self.site_statistic.inc_success_count(self.site, self.site_name)
                if task.seed_id:
                    seed_id = task.seed_id
                    seed_name = self.seeds_info[seed_id]["name"]
                    self.seed_statistic.inc_download_success_count(self.site, seed_id, seed_name)
            else:
                self.site_statistic.inc_fail_count(self.site, self.site_name)
                if task.seed_id:
                    seed_id = task.seed_id
                    seed_name = self.seeds_info[seed_id]["name"]
                    self.seed_statistic.inc_download_fail_count(self.site, seed_id, seed_name)

            if task.crawl_info.status_code == 0 and task.sub_links:
                for sub_link in task.sub_links:
                    if sub_link.type > 0:
                       self.log.info("new_sub_link\turl:%s\tparent:%s\ttype:%d\tnew:%s" %
                                     (sub_link.url, task.url, sub_link.type, sub_link.is_new))

                    if sub_link.url in self.index_urls_dict:
                        self.log.warning("sub_link is not new, find in index_urls_dict.\turl:%s" % sub_link.url)
                        continue

                    if sub_link.url in self.content_urls_dict:
                        self.log.warning("sub_link is not new, find in content_urls_dict.\turl:%s" % sub_link.url)
                        continue

                    if sub_link.is_new is False and sub_link.type == LinkType.kContentLink:
                        self.log.warning("content_sub_link is not new.\turl:%s" % sub_link.url)
                        continue

                    sub_link_num += 1
                    download_req = DownLoadReq()
                    download_req.url = sub_link.url
                    download_req.parse_extends = sub_link.parse_extends
                    if task.seed_id:
                        scheduler_info = {}
                        scheduler_info["schedule_time"] = time.time()
                        scheduler_info["seed_id"] = task.seed_id
                        if sub_link.type == LinkType.kContentLink:
                            scheduler_info['content_type'] = 'content_link'

                        download_req.scheduler = json.dumps(scheduler_info)

                    priority_key = str(time.time())
                    if len(self.cookies) > 0:
                        download_req.http_header = {}
                        cookie_index = random.randint(0, len(self.cookies)-1)
                        download_req.http_header['Cookie'] = self.cookies[cookie_index].get('cookie', "")
                    if sub_link.type == LinkType.kContentLink:
                        self.content_urls_dict[sub_link.url] = time.time()
                        content_link_num += 1
                        if meta:
                            download_req.src_type = "seed_follow"
                        download_req.download_type = folow_download_type
                        self.item_queue.put((priority_key, download_req))
                        if task.seed_id:
                            seed_id = task.seed_id
                            seed_name = self.seeds_info[seed_id]["name"]
                            self.seed_statistic.inc_content_page_count(self.site, seed_id, seed_name)

                    elif sub_link.type == LinkType.kSectionTitleLink or sub_link.type == LinkType.kTurnPageLink or \
                                    sub_link.type == LinkType.kNextPageLink:
                        self.index_urls_dict[sub_link.url] = time.time()
                        index_link_num += 1
                        if meta:
                            download_req.src_type = "seed_follow"
                        self.index_queue.put((priority_key, download_req))
                    elif sub_link.type == LinkType.kDownLoadLink:
                        self.content_urls_dict[sub_link.url] = time.time()
                        download_link_num += 1
                        download_req.src_type = "doc"
                        self.item_queue.put((priority_key, download_req))
                    else:
                        not_type_link += 1

            # self.get_next_page(task, content_link_num)

            self.log.info("schedule_sublink\turl:%s\tcontent:%s\tindex:%d\tdownload:%d\tnottype:%d" %
                        (task.url, content_link_num, index_link_num, download_link_num, not_type_link))
        except:
            self.log.error('site_schedule\t' + str(traceback.format_exc()))
        finally:
            self.wlock.release()
            self.log.info('site_schedule' +
                          '\tsite:' + str(self.site) +
                          '\tseeds:' + str(len(self.seeds_meta)) +
                          '\tseed_query:' + str(self.seed_queue.qsize()) +
                          '\tindex_query:' + str(self.index_queue.qsize()) +
                          '\titem_query:' + str(self.item_queue.qsize()))

    def get_next_page(self, task, content_link_num):
        seed_id = task.crawl_info.seed_id
        if seed_id is None:
            return

        seed = self.seeds_info[seed_id]
        method = seed.get('method', 'get')

        if seed.has_key('page_turning_rule'):
            page_turning_rule = seed['page_turning_rule']
            page_turning_type = page_turning_rule.get('type', "")
            page_turning_data = page_turning_rule.get('data', {})
            count_param = page_turning_data.get('count_param', "")
            if page_turning_data and len(count_param) > 0:
                count_interval = int(page_turning_data.get('count_interval', "1"))
                end_condition = int(page_turning_data.get('end_condition', "-1"))

                def _add_seed(_page, _seed, _url, _end_condition, _method, _content_link_num):
                    if 2000 <= _page <= _end_condition or _content_link_num > 0:

                        download_req = self._construct_download_req(_seed, _url)
                        download_req.src_type = "seed"
                        if download_req.url is not None and (download_req.url not in self.index_urls_dict):
                            if _method == "post":
                                download_req.post_data = {}

                            download_req.download_type = seed.get('download_type', 'simple')
                            priority_key = str(time.time())
                            self.index_urls_dict[download_req.url] = time.time()
                            self.index_queue.put((priority_key, download_req))
                            self.log.info("get_next_page\turl:%s\tdownload_type:%s\tpage:%d" %
                                          (download_req.url, download_req.download_type, _page))

                if page_turning_type == 'post_count':
                    hz_post_url = extract_hzpost_url(task.url)
                    url = hz_post_url.get('url', '')
                    post_data = hz_post_url.get('postdata', {})
                    page = post_data.get(count_param, None)
                    if page is None:
                        return

                    page = int(page)
                    post_data['%s' % count_param] = page + count_interval
                    req_url = build_hzpost_url(url, post_data)
                    self.log.info("post_count req_url:%s, page:%s" % (req_url, page))
                    _add_seed(page, seed, req_url, end_condition, method, content_link_num)

                elif page_turning_type == 'get_count':
                    req_url_index = task.url.find('?')
                    if req_url_index > 0:
                        url = task.url[0:req_url_index]
                        query_params = task.url[req_url_index + 1:]
                        params_dict = url_query_decode(query_params)
                        page = params_dict.get(count_param, None)
                        if page is None:
                            return

                        page = int(page)
                        params_dict['%s' % count_param] = page + count_interval
                        params_str = urllib.urlencode(params_dict)
                        req_url = "%s?%s" % (url, params_str)
                        self.log.info("get_count req_url:%s, page:%s" % (req_url, page))
                        _add_seed(page, seed, req_url, end_condition, method, content_link_num)

                elif page_turning_type == 'reg_count':
                    regex = re.compile(count_param)
                    items = regex.findall(task.url)
                    if items is not None and len(items) > 0:
                        this_item = items[0][0]
                        page = items[0][1]
                        if page is None:
                            return

                        page = int(page)
                        next_item = this_item.replace(str(page), str(page+count_interval))
                        req_url = task.url.replace(this_item, next_item)
                        self.log.info("reg_count req_url:%s, page:%s" % (req_url, page))
                        _add_seed(page, seed, req_url, end_condition, method, content_link_num)

    def merge_seed_meta(self, meta_in_site, meta, merge_type):
        if meta.found_time and meta.found_time < meta_in_site.found_time:
            meta_in_site.found_time = meta.found_time
        if not meta_in_site.normal_crawl_his:
            meta_in_site.normal_crawl_his = []
        his_before = len(meta_in_site.normal_crawl_his)
        if meta.normal_crawl_his:
            if not meta_in_site.normal_crawl_his:
                meta_in_site.normal_crawl_his.append(meta.normal_crawl_his[0])
            else:
                if meta_in_site.normal_crawl_his[0].download_time < meta.normal_crawl_his[0].download_time:
                    meta_in_site.normal_crawl_his.insert(0,  meta.normal_crawl_his[0])
        meta_in_site.normal_crawl_his = meta_in_site.normal_crawl_his[:7]
        his_after = len(meta_in_site.normal_crawl_his)
        self.log.info("merge_seed_meta\ttype:%s\turl:%s\this_before:%s\this_after:%s" %
                      (merge_type, meta.url, his_before, his_after))

        if meta.depth:
            if not meta_in_site.depth:
                meta_in_site.depth = meta.depth
            elif meta_in_site.depth > meta.depth:
                meta_in_site.depth = meta.depth
        if meta.crawl_info:
            #schedule_time = meta_in_site.crawl_info.schedule_time
            meta_in_site.crawl_info = meta.crawl_info
            if not meta_in_site.crawl_info.schedule_time:
                meta_in_site.crawl_info.schedule_time = time.time()

    def site_static_log(self):
        self.log.info('SiteScheduler.schedule' +
                      '\tsite:' + str(self.site) +
                      '\tseeds:' + str(len(self.seeds_meta)) +
                      '\tseed_query:' + str(self.seed_queue.qsize()) +
                      '\tindex_query:' + str(self.index_queue.qsize()) +
                      '\titem_query:' + str(self.item_queue.qsize()))

    @with_after_effect('_report_status')
    def dispatch(self):
        #if not self.isempty():
        #    self.log.info('dispatch' +
        #                  '\tsite:' + str(self.site) +
        #                  '\tseed:' + str(self.seed_queue.qsize()) +
        #                  '\tindex:' + str(self.index_queue.qsize()) +
        #                  '\titem:' + str(self.item_queue.qsize()))

        if len(self.index_urls_dict) > 100000:
            now_time = int(time.time())
            del_before = len(self.index_urls_dict)
            for key, value in self.index_urls_dict.items():
                if value < now_time - 36000:
                    del self.index_urls_dict[key]

            del_after = len(self.index_urls_dict)
            self.log.info('remove_cache\tsite:%s\tbefore:%d\tafter:%d' % (self.site, del_before, del_after))
        queue_choices = (self.item_queue, self.index_queue, self.seed_queue)
        for queue in queue_choices:
            try:
                priority, task = queue.get_nowait()
                #self.next_dispatch_time += self.avg_interval 
                self.counter += 1
                self.site_statistic.inc_request_count(self.site, self.site_name)
                return task
            except Empty:
                continue
        return None

    def isempty(self):
        if self.seed_queue.qsize() == 0 and \
           self.index_queue.qsize() == 0 and \
           self.item_queue.qsize() == 0:
            return True
        else:
            return False


def usage():
    pass


def main(conf):
    """
    # 测试百度，可变参数
    site_info = {};
    site_info['site_id'] = 5409283743049397654;
    site_info['avg_interval'] = 4;
    site_info['site'] = 'tieba.baidu.com';
    site_info['name'] = '百度贴吧';
    site_scheduler = SiteScheduler(site_info, conf.redis_tasks, conf.log);
    loader = SchedulerConfigLoader(conf.mysql_conf, conf.mongodb_conf, conf.redis_tasks, conf.log);
    seeds = loader.load_seeds(site_info['site']);
    for (seed_id, seed) in seeds.items():
        site_scheduler.add_seed_json(seed);
    # 测试普通页面
    site_info = {};
    site_info['site_id'] = 2942287653800985910;
    site_info['avg_interval'] = 4;
    site_info['site'] = 'www.zjsfgkw.cn';
    site_info['name'] = '浙江法庭';
    site_scheduler = SiteScheduler(site_info, conf.redis_tasks, conf.log);
    loader = SchedulerConfigLoader(conf.mysql_conf, conf.mongodb_conf, conf.redis_tasks, conf.log);
    sites = {};
    seeds = loader.load_seeds(site_info['site']);
    for (seed_id, seed) in seeds.items():
        site_scheduler.add_seed_json(seed);
    """
    # 测试普通页面
    site_info = {};
    site_info['site'] = 'epub.sipo.gov.cn'
    site_info['site_id'] = get_md5_i64(site_info['site'])
    site_info['avg_interval'] = 4
    site_info['name'] = 'epub.sipo.gov.cn'
    site_info['encoding'] = 'gbk'
    site_scheduler = SiteScheduler(site_info, conf['redis_tasks'], conf['log'])
    loader = SchedulerConfigLoader(conf['mysql_conf'], conf['mongodb_conf'], conf['redis_tasks'], conf['log'])
    sites = {}
    seeds = loader.load_seeds(site_info['site'])
    site_scheduler.start(seeds)
    site_scheduler.stop()
    """
    # 测试普通页面
    site_info = {};
    site_info['site'] = 'www.bjztb.gov.cn';
    site_info['site_id'] = get_md5_i64(site_info['site']);
    site_info['avg_interval'] = 4;
    site_info['name'] = 'www.bjztb.gov.cn';
    site_scheduler = SiteScheduler(site_info, conf.redis_tasks, conf.log);
    loader = SchedulerConfigLoader(conf.mysql_conf, conf.mongodb_conf, conf.redis_tasks, conf.log);
    sites = {};
    seeds = loader.load_seeds(site_info['site']);
    site_scheduler.start(seeds);
    #site_scheduler.stop();
    """


if __name__ == '__main__':
    try:
        file_path = './scheduler.toml'
        opt, args = getopt.getopt(sys.argv[1:], 'f:', ['help'])
        for name, value in opt:
            if name == "-f":
                file_path = value
            elif name in ("-h", "--help"):
                usage()
                sys.exit()
            else:
                assert False, "unhandled option"

        with open(file_path, 'rb') as config:
            conf = pytoml.load(config)
            conf['log'] = LogHandler(conf['log_name'])

        main(conf)
    except getopt.GetoptError:
        sys.exit()


