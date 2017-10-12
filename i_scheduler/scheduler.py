# -*- coding=utf-8 -*-
import json
import sys
from redis import StrictRedis
from Queue import PriorityQueue, Queue, Empty
import time, traceback
import functools
from selector import Selector
from config_loader import SchedulerConfigLoader
from site_scheduler import SiteScheduler
from crawl_selector import CrawlSelector
from site_statistic import SiteStatistics
from seed_statistic import SeedStatistics

sys.path.append('..')
from i_util.tools import get_md5_i64, get_url_info


class SiteSchedulerManager(object):

    def __init__(self, conf, site_statistic, seed_statistic):
        self.conf = conf
        self.site_statistic = site_statistic
        self.seed_statistic = seed_statistic
        self.log = conf.get('log')
        # 初始化队列信息
        self.site_queue = PriorityQueue()
        self.dispatched_task_queue = Queue()
        # 初始化配置加载器,包括种子与站点
        self.loader = SchedulerConfigLoader(conf, conf['mongodb_conf'], conf['redis_tasks'], conf['log'])
        self.log.info("开始加载种子信息...")
        self.sites = self.loader.load_sites()
        self.log.info("加载种子信息完成...")
        #self.seeds = self.loader.load_seeds(self.sites);
        self.site_empty = {}
        self.site_schedulers = {}
        self.running_site_schedulers = {}
        # # 加载抓取失败数据
        # self.fail_infos = self.loader.load_fail_task()
        # # 加载种子调度信息
        # self.scheduler_meta = self.loader.load_scheduler_mata()

    def save_status(self):
        for site_id, site_scheduler in self.running_site_schedulers.items():
            site_scheduler.save_status()

    def stop(self):
        for site_id, site_scheduler in self.running_site_schedulers.items():
            site_scheduler.stop()

    def start(self):
        for site_info in self.sites.values():
            site_id = int(site_info['site_id'])
            site_scheduler = SiteScheduler(site_info, self.conf['redis_tasks'], self.conf['log'], self.site_statistic,
                                           self.seed_statistic)
            self.site_schedulers[site_id] = site_scheduler
            if not site_scheduler.stopped:
                seeds = self.loader.load_seeds(site_info['site'])
                site_scheduler.resume(seeds)
                self.site_queue.put((site_scheduler.next_dispatch_time, site_scheduler))
                self.running_site_schedulers[site_id] = site_scheduler
                self.log.info("start\tsite:%s" % site_info['site'])
            else:
                # self.log.info("start\tsite:%s\tstatus:stop" % site_info['site'])
                pass

    def restart_seed(self, seed_id, site):
        self.log.info('restart seed, seed_id:%s, site:%s' % (seed_id, site))
        site_id = get_md5_i64(site)
        seeds = self.loader.load_seed_by_id(seed_id, True)
        site_scheduler = self.site_schedulers.get(site_id, None)
        if not site_scheduler:
            return True

        site_scheduler.add_seed_json(seeds)
        return True

    def clear_one_site_cache(self, site):
        site_id = get_md5_i64(site)
        site_scheduler = self.site_schedulers.get(site_id, None)
        if not site_scheduler:
            self.log.warning("clear on site cache fail, site:%s, site_id:%s, site_scheduler_key:%s" %
                             (site, site_id, self.site_schedulers.keys()))
            return True

        try:
            site_scheduler.clear_one_site_cache()
        except Exception, e:
            self.log.error("clear site:%s cache error: %s" % (site, traceback.format_exc()))
            return False

        return True

    def start_one_site_tasks(self, site):
        site_id = get_md5_i64(site)
        self.log.info("start on site:%s, site_id:%s" % (site, site_id))
        site_scheduler = self.site_schedulers.get(site_id, None)
        if not site_scheduler:
            self.log.info("当前需要加载站点信息: site = {}".format(site))
            sites = self.loader.load_sites(site)
            self.log.info("加载站点信息完成: site = {}".format(site))
            if sites and len(sites) > 0:
                site_info = sites[site]
                site_id = int(site_info['site_id'])
                site_scheduler = SiteScheduler(site_info, self.conf['redis_tasks'], self.conf['log'],
                                               self.site_statistic, self.seed_statistic)
                self.site_schedulers[site_id] = site_scheduler
        else:
            try:
                self.log.info("当前需要加载站点信息: site = {}".format(site))
                sites = self.loader.load_sites(site)
                self.log.info("加载站点信息完成: site = {}".format(site))
                if sites and len(sites) > 0 and sites.has_key(site):
                    site_info = sites[site]
                    site_scheduler.reload_site(site_info)
            except Exception, e:
                self.log.error("load sites info fail:%s" % traceback.format_exc())
                return False

        if not site_scheduler:
            return False

        if site_scheduler.stopped:
            seeds = self.loader.load_seeds(site_scheduler.site)
            site_scheduler.start(seeds)
            site_scheduler.stopped = False
            site_scheduler.next_dispatch_time = time.time()
            self.site_queue.put((site_scheduler.next_dispatch_time, site_scheduler))
            self.running_site_schedulers[site_id] = site_scheduler
        return True

    def stop_one_site_tasks(self, site):
        self.log.info('stop site:%s' % site)
        site_id = get_md5_i64(site)
        site_scheduler = self.site_schedulers.get(site_id, None)
        if not site_scheduler:
            self.log.info("stop_one_site_tasks\tsite:%s\t not_exist" % site)
            return False

        try:
            self.running_site_schedulers.pop(site_id)
        except KeyError, e:
            self.log.warning("site_id:%s not in self.running_site_schedulers:%s" %
                             (site_id, self.running_site_schedulers))
            pass

        return site_scheduler.stop()

    def dispatch_site(self):
        next_dispatch_time, site_scheduler = self.site_queue.get()
        self.site_empty[site_scheduler.site_id] = site_scheduler.isempty()
        early = next_dispatch_time - time.time()
        if early > 0:
            self.site_queue.put((next_dispatch_time, site_scheduler))
            return None
        next_dispatch_time += site_scheduler.avg_interval
        if not site_scheduler.stopped:
            self.site_queue.put((next_dispatch_time, site_scheduler))
            self.log.info("dispatch_site\tsite:%s\tnext:%f" % (site_scheduler.site, next_dispatch_time))
        return site_scheduler

    def select_seed(self):
        for site_id, site_scheduler in self.running_site_schedulers.items():
            if not site_scheduler.stopped:
                site_scheduler.select_seed()

    def get_schedule_seeds(self):
        for site_id, site_scheduler in self.running_site_schedulers.items():
            if not site_scheduler.stopped:
                seed_ids = site_scheduler.get_schedule_seeds()
                for seed_id in seed_ids:
                    seed_info = self.loader.load_seed_by_id(seed_id)
                    if seed_info:
                        site_scheduler.add_seed_json(seed_info)
                if seed_ids:
                    self.log.info("schedule_seeds\tsite:%s\tseed_ids:%s" % (site_scheduler.site, seed_ids))

    def dispatch(self):
        tasks = []
        try:
            task = self.dispatched_task_queue.get_nowait()
            tasks.append(task)
            return tasks
        except Empty:
            pass
        now_time = time.time()
        for site_id, site_scheduler in self.site_schedulers.items():
            if not site_scheduler.stopped:
                url_size = 0
                self.log.info("dispatch_start\tsite:%s\tnext:%f" % (site_scheduler.site,
                                                                    site_scheduler.next_dispatch_time))

                if site_scheduler.next_dispatch_time < (now_time - site_scheduler.avg_interval * 20):
                    site_scheduler.next_dispatch_time = now_time - site_scheduler.avg_interval * 20

                while True:
                    task = None
                    if site_scheduler.next_dispatch_time <= now_time:
                        task = site_scheduler.dispatch()
                        site_scheduler.next_dispatch_time += site_scheduler.avg_interval

                    if task:
                        url_size += 1
                        tasks.append(task)
                    else:
                        break

                self.log.info("dispatch_end\tsite:%s\tsize:%d" % (site_scheduler.site, url_size))

        return tasks

    def schedule_task(self, task):
        # dispatched_task_queue is the special queue, having the highest priority
        if not task:
            return None
        site_info = get_url_info(task.url)
        site_scheduler = self.get_site_scheduler(site_info)

        if site_scheduler:
            self.site_empty[site_scheduler.site_id] = False
        else:
            site_info = self.sites.get(site_info['site_id'], {})
            if site_info == {}:
                return None
            site_info['avg_interval'] = 10
            site_scheduler = SiteScheduler(site_info, self.conf['redis_tasks'], self.conf['log'], self.site_statistic,
                                           self.seed_statistic)
            self.site_schedulers[site_info['site_id']] = site_scheduler
            self.start_one_site_tasks(site_info['site'])
            self.log.info('schedule_task\turl:%s\tsite:%s\tnot_exit' % (task.url,site_info['site']))

        return site_scheduler.schedule(task)

    def get_site_scheduler(self, site_info):
        site_id = site_info['site_id']
        site_scheduler = self.site_schedulers.get(site_id, None)
        return site_scheduler
        """
        if not (site_id in self.site_schedulers):
            site_info = self.sites.get(site_id, {})
            site_info['site_id'] = site_id
            site_info['site'] = site
            self.site_schedulers[site_id] = SiteScheduler(site_info, self.conf)
            next_dispatch_time = time.time()
            self.site_queue.put((next_dispatch_time, self.site_schedulers[site_id]))
            self.site_empty[site_id] = False
        return self.site_schedulers[site_id]
        """

    def isempty(self):
        if self.dispatched_task_queue.qsize() == 0:
            for site_id in self.site_empty.keys():
                if not self.site_empty[site_id]:
                    return False
            return True
        else:
            return False


class Scheduler(object):

    def __init__(self, conf):
        self.conf = conf
        self.log = conf.get('log')
        self.site_statistic = SiteStatistics(conf['site_task_collect_db'])
        self.seed_statistic = SeedStatistics(conf['seed_task_collect_db'])
        self.scheduler = SiteSchedulerManager(self.conf, self.site_statistic, self.seed_statistic)
        self.selector = Selector(self.scheduler, self.conf)
        self.crawl_selector = CrawlSelector(self.log, conf['selector_conf'],
                                            conf['beanstalk_conf'], self.scheduler)


    def schedule_task(self, task):
        if task:
            self.scheduler.schedule_task(task)

    def select_seed(self):
        try:
            self.scheduler.select_seed()
        except Exception:
            self.log.error('start_on_site_tasks\terror:%s' % str(traceback.format_exc()))
            return False

    def start_one_site_tasks(self, site):
        try:
            self.log.info('start_one_site_tasks\tsite:%s starting' % site)
            return self.scheduler.start_one_site_tasks(site)
        except Exception:
            self.log.error('start_on_site_tasks\tsite:%s\terror:%s' % (site, str(traceback.format_exc())))
            return False

    def stop_one_site_tasks(self, site):
        self.log.info('site:%s stopping...' % site)
        return self.scheduler.stop_one_site_tasks(site)

    def clear_one_site_cache(self, site):
        self.log.info('site:%s clear cache...' % site)
        return self.scheduler.clear_one_site_cache(site)

    def restart_seed(self, seed_id, site):
        self.log.info('seed:%s site:%s restart...' % (seed_id, site))
        return self.scheduler.restart_seed(seed_id, site)

    def start(self):
        self.scheduler.start()
        self.crawl_selector.start()
        self.selector.start()
        self.site_statistic.start()
        self.seed_statistic.start()
        self.log.info('scheduler start successful')

    def save_status(self):
        self.scheduler.save_status()
        self.selector.stop()
        self.crawl_selector.stop()
        self.site_statistic.stop()
        self.seed_statistic.stop()

    def stop(self):
        self.scheduler.stop()
        self.selector.stop()
        self.crawl_selector.stop()
        self.site_statistic.stop()
        self.seed_statistic.stop()
