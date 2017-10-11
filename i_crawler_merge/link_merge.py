#!/usr/bin/env python
#-*- coding:utf-8 -*-
import pickle
import time
import traceback
from copy import deepcopy

from bdp.i_crawler.i_crawler_merge.ttypes import LinkAttr, CrawlInfo as MergeCrawlInfo, CrawlHistory, \
    ExtractMessage
from i_util.tools import is_index_url, get_md5_i64, timestampToTimestr
from libs.constants import FRESH_STATUS


class LinkMerge():
    def __init__(self, link_connection, url_info, links, logger):
        self.logger = logger
        self.link_connection = link_connection
        if links is None:
            self.links = []
        else:
            self.links = links[:]
        self.url_info = dict(url_info)
        self.schedule_sublinks = []
        self.unkown_sub_links = []
        self.inner_links = []
        self.outer_links = []
        domain = url_info.get('domain')
        for link in self.links:
            if not link.type or link.type <= 0:
                continue
            if domain == link.domain:
                if not link.url == self.url_info['url']:
                    self.inner_links.append(link)
            elif link.type > 0:
                self.outer_links.append(link)
        cur_t = time.time()
        self.history_links_cache = self._fetch_all_links_history()
        self.logger.info("fetch {} length {} cost {}".format(url_info['url'],
                                                             len(self.inner_links)+len(self.outer_links),
                                                             time.time() - cur_t
                                                             ))
        self._merge_schedule_sublinks(self.inner_links)
        self._merge_schedule_sublinks(self.outer_links)


    def _fetch_all_links_history(self):
        """
        :return:
        从库中获取所有链接的历史
        """
        url = self.url_info.get('url')
        domain = self.url_info.get('domain')
        try:
            history_links_cache = {}
            domain_sublinks = dict()
            domain_sublinks[domain]  = []
            domain_sublinks[domain].append(url)
            for link in self.inner_links:
                domain_sublinks[link.domain].append(link.url)

            for link in self.outer_links:
                if not domain_sublinks.has_key(link.domain):
                    domain_sublinks[link.domain] = []
                domain_sublinks[link.domain].append(link.url)

            for domain in domain_sublinks.keys():
                if not domain_sublinks[domain]:continue
                for item in self.link_connection.check_history_links(domain, domain_sublinks[domain]):
                    if item.get('link_attr'):
                        item['link_attr']  = pickle.loads(item['link_attr'])
                    else:
                        item['link_attr']  = None
                    history_links_cache[item.get('url')] = item
        except Exception as e:
            raise Exception("{} fetch history error: {}".format(url, traceback.format_exc()))
        return history_links_cache

    def _check_history_links(self,domain, urls):
        result = []
        try:
            for url in urls:
                result.append(self.history_links_cache.get(url))
        except Exception as e:
            return []
        return result

    def _get_merge_crawl_info(self, base_crawl_info, schedule_obj):
        merge_crawl_info = MergeCrawlInfo()
        for attr in dir(merge_crawl_info):
            if base_crawl_info.get(attr) is not None:
                setattr(merge_crawl_info, attr, base_crawl_info.get(attr))
        return merge_crawl_info

    def _get_crawl_history(self, extract_info, crawl_info):
        crawl_historys = []
        crawl_history = CrawlHistory()
        try:
            crawl_history.download_time =  crawl_info.download_time
            crawl_history.content_finger = extract_info.content_finger
            crawl_history.link_finger = extract_info.link_finger
            crawl_history.real_title = extract_info.html_tag_title
            crawl_history.analyse_title = extract_info.analyse_title
            crawl_history.content_time = extract_info.content_time
            crawl_history.page_type = extract_info.content_type

            crawl_history.status_code = crawl_info.status_code
            crawl_history.http_code = crawl_info.http_code
            crawl_history.page_size = crawl_info.page_size

            self.inner_links.sort(key=lambda inner_link: get_md5_i64(inner_link.url), reverse=True)
            inner_links_str = ','.join(map(lambda link:link.url, self.inner_links))
            crawl_history.inner_link_finger = get_md5_i64(inner_links_str)
            crawl_history.inner_links_num = len(self.inner_links)
            crawl_history.outer_links_num = len(self.outer_links)

            # todo 以下待补充
            crawl_history.new_links_num_for_self = 0
            crawl_history.good_links_num_for_for_self = 0
            crawl_history.new_links_num_for_all = 0
            crawl_history.good_links_num_for_all = 0
            crawl_history.dead_page_type = 0
            crawl_history.dead_page_time = 0
        except Exception as e:
            self.logger.warning(e.message)
            pass
        crawl_historys.append(crawl_history)
        try:
            old_history_link = self.history_links_cache.get(self.url_info.get('url'))
            if old_history_link and old_history_link.get('link_attr'):
                crawl_historys.extend(old_history_link['link_attr'].normal_crawl_his)
                crawl_historys = crawl_historys[:7]
        except Exception as e:
            #发生异常应该是没有历史了吧
            pass
        return crawl_historys

    def _get_page_info(self):

        pass
    def merge_link_attr(self,
                        base_info,
                        extract_info,
                        scheduler_obj,
                        base_crawl_info,
                        data_extends
                        ):
        self.link_attr = LinkAttr()
        self.link_attr.url = self.url_info.get('url')
        self.link_attr.url_id = self.url_info.get('url_id')
        self.link_attr.crawl_info = self._get_merge_crawl_info(base_crawl_info, scheduler_obj)
        self.link_attr.src_type = base_info.src_type
        self.link_attr.depth = scheduler_obj.get('depth', (0 if is_index_url(self.url_info.get('url')) else None))
        self.link_attr.del_reason = None
        self.link_attr.seed_id = scheduler_obj.get('seed_id')
        #当前抓取链接不设置parent_info
        self.link_attr.normal_crawl_his = []
        self.link_attr.extract_message = ExtractMessage(ex_status=extract_info.ex_status,
                                         topic_id=extract_info.topic_id,
                                         extract_data=None)
        self.link_attr.normal_crawl_his = self._get_crawl_history(extract_info,self.link_attr.crawl_info)
        self.link_attr.found_time = self.link_attr.crawl_info.download_time
        #self.link_attr.page_info = self._get_page_info(extract_info, self.link_attr.crawl_info)
        self.link_attr.data_extends = data_extends
        self.link_attr.sub_links = self.schedule_sublinks
        return self.link_attr


    def _build_sublink_obj(self, link):
        history_link = self.history_links_cache.get(link.url)
        obj = {}
        #没有保存到历史链接信息
        if history_link is None or not history_link.get('link_attr'):
            link_attr = LinkAttr()
            link_attr.url = link.url
            if self.link_attr.depth is not None:
                link_attr.depth = self.link_attr.depth + 1
            else:
                link_attr.depth = 1
            link_attr.found_time = int(time.time())
            link_attr.normal_crawl_his = []
            link_attr.seed_id = None
            link_attr.src_type = self.link_attr.src_type
            obj = {
                "url":link.url,
                "url_id":link.url_id,
                "site":link.site,
                "site_id":link.site_id,
                "link_attr":pickle.dumps(link_attr),
                "status": FRESH_STATUS.IS_NEW
            }
        #有历史信息,修改发现时间和深度
        else:
            obj = deepcopy(history_link)
            link_attr = obj['link_attr']
            if link_attr.found_time is not None and obj.get('status') != FRESH_STATUS.IS_NEW:
                link_attr.found_time = min(link_attr.found_time, int(time.time()))
            else:
                link_attr.found_time = int(time.time())
            if self.link_attr.depth is not None:
                link_attr.depth = min(link_attr.depth, self.link_attr.depth + 1)
            else:
                link_attr.depth = 1
            obj['link_attr'] = pickle.dumps(link_attr)
        return obj
    def save_link_attrs(self):
        link_attr = deepcopy(self.link_attr)
        # 去除当前链接的子链信息
        del link_attr.sub_links
        save_objs = {}
        obj = {
            "url": self.url_info.get("url"),
            "url_id": self.url_info.get("url_id"),
            "site": self.url_info.get("site"),
            "site_id": self.url_info.get("site_id"),
            "link_attr": pickle.dumps(link_attr),
            "status": FRESH_STATUS.IS_NOT_NEW,
        }
        if link_attr.crawl_info.download_time:
            obj['download_time'] = timestampToTimestr(float(link_attr.crawl_info.download_time))
        else:
            obj['download_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

        domain = self.url_info.get('domain')
        save_objs[domain] = []
        save_objs[domain].append(obj)
        #添加站内子链
        for link in self.inner_links:
            save_objs[domain].append(self._build_sublink_obj(link))
        #添加站外了链
        for link in self.outer_links:
            if not save_objs.has_key(link.domain):
                save_objs[link.domain] = []
            save_objs[link.domain].append(self._build_sublink_obj(link))
        for domain in save_objs.keys():
            self.link_connection.bulk_save_links(domain, save_objs[domain])

    def _merge_schedule_sublinks(self, links):
        try:
            url = self.url_info.get('url')
            for link in links:
                if link.type > 0 and link.url != url:
                    history_link = self.history_links_cache.get(link.url)
                    if (not history_link) or history_link.get("status") == FRESH_STATUS.IS_NEW:
                        link.is_new = True
                    else:
                        link.is_new = False
                    self.schedule_sublinks.append(link)
        except Exception as e:
            self.logger.warning(e.message)
