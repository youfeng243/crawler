#!/usr/bin/env python
# -*- coding:utf-8 -*-

from pymongo import MongoClient, ASCENDING,DESCENDING
from pymongo import UpdateOne

from i_crawler_merge.libs.tools import short_url
from merge_database import LinkConnection


class MongoLinkConnection(LinkConnection):
    def __init__(self, host = "127.0.0.1", port = 27017, db = "crawl_merge_linkattr", username = None, password = None):
        self.client = MongoClient(host, port)
        self.database = self.client[db]
        if username and password:
            self.database.authenticate(username, password)

    def check_history_links(self, domain, urls):
        result = []
        urls = map(short_url, urls)
        for item in self.database[domain].find({"url":{"$in":urls}}):
            if item.has_key("long_url"):
                item['url'] = item['long_url']
            result.append(item)
        return result

    def bulk_save_links(self, domain, links):
        self.database[domain].ensure_index([("url", ASCENDING)], unique=True, background=True)
        self.database[domain].ensure_index([("download_time", DESCENDING)], background=True)
        requests = []
        for link_obj in links:
            main_url = link_obj.get("url")
            if "HZPOST" in main_url or len(main_url) > 512:
                link_obj['long_url'] = main_url
                link_obj["url"] = short_url(main_url)
            requests.append(UpdateOne({'url': link_obj['url']}, {'$set':link_obj}, upsert = True))
        save_result = self.database[domain].bulk_write(requests, ordered=False)
        return save_result.bulk_api_result

class MongoWebpageConnection():
    def __init__(self, host = "127.0.0.1", port = 27017, db = "crawl_merge_webpage", username = None, password = None):
        self.client = MongoClient(host, port)
        self.database = self.client[db]
        if username and password:
            self.database.authenticate(username, password)

    def save_webpage(self, domain, webpage):
        self.database[domain].ensure_index([("url", ASCENDING)], unique=True, background=True)
        self.database[domain].ensure_index([("download_time", DESCENDING)], background=True)
        main_url = webpage["url"]
        if "HZPOST" in main_url or len(main_url) > 512:
            webpage['long_url'] = main_url
            webpage["url"] = short_url(main_url)
        result = self.database[domain].update_one({"url":webpage["url"]}, {"$set":webpage}, upsert=True)
        return result.raw_result

    def get_webpage(self, domain, url):
        url = short_url(url)
        return self.database[domain].find_one({"url":url})

