#!/usr/bin/env python
# -*- coding:utf-8 -*-

class LinkConnection():
    def __init__(self):
        raise NotImplementedError
    def check_history_links(self, domain, url_list):
        raise NotImplementedError
    def bulk_save_links(self, domain, links):
        raise NotImplementedError

class WebpageConnection():
    def __init__(self):
        raise NotImplementedError

    def merge_webpage(self, base_info, extract_crawl_info,data_extens, parse_extends):
        raise NotImplementedError

    def get_webpage(self, domain, url):
        raise NotImplementedError


