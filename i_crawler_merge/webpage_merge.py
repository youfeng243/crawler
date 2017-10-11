#!/usr/bin/env python
# -*- coding:utf-8 -*-
import time

from i_util.tools import timestampToTimestr,decode_content

class WebpageMerge():
    def __init__(self, webpage_connection):
        self.webpage_connection = webpage_connection
        self.webpage = None
    def merge_webpage(self, base_info, extract_crawl_info,data_extens, parse_extends):
        webpage_obj = {}
        webpage_obj['url'] = base_info.url
        webpage_obj['url_id'] = base_info.url_id
        webpage_obj['site'] = base_info.site
        webpage_obj['site_id'] = base_info.site_id
        string, charset = decode_content(extract_crawl_info.content)
        if charset != None:
            webpage_obj['content'] = string.encode('utf-8')
        else:
            webpage_obj['content'] = ""

        if extract_crawl_info.download_time:
            webpage_obj['download_time'] = timestampToTimestr(float(extract_crawl_info.download_time))
        else:
            webpage_obj['download_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        webpage_obj['data_extends'] = data_extens
        webpage_obj['parse_extends'] = parse_extends
        self.webpage  = webpage_obj
        return webpage_obj


    def save_webepage(self,domain, webpage):
        if not domain or not webpage:return None
        self.webpage_connection.save_webpage(domain, webpage)
