# -*- coding: utf-8 -*-

from bdp.i_crawler.i_downloader.ttypes import CrawlStatus

class CheckMiddleware(object):
    def __init__(self, conf):
        self.conf = conf
        self.logger = conf.get('log')

    def process_response(self, request, response):
        if response==None:
            return response
        if response.content==None:
            return response
        if len(response.content) < request.check_size:
            request.identify_status = CrawlStatus.CRAWL_SizetooSmall
        return response
