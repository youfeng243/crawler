# -*- coding: utf-8 -*-

import json
import time
from i_util.tools import str_obj, unicode_obj
from bdp.i_crawler.i_downloader.ttypes import DownLoadRsp
from bdp.i_crawler.i_downloader.ttypes import CrawlStatus

class DefaultMiddleware(object):
    def __init__(self, conf):
        self.conf = conf
        self.logger = conf.get('log')

    def process_request(self, request):
        for k, v in self.conf.get('default_request_kwargs').items():
            # 检查实例是否有这个属性   属性是否为空
            if not hasattr(request, k) or getattr(request, k)==None:
                setattr(request, k, v)

    def process_response(self, request, response):
        resp = {}
        resp['url'] = str_obj(request.url)
        if hasattr(request, 'src_type') and getattr(request, 'src_type') != None:
            resp['src_type'] = str_obj(request.src_type)
        resp['download_time'] = int(time.time())
        resp['pages'] = []
        resp['content'] = ''
        resp['info'] = request.info
        resp['scheduler'] = request.scheduler
        resp['parse_extends'] = request.parse_extends
        resp['data_extends'] = request.data_extends
        resp['http_code'] = 0
        resp['elapsed'] = -1
        if response is not None:
            resp['redirect_url'] = str_obj(response.url)
            resp['http_code']=response.status_code
            resp['elapsed'] = int(response.elapsed.microseconds/1000.0)
            resp['content_type']=response.headers.get('content-type')
            resp['content']=str_obj(response.content)
            resp['page_size']=len(resp['content'])
        if hasattr(request, 'contenta'):
            resp['content'] = str_obj(request.contenta)
        if hasattr(request, 'identify_status'):
            resp['status'] = request.identify_status
        else:
            if resp.get('http_code')==200:
                resp['status']=CrawlStatus.CRAWL_SUCCESS
            else:
                resp['status'] = CrawlStatus.CRAWL_FAILT
        return DownLoadRsp(**resp)
