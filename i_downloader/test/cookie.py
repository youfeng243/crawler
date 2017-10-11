# -*- coding: utf-8 -*-
import sys
import random
from i_downloader.util.get_cookies import get_cookie
from i_util.tools import get_md5_i64

class CookieMiddleware(object):
    def __init__(self, conf):
        self.conf = conf
        self.logger = conf.get('log')

    def process_request(self, request):
        try:
            site_id = request.url.split('/')[2]
        except :
            site_id=request.get('url')
        site_id=get_md5_i64(site_id)
        try:
            site_id = int(site_id)
        except:
            site_id = None
        cookies = get_cookie(site_id)
        if not cookies:
            return
        cookie = cookies[random.randint(0, sys.maxint) % len(cookies)]
        self.logger.info(" using cookie site_id: %s user_id: %s" % ( site_id, cookie['user_id']))
        request.info.setdefault('headers', {})['Cookie'] = cookie['cookie']
