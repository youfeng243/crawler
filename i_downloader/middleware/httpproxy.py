# -*- coding: utf-8 -*-
import traceback

from bdp.i_crawler.i_downloader.ttypes import Proxy
from i_downloader.util.proxy_new import Proxies


class HttpProxyMiddleware(object):
    def __init__(self, conf):
        self.conf = conf
        self.logger = conf.get('log')
        self.proxy = Proxies(log=self.logger, config=conf.get('redis_proxies'))

        self.proxy.init_proxy()

    def process_request(self, request):
        if request.use_proxy:
            try:
                site = request.url.split('/')[2]
                proxy = self.proxy.get_proxy(site)
                proxy = self.format_proxy(proxy)
                proxy_req = request.proxy
                if proxy_req is not None:
                    return
                else:
                    request.proxy = proxy
            except:
                self.logger.error('process_request\turl:%s\terror:%s' % (request.url, traceback.format_exc()))
                return

    def process_response(self, request, response):
        aim_proxy = 'none'
        try:
            site = request.url.split('/')[2]

            # aim_proxy=self.proxy_to_str(request.proxy)
            if hasattr(request, 'identify_status') and request.use_proxy == True:
                aim_proxy = self.proxy_to_str(request.proxy)
                # if request.identify_status == 8:
                #     self.proxy.proxy_cannot_ping(aim_proxy)
                # if request.identify_status == 7:
                #     self.proxy.site_proxy_mark(site, aim_proxy)
        except Exception as e:
            self.logger.error('url:%s\tproxy:%s format error reason is %s' % (request.url, aim_proxy, e.message))

        return response

    def format_proxy(self, proxys):
        proxy = Proxy()
        type = proxys.split('://')[0]
        user = proxys.split('://')[1].split('@')[0]
        ip = proxys.split('://')[1].split('@')[1]
        proxy.user = user.split(':')[0]
        proxy.password = user.split(':')[1]
        proxy.host = ip.split(':')[0]
        proxy.port = int(ip.split(':')[1])
        proxy.type = type
        return proxy

    def proxy_to_str(self, proxy):
        proxy = 'http://%s:%s@%s:%s' % (proxy.user, proxy.password, proxy.host, proxy.port)
        return proxy
