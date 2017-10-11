# -*- coding: utf-8 -*-
from i_downloader.util.proxy_new import Proxies
import traceback
import threading
from bdp.i_crawler.i_downloader.ttypes import Proxy
class HttpProxyMiddleware(object):
    def __init__(self, conf):
        self.conf = conf
        self.logger = conf.get('log')
        self.proxy=Proxies(log=self.logger,config=conf.get('redis_proxies'))

        self.proxy.init_proxy()
        self.proxy.update_proxy_available = threading.Thread(target=self.proxy.update_proxy_time,
                                                   args=(self.proxy.redis_conf['proxy_test_available'],))
        self.proxy.update_proxy_available.start()

    def process_request(self, request):
        if request.use_proxy==True:
            try:
                site=request.url.split('/')[2]
                proxy=self.proxy.get_proxy(site)
                proxy=self.format_proxy(proxy)
                proxy_req=request.proxy
                if proxy_req!=None:
                    return
                else:
                    request.proxy=proxy
            except:
                self.logger.error('process_request\turl:%s\terror:%s' % (request.url, traceback.format_exc()))
                return


    def process_response(self, request, response):
        try:
            site=request.url.split('/')[2]
            aim_proxy='none'
            #aim_proxy=self.proxy_to_str(request.proxy)
            if hasattr(request, 'identify_status') and request.use_proxy==True:
                aim_proxy=self.proxy_to_str(request.proxy)
                if request.identify_status==8:
                    self.proxy.proxy_cannot_ping(aim_proxy)
                if request.identify_status==7:
                    self.proxy.site_proxy_mark(site,aim_proxy)
            if response==None:
                return response
        except Exception as e:
            self.logger.error('url:%s\tproxy:%s format error reason is %s'%(request.url,aim_proxy,e.message))

        return response


    def format_proxy(self,proxys):
        proxy = Proxy()
        type=proxys.split('://')[0]
        user = proxys.split('://')[1].split('@')[0]
        ip = proxys.split('://')[1].split('@')[1]
        proxy.user = user.split(':')[0]
        proxy.password = user.split(':')[1]
        proxy.host = ip.split(':')[0]
        proxy.port = int(ip.split(':')[1])
        proxy.type = type
        return  proxy
    def proxy_to_str(self,proxy):
        proxy='http://%s:%s@%s:%s'%(proxy.user,proxy.password,proxy.host,proxy.port)
        return proxy
