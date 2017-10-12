# -*- coding: utf-8 -*-
import datetime
import json
import traceback
import urllib

import requests
from requests import Response
from requests.cookies import cookiejar_from_dict
from requests.exceptions import HTTPError
from i_util.tools import crawler_basic_path
from requests.exceptions import ProxyError
from requests.structures import CaseInsensitiveDict

from i_util.tools import get_tld
from .phantomjs_server import PhantomjsServer

DEADLINE = 3600
class PhantomResponse(Response):
    def __init__(self,attr_dict=None):
        super(PhantomResponse, self).__init__()
        self.js_script_result = None
        if attr_dict:
            self.attr_from_dict(attr_dict)

    def attr_from_dict(self, dict):
        self.status_code = dict['status_code']
        self.url = dict['url']
        self._content = dict['content']
        self.js_script_result = dict.get('js_script_result')
        self.elapsed = datetime.timedelta(seconds=dict['time'])
        self.cookies = cookiejar_from_dict(dict['cookies'])
        self.headers = CaseInsensitiveDict(dict['headers'])


class PhantomDownloader(object):
    default_body = {
        'url':'',
        'method': 'get',
        'headers': {
            'Connection': 'close'
        },
        'allow_redirects': True,
        'use_gzip': True,
        'proxy':{},
        'timeout':60,
        'priority': 0,
        'verify': False,
        'download_type': 'phantom',
        'check_size': 1000,
    }

    def __init__(self, conf):
        self.conf=conf
        self.log=conf.get('log')
        self.phantomjs_server = PhantomjsServer(conf)
        self.running = False

    def package_body(self, url, **kwargs):
        body = self.default_body.copy()
        body['headers']=kwargs['http_header']
        body.update(kwargs)
        body['url'] = url
        if body['method'] == 'post':
            try:
                body['data'] = urllib.urlencode(body['post_data'])
            except:
                traceback.format_exc()
        for k, v in body.items():
            if isinstance(v, dict):
                body[k] = json.dumps(v)
        return body

    def package_response(self, res):
        response = PhantomResponse(json.loads(res.content))
        return response

    def proxy_package(self,proxy,kw):
        if not proxy:
            return
        kw['host']=proxy.host
        kw['password']=proxy.password
        kw['user']=proxy.user
        kw['port']=str(proxy.port)
        kw['type']=proxy.type

    def req_to_kw(self,req,kw):
        for k, v in self.default_body.items():
            if hasattr(req,k) and getattr(req,k)!=None:
                kw[k] = getattr(req,k)
            else:
                kw[k]=v
        if req.method=='post':
            kw['data'] = urllib.urlencode(req.post_data)

        if kw.get('data','')=='':
            kw['data']='{}'
        kw['headers'] = req.http_header
        kw['timeout'] = req.time_out
        proxy={}
        try:
            kw['domain']=get_tld(req.url)
        except:
            kw['domain']=req.url.split('/')[2]
            self.log.error('phantom\tdownload\turl:%s\tget doamin fail'%(req.url))
        self.proxy_package(req.proxy,proxy)
        kw['proxy']=proxy
        for k, v in kw.items():
            if isinstance(v, dict):
                kw[k] = json.dumps(v)

    def download(self, req):
        kwargs_ = {}
        server = self.phantomjs_server.get_server_info()
        if not server:
            self.log.error('Server not exist! url: %s' % req.url)
            return None
        server_addr = server['server_addr']
        #body = self.package_body(kw['url'],kw)
        self.req_to_kw(req, kwargs_)
        try:
            response = requests.post(server_addr, data=kwargs_, timeout=kwargs_['timeout'])
            response = self.package_response(response)
            if response.status_code in [403, 599, 500, 502, 503, 504, 400, 408]:
                req.identify_status = 7
            # if response.status_code in [403, 599, 500, 502, 503, 504, 400, 408]:
            #     raise Exception(response.reason)
            return response
        except ProxyError as e:
            req.identify_status = 8
            self.log.error('simple_download\turl:%s\tproxy:%s\tping\tfail' % (req.url, kwargs_['proxies']))
        except HTTPError or requests.ConnectTimeout or requests.ConnectionError as e:
            req.identify_status = 7
            self.log.error('simple_download\turl:%s\tmethod:%s\tproxy\tfail' % (req.url, kwargs_['proxies']))
        except Exception as e:
            self.log.error('phantom_downlaod\tfail\treason%s'%e)
            return None

    def stop(self):
        self.phantomjs_server.stop()
