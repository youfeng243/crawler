# -*- coding: utf-8 -*-
import json

import requests
import requests.packages.urllib3
from requests.exceptions import ConnectTimeout
from requests.exceptions import HTTPError
from requests.exceptions import ProxyError

requests.packages.urllib3.disable_warnings()
class SimpleDownloader(object):
    def __init__(self,conf):
        self.conf=conf
        self.log=conf.get('log')
    """
    普通下载，调用requests模块
    """
    download_kwargs = {
        'allow_redirects': True,
        'params': None,
        'data': None,
        'headers': None,
        'cookies': None,
        'files': None,
        'auth': None,
        'timeout': None,
        'proxies': None,
        'hooks': None,
        'stream': None,
        'verify': None,
        'cert': None,
        'json': None,
    }

    def _transfer_proxy(self,url, proxy, kwargs):
        if proxy is None:
            return
        scheme_type = url.split(':')[0]
        if proxy.type==None:
            proxy.type = scheme_type
        if proxy.password:
            kwargs["proxies"] = {scheme_type: "%s://%s:%s@%s:%s" % (proxy.type, proxy.user, proxy.password, proxy.host, str(proxy.port))}
        else:
            kwargs["proxies"] = {scheme_type: "%s://%s:%s" % (proxy.type, proxy.host, str(proxy.port))}

    def _req_to_kw(self,req,kw):
        for k, v in self.download_kwargs.items():
           if hasattr(req,k) and getattr(req,k)!=None:
               kw[k] = getattr(req,k)
           else:
               kw[k]=v
        kw['data'] = req.post_data
        kw['headers'] = req.http_header
        kw['timeout']=req.time_out
        self._transfer_proxy(req.url,req.proxy, kw)

    def download(self, req):
        url=req.url
        method=req.method
        kwargs_={}
        self._req_to_kw(req,kwargs_)
        res = None
        try:
            if hasattr(req,'session'):
                post_data={}
                post_data['url']=req.url
                post_data['post_data']=json.dumps(kwargs_['data'])
                proxy_url='http://gsxtonlinecrawl.sz.haizhi.com/'
                res=req.session.post(url=proxy_url,data=post_data)

                # for i in range(3):
                #     res=req.session.post(url,data=kwargs_['data'])
                #     if self.check_body(req,res.text):
                #         break
                #     if i==3:
                #         req.identify_status=6
            else:
                if method == 'get':
                    if kwargs_.has_key('data'):
                        kwargs_.pop('data')
                    res= requests.get(url, **kwargs_)
                elif method == 'post':
                    res= requests.post(url, **kwargs_)
                else:
                    self.log.error('method error : ' + kwargs_['method'])
                    return None
            return res
        except ProxyError as e :
            req.identify_status = 8
            self.log.error('simple_download\turl:%s\tproxy:%s\tping\tfail' % (url, kwargs_['proxies']))
        except HTTPError or ConnectTimeout or requests.ConnectionError as e :
            req.identify_status = 7
            self.log.error('simple_download\turl:%s\tmethod:%s\tproxy\tfail'%(url, method))
        except Exception as e:
            self.log.error("simple_download\turl:%s\tmethod:%s\texcept:%s" % (url, method, e))
        return res
    def check_body(self,req,content):
        check_body=req.session_commit.check_body
        check_body_not=req.session_commit.check_body_not
        if check_body or check_body_not:
            #content = str_obj(content)
            #str.find()
            if not check_body_not and not content.find(check_body) > 0:
                self.log.warning('url:%s response not contain %s'%(req.url,check_body))
                return False
            elif not check_body and content.find(check_body_not):
                self.log.warning('url:%s response contain %s' % (req.url, check_body_not))
                return False
            elif check_body_not and content.find(check_body_not) and \
                 check_body and not content.find(check_body):
                 self.log.warning('url:%s response not contain %s and contain %s' % (req.url, check_body,check_body_not))
                 return False
        return True
    def stop(self):
        pass