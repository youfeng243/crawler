# -*- coding: utf-8 -*-
import urllib

from i_util.tools import build_hzpost_url,extract_hzpost_url
from i_util.tools import get_url_info,url_query_decode, base64_encode_json,base64_decode_json


class HzurlMiddleware(object):
    def __init__(self, conf):
        self.conf = conf
        self.logger = conf.get('log')

    def process_request(self, request):
        try:
            if request.url.find('HZPOST')>=0:
                dicta=extract_hzpost_url(request.url)
                request.url=dicta.get('url')
                request.post_data=self.unicode2str(dicta.get('postdata'))
                request.method='post'
            else:
                return
        except Exception as e:
            self.logger.error('hz_url:%s convert to normal_url fail reason:%s'%(request.url,e.message))
            return
        return

    def process_response(self, request, response):
        try:
            if request.post_data and request.method == "post":
                hzurl=build_hzpost_url(request.url,request.post_data)
                request.url=hzurl
            else:
                return response
        except Exception as e:
            self.logger.error("normal_url:%s convert to hz_url fail reason:%s"%(request.url,e.message))
            return response
        return response

    def unicode2str(self,post_data):
        dict={}
        for k, v in post_data.items():
            if isinstance(v,unicode):
                key=k.encode('utf8')
                value=v.encode('utf8')
                dict[key]=value
            else:
                dict[k]=v
        return dict

    def hz_to_nor(self,request):
        url_info = get_url_info(request.url)
        query_info = url_query_decode(url_info.get('query'))
        request.post_data = base64_decode_json(query_info.get('HZPOST'))
        query_info.pop('HZPOST')
        nor_url = request.url.split("?")[0] + "?" + urllib.urlencode(query_info)
        request.url=nor_url
        request.method='post'

    def nor_to_hz(self,request):
        if request.post_data and request.method == "post":
            url_info = get_url_info(request.url)
            query_info = url_query_decode(url_info.get('query'))
            query_info['HZPOST'] = base64_encode_json(request.post_data)
            hz_url = request.url.split("?")[0] + "?" + urllib.urlencode(query_info)
            request.url=hz_url

