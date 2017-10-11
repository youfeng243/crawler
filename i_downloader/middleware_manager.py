# -*- coding: utf-8 -*-

from i_util.load_objects import load_object

class MiddlewareManager(object):
    """
    中间件管理模块
    单例模式
    """
    def __init__(self, conf):
        self.conf = conf
        self.logger = conf.get('log')
        mw_cls = load_object(conf.get('middleware_module'), suffix='middleware')
        self.middlewares = {
            'request':  [],
            'response': [],
        }
        for key in self.conf.get('middlewares'):
            mw = mw_cls[key](conf)
            if hasattr(mw_cls[key], 'process_request'):
                self.middlewares['request'].append(mw)
            if hasattr(mw_cls[key], 'process_response'):
                self.middlewares['response'] = [mw] + self.middlewares['response']

    def process_request(self, request):
        for mw in self.middlewares['request']:
            mw.process_request(request)

    def process_response(self, request, response):
        for mw in self.middlewares['response']:
            #response.content = "";
            response = mw.process_response(request, response)
        return response
