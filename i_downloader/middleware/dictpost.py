# -*- coding: utf-8 -*-
class DictpostMiddleware(object):
    def __init__(self, conf):
        self.conf = conf
        self.logger = conf.get('log')
    def process_request(self, request):
        try:
            dict={}
            post_data=request.post_data
            flag=False
            for i in post_data.keys():
                if i.find('/') >= 0:
                    flag=True
                    dict[i.split('/')[0]] = {}
            if flag:
                data=self.map2dict(post_data,dict)
                request.post_data=data
        except Exception as e:
            self.logger.error('post_data:%s convert fail'%(post_data))
            return

    def process_response(self, request, response):
        return response
        pass
    def map2dict(self,map,dict):
        for i in map.keys():
            if i.find('/') >= 0:
                dict[i.split('/')[0]][i.split('/')[1]] = map[i]
            else:
                dict[i]=map[i]
        return dict

    def dict2map(self,dict):
        pass
