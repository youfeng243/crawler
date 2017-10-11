# -*- coding: utf-8 -*-
from phantom import PhantomDownloader
from simple import SimpleDownloader


class Downloader(object):
    def __init__(self,conf):
        self.conf=conf
        self.downloaders = {
            'simple':  SimpleDownloader(conf),
            'phantom': PhantomDownloader(conf),
        }

    def download(self, req):
        downloader = self.downloaders.get(req.download_type)
        if downloader:
            res= downloader.download(req)
        else:
            res= self.downloaders.get('simple').download(req)
        return res
    #回收downloader一的些资源比如phantomjs进程
    def stop(self):
        if not self.downloaders:
            return
        for name, obj in self.downloaders.items():
            obj.stop()