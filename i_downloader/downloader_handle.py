#!/usr/bin/Python
import sys
import traceback
sys.path.append('..')
from bdp.i_crawler.i_downloader.ttypes import DownLoadRsp
from bdp.i_crawler.i_downloader.ttypes import CrawlStatus
from middleware_manager import MiddlewareManager
from downloader.downloader import Downloader
import copy
import time

class DownloadHandler(object):
    def __init__(self, conf):
        self.conf=conf
        self.log=conf.get('log')
        self.log.info('DownloadHandler load start')
        self.downloader=Downloader(conf=conf)
        self.middleware_manager = MiddlewareManager(conf)
    def write_log(self, request, response):
        info = "status:%s\turl:%s\tdownload_type:%s\telapsed:%s\tproxy_time:%s\thttpcode:%d" \
              % (response.status, request.url, request.download_type, response.elapsed, request.proxytime,
                 response.http_code)
        if request.proxy != None:
            info += "\tproxy:%s:%s" % (request.proxy.host, request.proxy.port)  # str(request['proxy']['port']
        if response.status == CrawlStatus.CRAWL_SUCCESS:
            self.log.info(info)
        else:
            self.log.error(info)

    def download(self, request):
        self.log.info(
            "start_crawl\turl::%s\tmethod:%s\tdownload_type:%s" % (request.url, request.method, request.download_type))
        start = time.time()
        response = DownLoadRsp(status=CrawlStatus.CRAWL_FAILT, )
        try:
            if request.retry_times is None:
                retry_times = self.conf.get('default_request_kwargs')['retry_times']
            else:
                retry_times = request.retry_times
            for t in xrange(retry_times):
                request = copy.deepcopy(request)
                self.middleware_manager.process_request(request)
                start_time = time.time()
                res = self.downloader.download(request)
                response = self.middleware_manager.process_response(request, res)
                request.proxytime = (time.time() - start_time) * 1000.0 - response.elapsed
                self.write_log(request, response)
                if response.status == CrawlStatus.CRAWL_SUCCESS:
                    break
                time.sleep(3)
        except Exception as e:
            self.log.error('url:' + request.url + '\terror_msg:' + str(traceback.format_exc()))
        finally:
            content_len = -1
            if response.content:
                content_len = len(response.content)
            self.log.info(
                'finish_crawl\tuse_time:' + str(time.time() - start) + '\tlens:' + str(content_len) + '\tstatus:' + str(
                    response.status) + '\turl:' + str(request.url))
        return response

    def stop(self):
        self.downloader.stop()
