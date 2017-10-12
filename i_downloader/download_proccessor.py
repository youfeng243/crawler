#!/usr/bin/Python
# coding=utf-8
import sys

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer

sys.path.append('..')
from bdp.i_crawler.i_downloader.ttypes import DownLoadReq
from i_util.normal_proccessor import NormalProccessor
from downloader_handle import DownloadHandler
class DownloaderProccessor(NormalProccessor):
    def __init__(self, conf):
        self.log = conf['log']
        self.downloader = DownloadHandler(conf=conf)

    def to_string(self, download_rsp):
        str_rsq = None
        try:
            tMemory_b = TMemoryBuffer()
            tBinaryProtocol_b = TBinaryProtocol(tMemory_b)
            download_rsp.write(tBinaryProtocol_b)
            str_rsq = tMemory_b.getvalue()
            self.log.info('data-length is {}'.format(str(len(str_rsq))))
        except EOFError, e:
            self.log.warning("cann't write PageParseInfo to string")
        return str_rsq

    def do_task(self, body):
        download_req = DownLoadReq()
        try:
            tMemory_o = TMemoryBuffer(body)
            tBinaryProtocol_o = TBinaryProtocol(tMemory_o)
            download_req.read(tBinaryProtocol_o)
            self.log.info("request_msg\t%s" % (download_req))
            download_rsp = self.downloader.download(download_req)
            self.log.debug('haizhi- do_task %s' % str(download_rsp))
            return self.to_string(download_rsp)
        except EOFError, e:
            self.log.warning("cann't read DownLoadRsp from string")
            return None
        #return download_rsp
    def do_output(self,body):
        return True

    def stop(self):
        self.downloader.stop()