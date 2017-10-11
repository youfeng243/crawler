#!/usr/bin/Python
# coding=utf-8
import sys
import getopt
import pytoml
from thrift.protocol.TBinaryProtocol import TBinaryProtocol  
from thrift.transport.TTransport import TMemoryBuffer
sys.path.append('..')
from bdp.i_crawler.i_crawler_merge.ttypes import LinkAttr
from bdp.i_crawler.i_downloader.ttypes import DownLoadReq
from i_util.normal_proccessor import NormalProccessor
from i_util.logs import LogHandler


class SchedulerProccessor(NormalProccessor):
    def __init__(self, log, scheduler):
        self.log = log
        self.scheduler = scheduler

    def to_string(self, download_req_info):
        str_req = None
        try:
            tMemory_b = TMemoryBuffer()
            tBinaryProtocol_b = TBinaryProtocol(tMemory_b)  
            download_req_info.write(tBinaryProtocol_b)  
            str_req = tMemory_b.getvalue()
        except EOFError, e:
            self.log.warning("cann't write DownLoadReq to string")
        return str_req

    def do_task(self, body):
        link_info = LinkAttr()
        try:
            tMemory_o = TMemoryBuffer(body)  
            tBinaryProtocol_o = TBinaryProtocol(tMemory_o)  
            link_info.read(tBinaryProtocol_o)
            self.scheduler.schedule_task(link_info)
        except EOFError, e:
            self.log.warning("can't read LinkAttr from string")
            return None
        return link_info

    def do_output(self,body):
        return True



def usage():
    pass


def test(conf):
    processor = SchedulerProccessor(conf['log'], 1)
    print processor.do_task("test")

if __name__ == '__main__':
    try:
        file_path = './scheduler.toml'
        opt, args = getopt.getopt(sys.argv[1:], 'f:', ['help'])
        for name, value in opt:
            if name == "-f":
                file_path = value
            elif name in ("-h", "--help"):
                usage()
                sys.exit()
            else:
                assert False, "unhandled option"

        with open(file_path, 'rb') as config:
            conf = pytoml.load(config)
            conf['log']=LogHandler(conf['server']['name']+str(conf['server']['port']))
        test(conf)

    except getopt.GetoptError:
        sys.exit()
