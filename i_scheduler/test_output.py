#!/usr/bin/Python
# coding=utf-8
import sys

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer

sys.path.append('..')
from bdp.i_crawler.i_crawler_merge.ttypes import LinkAttr
from bdp.i_crawler.i_downloader.ttypes import DownLoadReq
from bdp.i_crawler.i_extractor.ttypes import PageParseInfo 
from i_util.pybeanstalk import PyBeanstalk

if __name__ == '__main__':
    pybeanstalk = PyBeanstalk('10.25.114.50');
    try:
        #link_info = DownLoadReq();
        info = PageParseInfo()
        while True:
            #print pybeanstalk.stats_tube('download_req')
            job = pybeanstalk.reserve('online_extract_info');
            """
            tMemory_o = TMemoryBuffer(job.body)
            tBinaryProtocol_o = TBinaryProtocol(tMemory_o)
            info.read(tBinaryProtocol_o)
            d = vars(info)
            for k,v in d.items():
                print k,v
            """
            job.delete();
            #break;
    except EOFError, e:
        print e
