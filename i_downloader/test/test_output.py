#!/usr/bin/Python
# coding=utf-8
import sys

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer

sys.path.append('..')
from bdp.i_crawler.i_extractor.ttypes import PageParseInfo
from i_util.pybeanstalk import PyBeanstalk

if __name__ == '__main__':
    pybeanstalk = PyBeanstalk('101.201.100.58')
    try:
        rsp_info = PageParseInfo()
        job = pybeanstalk.reserve('extract_info_ws')
        # while True:
        if job:
            tMemory_o = TMemoryBuffer(job.body)
            tBinaryProtocol_o = TBinaryProtocol(tMemory_o)
            rsp_info.read(tBinaryProtocol_o)
            d = vars(rsp_info)
            print d
            for k,v in d.items():
                print k,v
            job.delete()
    except EOFError, e:
        print e
