#!/usr/bin/Python
# coding=utf-8
import sys

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer

sys.path.append('..')
from bdp.i_crawler.i_downloader.ttypes import DownLoadRsp
from bdp.i_crawler.i_extractor.ttypes import PageParseInfo 
from i_util.pybeanstalk import PyBeanstalk

if __name__ == '__main__':
    pybeanstalk = PyBeanstalk('101.201.102.37');
    try:
        extractor_info = PageParseInfo();
        body = pybeanstalk.reserve('extract_info').body;
        tMemory_o = TMemoryBuffer(body)
        tBinaryProtocol_o = TBinaryProtocol(tMemory_o)
        extractor_info.read(tBinaryProtocol_o)
        print extractor_info
    except EOFError, e:
        print e
