# -*- coding: utf-8 -*-

import os
import sys
SCRIPT_PATH = os.path.dirname(__file__)
sys.path.append(os.path.join(SCRIPT_PATH, '..'))
sys.path.append(os.path.join(SCRIPT_PATH, '../..'))
import json
import beanstalkc
from i_data_saver.conf import config as conf
from thrift.protocol.TBinaryProtocol import TBinaryProtocol  
from thrift.transport.TTransport import TMemoryBuffer 
from bdp.i_crawler.i_entity_extractor.ttypes import EntityExtractorInfo, EntitySource
import pickle

import logging
logging.basicConfig(level = logging.DEBUG)

import test_entity_data

if __name__ == '__main__':
    # 测试数据
    #test_entity_extractor_info = test_entity_data.data_foo
    ifile = open("entity_extractor.pkb")
    test_entity_extractor_info = pickle.load(ifile)
    aa = json.loads(test_entity_extractor_info.entity_data)
    aa["registered_capital"] = "1234545"
    test_entity_extractor_info.entity_data = json.dumps(aa)
    print test_entity_extractor_info
    # test_entity_extractor_info = test_entity_data.data_ssgg
    #test_entity_extractor_info = test_entity_data.data_ktgg


    # 将EntityExtractorInfo序列化
    tMemory_b = TMemoryBuffer()
    tBinaryProtocol_b = TBinaryProtocol(tMemory_b)
    test_entity_extractor_info.write(tBinaryProtocol_b)
    serialized_data = tMemory_b.getvalue()

    #logging.debug('serialized_data: %s' % repr(serialized_data))

    # 使用beanstalk发出数据
    # conn = beanstalkc.Connection('127.0.0.1', 11300)
    # conn.use('entity_info')
    # conn.put(serialized_data)
    #
    # logging.debug('data sent!')
    #
    # import time
    # time.sleep(0.1)
