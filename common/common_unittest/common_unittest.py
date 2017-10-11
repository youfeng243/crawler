import unittest
from ..conf import conf
from ..log import log
import logging
import os
import pytoml
from ..topic_manager import TopicManager
from ..validate_manager import ValidateManager

def get_config(dict):
    config = conf()
    config.host=dict.get('host')
    config.port=dict.get('port')
    config.server_thread_num = dict.get('server_thread_num')
    config.process_thread_num = dict.get('process_thread_num')
    config.beanstalk_conf=dict.get('beanstalk_conf')
    config.MYSQL=dict.get('MYSQL')
    config.STATISTICS_COLLECTION_NAME = dict.get('STATISTICS_COLLECTION_NAME')
    # config.MONGODB = dict.get('MONGODB')
    config.logname=dict.get('logname')
    config.server=dict.get('server')
    config.backend=dict.get('backend')
    config.kafka=dict.get('kafka_server')
    config.hbase = dict.get('HBASE')
    return config

class CommonUnitTest(unittest.TestCase):
    def setUp(self):
        conf_path = os.path.join(os.path.dirname(__file__), 'common_unittest.toml')
        with open(conf_path, 'rb') as config:
            config = pytoml.load(config)
        conf = get_config(config)
        log.init_log(conf, logging.DEBUG)
        # self.topic_manager = TopicManager(conf)
        # self.validate_manager = ValidateManager(self.topic_manager, conf)

    def tearDown(self):
        pass

    def testSimple(self):
        pass

def casesuite():
    suite = unittest.TestSuite()
    suite.addTest(CommonUnitTest("testSimple"))
    unittest.TextTestRunner().run(suite)