#!/usr/bin/Python
# coding=utf-8
import os
import signal
import sys
import getopt
import pytoml

from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport
sys.path.append('..')
from i_util.heart_beat import HeartbeatThread
from bdp.i_crawler.i_entity_extractor import EntityExtractorService
from bdp.i_crawler.i_entity_extractor.ttypes import ResultResp, EntityResp
import traceback
import json
from i_util.tools import crawler_basic_path, remove_pid_file, make_pid_file

from common.log import log
from entity_extractor_worker import EEWorkerPool
import time
from i_entity_extractor.entity_extractor import EntityExtractor
from common.singleton_holder import singletons
from common.interruptable_thrift_server import InterruptableThreadPoolServer
from multiprocessing import Process, Queue
from common.topic_manager import TopicManager
from common.mongo_manager import MongoManager
from Queue import Full



class EntityExtractorHandler(object):
    """
    thrift handler
    """
    def __init__(self, conf, worker_pool):
        self.conf = conf
        self.log = log
        self.worker_pool = worker_pool

    def reload(self, topic_id=-1):
        '''重新加载topic schema和解析器'''
        data = {}
        try:
            self.log.info("start reload topic_id[%s]" % topic_id)
            data = singletons[EntityExtractor.__name__].route.reload(topic_id)
            self.worker_pool.reload_all(topic_id)
            self.log.info("finish reload topic_id[%s]" % topic_id)
            msg  = "finish reload topic_id[%s]" % topic_id
        except:
            self.log.error("reload fail reason[%s]" % traceback.format_exc())
            msg = "reload fail reason[%s]" % traceback.format_exc()
        resp = ResultResp(code=1, msg=msg, data=json.dumps(data))
        return resp


    def add_topic(self, topic_info):
        '''添加topic schema'''
        try:
            topic_info = json.loads(topic_info)
            self.log.info("start_add_topic, topic_info[%s]" % topic_info)
            entity_route_obj = singletons[EntityExtractor.__name__].route
            result = entity_route_obj.add_topic(topic_info)
            self.log.info("finish_add_topic, ret[%s]\ttopic_info[%s]" % (result.get('MSG',''), topic_info))
        except:
            self.log.error("add_topic_fail, ret:%s" % traceback.format_exc())

        resp = ResultResp(code=result.get('COSD',10000), msg=result.get('MSG',''), data='')
        return resp

    def add_extractor(self, extractor_info):
        '''添加解析器'''
        try:
            extractor_info = json.loads(extractor_info)
            self.log.info("start_add_extractor, extractor_info[%s]" % extractor_info)
            entity_route_obj = singletons[EntityExtractor.__name__].route
            result = entity_route_obj.insert_extractor(extractor_info)
            self.log.info("finish_add_extractor, ret[%s]\textractor_info[%s]" % (result.get('MSG',''), extractor_info))

            resp = ResultResp(code=result.get('COSD',10000), msg=result.get('MSG',''), data='')
        except:
            self.log.error("add_extractor_fail, ret:%s"%traceback.format_exc())
        return resp

    def entity_extract(self, parse_info):
        """
        :param parse_info: PageParseInfo
        :return: EntityRsp
        """
        try:
            extract_obj = singletons[EntityExtractor.__name__]
            ret = extract_obj.entity_extractor(parse_info)
            entity_extract_data_list = ret.get('LIST', [])
        except:
            msg = "entity_extract_fail, ret:%s" % traceback.format_exc()
            self.log.error(msg)
            resp = EntityResp(code=-10000, msg=msg,
                              entity_data_list=[])
        else:
            resp = EntityResp(code=ret.get('CODE', -10000), msg=ret.get('MSG', ''),
                              entity_data_list=entity_extract_data_list)
        return resp

running = True



def exit_isr(a, b):
    global running
    pool = singletons[EEWorkerPool.__name__]
    log.debug("main proc : waiting for sub processes to die...")
    res = pool.stop_all()
    if res:
        log.debug("main proc : all sub processes gracefully stopped")
        running = False
    else:
        log.debug("main proc : thread pool is not yet in running state")

    # sys.exit(0)



def process_mq_cleaning(conf):
    global running
    signal.signal(signal.SIGTERM, exit_isr)
    signal.signal(signal.SIGINT, exit_isr)

    # This is only used by thrift, each process has its own EntityExtractor instance
    singletons[EntityExtractor.__name__] = EntityExtractor(conf)
    singletons[EEWorkerPool.__name__] = EEWorkerPool(init_count=conf['server']['server_process_num'],
                                                     conf=conf)

    worker_pool = singletons[EEWorkerPool.__name__]
    worker_pool.start_all()
    conf['log'] = log
    heartbeat_thread = HeartbeatThread("entity_extractor", conf)
    heartbeat_thread.start()

    try:
        handler = EntityExtractorHandler(conf, worker_pool)
        processor = EntityExtractorService.Processor(handler)
        transport = TSocket.TServerSocket(port=conf['server']['port'])
        tfactory = TTransport.TBufferedTransportFactory()
        pfactory = TBinaryProtocol.TBinaryProtocolFactory()
        server = InterruptableThreadPoolServer(processor, transport, tfactory, pfactory)
        server.setNumThreads(1)
        server.serve()
    except Exception, e:
        log.error(str(e))
        log.error(traceback.format_exc())

    if running:
        worker_pool.stop_all()


def main(conf):

    pid_file = make_pid_file(sys.path[0])
    try:
        process_mq_cleaning(conf)
    finally:
        remove_pid_file(sys.path[0])



def usaget():
    pass


# sys.argv.extend("-f server1.toml".split())


if __name__ == '__main__':
    from conf import get_config
    try:
        file_path = './entity.toml'
        topic_ids = []
        from_db = False
        opt, args = getopt.getopt(sys.argv[1:], 'f:t:d', ['help'])
        for name, value in opt:
            if name == "-f":
                file_path = value
            elif name == "-t":
                topic_ids = [int(i) for i in value.split(',')]
            elif name == '-d':
                from_db = True
            elif name in ("-h", "--help"):
                usaget()
                sys.exit()
            else:
                assert False, "unhandled option"
        with open(file_path, 'rb') as config:
            conf = pytoml.load(config)
        conf['topic_ids'] = topic_ids
        conf['from_db'] = from_db
        log.init_log(conf, console_out=conf['logger']['console'], name="ee_main_proc")
        main(conf)
    except:
        print traceback.format_exc()
