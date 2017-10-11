# -*- coding: utf-8 -*-

import unittest

from common.conf import conf
from common.log import log
import logging
import os
import pytoml
from common.topic_manager import TopicManager
from common.validate_manager import ValidateManager
from common.singleton_holder import singletons

from i_entity_extractor.single_src_merger import SingleSourceMerger

from i_entity_extractor.entity_extractor import EntityExtractor
import beanstalkc
from bdp.i_crawler.i_extractor.ttypes import PageParseInfo

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer
from i_entity_extractor.entity_extractor_proccessor import EntityExtractorProccessor
import json
import time
from common.db_abstraction_layer import DBBackendFactory

from i_entity_extractor.single_src_merger import make_time_trace_tree

class BSQueueC(object):
    def __init__(self, host="localhost", port=11300):
        self.host = host
        self.port = port
        self.__conn = beanstalkc.Connection(host, port)

    def __del__(self):
        self.__conn.close()

    def put(self, tube, body, priority=2 ** 31, delay=0, ttr=120):
        self.__conn.use(tube)
        return self.__conn.put(body, priority, delay, ttr)

    def reserve(self, tube, timeout=20):
        for t in self.__conn.watching():
            self.__conn.ignore(t)
        self.__conn.watch(tube)
        return self.__conn.reserve(timeout)

    def clear(self, tube):
        try:
            while 1:
                job = self.reserve(tube, 1)
                if job is None:
                    break
                else:
                    job.delete()
        except Exception, e:
            print e

#
# def get_config(dict):
#     config = conf()
#     config.host = dict.get('host')
#     config.port = dict.get('port')
#     config.server_thread_num = dict.get('server_thread_num')
#     config.process_thread_num = dict.get('process_thread_num')
#     config.beanstalk_conf = dict.get('beanstalk_conf')
#     config.MYSQL = dict.get('MYSQL')
#     config.STATISTICS_COLLECTION_NAME = dict.get('STATISTICS_COLLECTION_NAME')
#     # config.MONGODB = dict.get('MONGODB')
#     config.logname = dict.get('logname')
#     config.server = dict.get('server')
#     config.backend = dict.get('backend')
#     config.kafka = dict.get('kafka_server')
#     config.hbase = dict.get('HBASE')
#     return config


class EntityExtractorUnitTest(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        conf_path = os.path.join(os.path.dirname(__file__), 'ee_unittest.toml')
        print conf_path
        with open(conf_path, 'rb') as config:
            config = pytoml.load(config)
        self.conf = config
        log.init_log(self.conf, logging.DEBUG, console_out=False)
        self.topic_manager = TopicManager(self.conf)
        # self.validate_manager = ValidateManager(self.topic_manager, self.conf)
        self.single_merge = SingleSourceMerger(self.topic_manager, self.conf)
        singletons[EntityExtractor.__name__] = EntityExtractor(self.conf)
        self.entity_extractor = singletons[EntityExtractor.__name__]
        self.processor = EntityExtractorProccessor(self.conf)

    def tearDown(self):
        pass

    def get_from_queue(self):
        job = self.queue.reserve(self.conf.beanstalk_conf['input_tube'], timeout=1)
        if job is None:
            return None
        parse_info = PageParseInfo()
        try:
            tMemory_o = TMemoryBuffer(job.body)
            tBinaryProtocol_o = TBinaryProtocol(tMemory_o)
            parse_info.read(tBinaryProtocol_o)
            job.delete()
            return parse_info
        except EOFError, e:
            print "can't read PageParseInfo from string " + str(e)

    def init_queue(self):
        self.queue = BSQueueC(host=self.conf.beanstalk_conf['host'], port=self.conf.beanstalk_conf['port'])

    def test_simple(self):
        self.init_queue()
        for i in xrange(12):
            job = self.get_from_queue()
            if job is None:
                break
            resp = self.entity_extractor.do_single_src_merge(job)
            print "## RESP LIST : " + str(resp["LIST"])

    def test_merge_logic(self):
        # 1. identical structure test
        doc_new = {'name': 'zhan new', 'sub1': {'attr1_in_sub1': 123, 'attr2_in_sub1': 'dong new',
                                                'sub2': {'attr1_in_sub2': 'hello world new'}}}
        doc_old = {'name': 'zhan old', 'sub1': {'attr1_in_sub1': 456, 'attr2_in_sub1': 'dong old',
                                                'sub2': {'attr1_in_sub2': 'hello world old'}}}
        old_time_trace = make_time_trace_tree(doc_old, 50, "www.baidu.com/old")
        old_time_trace['sub1']['attr2_in_sub1'][0] = 150
        print old_time_trace
        self.assertEqual(
            {'name': [50, 'www.baidu.com/old'],
             'sub1': {'attr1_in_sub1': [50, 'www.baidu.com/old'], 'attr2_in_sub1': [150, 'www.baidu.com/old'],
                      'sub2': {'attr1_in_sub2': [50, 'www.baidu.com/old']}}}
            , old_time_trace)

        topic_meta = {}
        plugin_merge_funcs = {}
        merged_doc, merged_trace, new_topic_meta, data_changed = \
            self.single_merge.merge_fragment("www.meizu.com", 567, doc_old, old_time_trace, topic_meta,
                                             doc_new, 100, "www.meizu.com/new", plugin_merge_funcs, [])
        print str(merged_doc)
        print str(merged_trace)
        self.assertEqual({'_doc_merged_times' : 1, 'name': 'zhan new', 'sub1': {'attr1_in_sub1': 123, 'attr2_in_sub1': 'dong old',
                                                       'sub2': {'attr1_in_sub2': 'hello world new'}}}, merged_doc)

        self.assertEqual(
            {'name': [100, 'www.meizu.com/new'],
             'sub1': {'attr1_in_sub1': [100, 'www.meizu.com/new'], 'attr2_in_sub1': [150, 'www.baidu.com/old'],
                      'sub2': {'attr1_in_sub2': [100, 'www.meizu.com/new']}}},merged_trace)

        # 1.5 plugin merge func
        from i_entity_extractor.single_src_merger import SMMergeType
        from i_entity_extractor.extractors.gsxx.gsxx_extractor import primirive_merge
        from i_entity_extractor.extractors.gsxx.gsxx_extractor import GsxxExtractor
        doc_new = {'name': 'zhan new', 'sub1': {'attr1_in_sub1': 123, 'attr2_in_sub1': 'dong new',
                                                'sub2': {'attr1_in_sub2': 'hello world new'}}}
        doc_old = {'name': 'zhan old', 'sub1': {'attr1_in_sub1': 456, 'attr2_in_sub1': 'dong old',
                                                'sub2': {'attr1_in_sub2': 'hello world old'}}}
        old_time_trace = make_time_trace_tree(doc_old, 50, "www.baidu.com/old")
        old_time_trace['sub1']['attr2_in_sub1'][0] = 150
        print old_time_trace
        self.assertEqual(
            {'name': [50, 'www.baidu.com/old'],
             'sub1': {'attr1_in_sub1': [50, 'www.baidu.com/old'], 'attr2_in_sub1': [150, 'www.baidu.com/old'],
                      'sub2': {'attr1_in_sub2': [50, 'www.baidu.com/old']}}}
            , old_time_trace)

        def make_prio_tree(input, prio):
            import copy
            tree = copy.deepcopy(input)
            if type(tree) != dict:
                return prio

            def walk_tree(root, prio):
                for k in root:
                    if type(root[k]) == dict:
                        walk_tree(root[k], prio)
                    else:
                        root[k] = prio

            walk_tree(tree, prio)
            return tree

        old_topic_meta = {GsxxExtractor.GSXX_KEY_PRIO: make_prio_tree(doc_old, 100)}
        new_topic_meta = {GsxxExtractor.GSXX_KEY_PRIO: make_prio_tree(doc_new, 110)}

        merge_func = lambda old, old_trace, input, input_ts, input_url, k, path, existing_meta, spec: \
            primirive_merge(old, old_trace, input, input_ts, input_url, k, path, existing_meta,
                            input_meta=new_topic_meta, spec=spec)

        plugin_merge_funcs = {
            SMMergeType.DEFAULT : merge_func
        }
        merged_doc, merged_trace, new_topic_meta, data_changed = \
            self.single_merge.merge_fragment("www.meizu.com", 567, doc_old, old_time_trace, old_topic_meta,
                                             doc_new, 100, "www.meizu.com/new", plugin_merge_funcs, [])
        print str(merged_doc)
        print str(merged_trace)
        self.assertEqual({'_doc_merged_times' : 1, 'name': 'zhan new', 'sub1': {'attr1_in_sub1': 123, 'attr2_in_sub1': 'dong new',
                                                       'sub2': {'attr1_in_sub2': 'hello world new'}}}, merged_doc)
        self.assertEqual({'name': 110, 'sub1': {'attr1_in_sub1': 110, 'attr2_in_sub1': 110,
                                                       'sub2': {'attr1_in_sub2': 110}}}, new_topic_meta[GsxxExtractor.GSXX_KEY_PRIO])
        self.assertEqual(
            {'name': [100, 'www.meizu.com/new'],
             'sub1': {'attr1_in_sub1': [100, 'www.meizu.com/new'], 'attr2_in_sub1': [100, 'www.meizu.com/new'],
                      'sub2': {'attr1_in_sub2': [100, 'www.meizu.com/new']}}}, merged_trace)

        # 1.6 expect name not to be overwritten

        doc_old = {'name': 'zhan old', 'sub1': {'attr1_in_sub1': 456, 'attr2_in_sub1': 'dong old',
                                                'sub2': {'attr1_in_sub2': 'hello world old'}}}
        old_time_trace = make_time_trace_tree(doc_old, 50, "www.baidu.com/old")
        old_time_trace['sub1']['attr2_in_sub1'][0] = 150
        new_topic_meta = {GsxxExtractor.GSXX_KEY_PRIO: make_prio_tree(doc_new, 110)}
        old_topic_meta = {GsxxExtractor.GSXX_KEY_PRIO: make_prio_tree(doc_new, 100)}

        new_topic_meta[GsxxExtractor.GSXX_KEY_PRIO]['name'] = 50
        merge_func = lambda old, old_trace, input, input_ts, input_url, k, path, existing_meta, spec: \
            primirive_merge(old, old_trace, input, input_ts, input_url, k, path, existing_meta,
                            input_meta=new_topic_meta, spec=spec)

        plugin_merge_funcs = {
            SMMergeType.DEFAULT: merge_func
        }
        merged_doc, merged_trace, new_topic_meta, data_changed = \
            self.single_merge.merge_fragment("www.meizu.com", 567, doc_old, old_time_trace, old_topic_meta,
                                             doc_new, 100, "www.meizu.com/new", plugin_merge_funcs, [])
        print str(merged_doc)
        print str(merged_trace)
        self.assertEqual({'_doc_merged_times' : 1, 'name': 'zhan old', 'sub1': {'attr1_in_sub1': 123, 'attr2_in_sub1': 'dong new',
                                                       'sub2': {'attr1_in_sub2': 'hello world new'}}}, merged_doc)
        self.assertEqual({'name': 100, 'sub1': {'attr1_in_sub1': 110, 'attr2_in_sub1': 110,
                                                'sub2': {'attr1_in_sub2': 110}}}, new_topic_meta[GsxxExtractor.GSXX_KEY_PRIO])
        self.assertEqual(
            {'name': [50, 'www.baidu.com/old'],
             'sub1': {'attr1_in_sub1': [100, 'www.meizu.com/new'], 'attr2_in_sub1': [100, 'www.meizu.com/new'],
                      'sub2': {'attr1_in_sub2': [100, 'www.meizu.com/new']}}}, merged_trace)

        # 1.7 new key when prio merging
        doc_old = {'name': 'zhan old', 'sub1': {'attr1_in_sub1': 456, 'attr2_in_sub1': 'dong old',
                                                'sub2': {'attr1_in_sub2': 'hello world old'}}}
        old_time_trace = make_time_trace_tree(doc_old, 50, "www.baidu.com/old")
        old_time_trace['sub1']['attr2_in_sub1'][0] = 150
        # expect name to be overwritten
        new_topic_meta = {GsxxExtractor.GSXX_KEY_PRIO: make_prio_tree(doc_new, 110)}
        old_topic_meta = {GsxxExtractor.GSXX_KEY_PRIO: make_prio_tree(doc_new, 100)}
        del old_topic_meta[GsxxExtractor.GSXX_KEY_PRIO]['name']

        new_topic_meta[GsxxExtractor.GSXX_KEY_PRIO]['name'] = 50
        merge_func = lambda old, old_trace, input, input_ts, input_url, k, path, existing_meta, spec: \
            primirive_merge(old, old_trace, input, input_ts, input_url, k, path, existing_meta,
                            input_meta=new_topic_meta, spec=spec)

        plugin_merge_funcs = {
            SMMergeType.DEFAULT: merge_func
        }
        merged_doc, merged_trace, new_topic_meta, data_changed = \
            self.single_merge.merge_fragment("www.meizu.com", 567, doc_old, old_time_trace, old_topic_meta,
                                             doc_new, 100, "www.meizu.com/new", plugin_merge_funcs, [])
        print str(merged_doc)
        print str(merged_trace)
        self.assertEqual({'_doc_merged_times' : 1, 'name': 'zhan new', 'sub1': {'attr1_in_sub1': 123, 'attr2_in_sub1': 'dong new', 'sub2': {'attr1_in_sub2': 'hello world new'}}}, merged_doc)
        self.assertEqual({'name': 50, 'sub1': {'attr1_in_sub1': 110, 'attr2_in_sub1': 110,
                                                'sub2': {'attr1_in_sub2': 110}}},
                         new_topic_meta[GsxxExtractor.GSXX_KEY_PRIO])
        self.assertEqual(
            {'name': [100, 'www.meizu.com/new'],
             'sub1': {'attr1_in_sub1': [100, 'www.meizu.com/new'], 'attr2_in_sub1': [100, 'www.meizu.com/new'],
                      'sub2': {'attr1_in_sub2': [100, 'www.meizu.com/new']}}}
            , merged_trace)


        # 2. add new property test
        doc_old = {'name': 'zhan old', 'sub1': {'attr1_in_sub1': 456, 'attr2_in_sub1': 'dong old',
                                                'sub2': {'attr1_in_sub2': 'hello world old'}}}
        old_time_trace = make_time_trace_tree(doc_old, 50, "www.baidu.com/old")
        old_time_trace['sub1']['attr2_in_sub1'][0] = 150
        old_topic_meta = {}
        plugin_merge_funcs = {}
        doc_new = {'name': 'zhan new', 'sub1': {'attr1_in_sub1': 123, 'attr2_in_sub1': 'dong new',
                                                'sub2': {'attr1_in_sub2': 'hello world new'}},
                   'new_sub': {'new_sub_2': {'attr1_in_new_sub2': 'plus one second'}}}
        merged_doc, merged_trace, new_topic_meta, data_changed = self.single_merge.merge_fragment("www.meizu.com", 567, doc_old, old_time_trace, old_topic_meta,
                                                                    doc_new, 100, "www.meizu.com/foobar", plugin_merge_funcs, [])
        print str(merged_doc)
        print str(merged_trace)
        self.assertEqual({'_doc_merged_times' : 1, 'new_sub': {'new_sub_2': {'attr1_in_new_sub2': 'plus one second'}}, 'name': 'zhan new', 'sub1': {'attr1_in_sub1': 123, 'attr2_in_sub1': 'dong old', 'sub2': {'attr1_in_sub2': 'hello world new'}}}, merged_doc)
        self.assertEqual({'new_sub': {'new_sub_2': {'attr1_in_new_sub2': [100, 'www.meizu.com/foobar']}}, 'name': [100, 'www.meizu.com/foobar'], 'sub1': {'attr1_in_sub1': [100, 'www.meizu.com/foobar'], 'attr2_in_sub1': [150, 'www.baidu.com/old'], 'sub2': {'attr1_in_sub2': [100, 'www.meizu.com/foobar']}}}, merged_trace)

        # 3. new doc has fewer attrs
        doc_old = {'name': 'zhan old', 'sub1': {'attr1_in_sub1': 456, 'attr2_in_sub1': 'dong old',
                                                'sub2': {'attr1_in_sub2': 'hello world old'}}}
        old_time_trace = make_time_trace_tree(doc_old, 50, "www.baidu.com/old")
        old_time_trace['sub1']['attr2_in_sub1'][0] = 150
        doc_new = {'name': 'zhan new'}
        merged_doc, merged_trace, new_topic_meta, data_changed = self.single_merge.merge_fragment("www.meizu.com", 567,
                                                                                                  doc_old,
                                                                                                  old_time_trace,
                                                                                                  old_topic_meta,
                                                                                                  doc_new, 100,
                                                                                                  "www.meizu.com/foobar",
                                                                                                  plugin_merge_funcs, [])
        print str(merged_doc)
        print str(merged_trace)
        self.assertEqual({'_doc_merged_times' : 1, 'name': 'zhan new', 'sub1': {'attr1_in_sub1': 456, 'attr2_in_sub1': 'dong old',
                                                       'sub2': {'attr1_in_sub2': 'hello world old'}}}, merged_doc)
        self.assertEqual(
            {'name': [100, 'www.meizu.com/foobar'],
             'sub1': {'attr1_in_sub1': [50, 'www.baidu.com/old'], 'attr2_in_sub1': [150, 'www.baidu.com/old'],
                      'sub2': {'attr1_in_sub2': [50, 'www.baidu.com/old']}}}, merged_trace)

        # 4. all new doc

        doc_new = {'name': 'zhan new', 'sub1': {'attr1_in_sub1': 123, 'attr2_in_sub1': 'dong new',
                                                'sub2': {'attr1_in_sub2': 'hello world new'}}}
        doc_old = {}
        old_time_trace = {}
        merged_doc, merged_trace, new_topic_meta, data_changed = self.single_merge.merge_fragment("www.meizu.com", 567,
                                                                                                  doc_old,
                                                                                                  old_time_trace,
                                                                                                  old_topic_meta,
                                                                                                  doc_new, 100,
                                                                                                  "www.meizu.com/foobar",
                                                                                                  plugin_merge_funcs, [])
        print str(merged_doc)
        print str(merged_trace)
        self.assertEqual({'_doc_merged_times' : 1, 'name': 'zhan new', 'sub1': {'attr1_in_sub1': 123, 'attr2_in_sub1': 'dong new',
                                                       'sub2': {'attr1_in_sub2': 'hello world new'}}}, merged_doc)
        self.assertEqual(
            {'name': [100, 'www.meizu.com/foobar'],
             'sub1': {'attr1_in_sub1': [100, 'www.meizu.com/foobar'], 'attr2_in_sub1': [100, 'www.meizu.com/foobar'],
                      'sub2': {'attr1_in_sub2': [100, 'www.meizu.com/foobar']}}}
            ,
            merged_trace)

        # 5. sort append unique merge with specific site
        from i_util.tools import put_to_dict_path
        from i_entity_extractor.single_src_merger import SingleSrcMergeRule
        doc_new = {'name': 'zhan new', 'sub1': {'attr1_in_sub1': 123, 'attr2_in_sub1': 'dong new',
                                                'sub2': {'attr1_in_sub2': 'hello world new',
                                                         'list_attr': [{'year': '2012-1-2', 'data': 'AAA'},
                                                                       {'year': '2012-1-3', 'data': 'BBB'},
                                                                       {'year': '2012-1-1', 'data': 'TTT'}]}}}
        doc_old = {'name': 'zhan old', 'sub1': {'attr1_in_sub1': 456, 'attr2_in_sub1': 'dong old',
                                                'sub2': {'attr1_in_sub2': 'hello world old',
                                                         'list_attr': [{'year': '2014-1-2', 'data': 'DDD'},
                                                                       {'year': '2012-1-3', 'data': 'BBB'},
                                                                       {'year': '2015-1-1', 'data': 'VVV'}]}}}
        old_time_trace = make_time_trace_tree(doc_old, 50, "www.baidu.com")
        path = ['sub1', 'sub2', 'list_attr']
        content = '{"type":"append", "sort_keys":[["year","time"],["data","default"]]}'
        put_to_dict_path(self.single_merge.topic_site_rule_dict, [567, 'www.meizu.com'] + ['.'.join(path)],
                         SingleSrcMergeRule.make_test_rule('www.meizu.com', 567, '.'.join(path), content))
        merged_doc, merged_trace, new_topic_meta, data_changed = self.single_merge.merge_fragment("www.meizu.com", 567,
                                                                                                  doc_old,
                                                                                                  old_time_trace,
                                                                                                  old_topic_meta,
                                                                                                  doc_new, 100,
                                                                                                  "www.meizu.com/foobar",
                                                                                                  plugin_merge_funcs, [])
        print str(merged_doc)
        print str(merged_trace)
        self.assertEqual({'_doc_merged_times' : 1, 'name': 'zhan new', 'sub1': {'attr1_in_sub1': 123, 'attr2_in_sub1': 'dong new',
                                                       'sub2': {'attr1_in_sub2': 'hello world new',
                                                                'list_attr': [{'data': 'TTT', 'year': '2012-1-1'},
                                                                              {'data': 'AAA', 'year': '2012-1-2'},
                                                                              {'data': 'BBB', 'year': '2012-1-3'},
                                                                              {'data': 'DDD', 'year': '2014-1-2'},
                                                                              {'data': 'VVV', 'year': '2015-1-1'}]}}},
                         merged_doc)

        self.assertEqual({'name': [100, 'www.meizu.com/foobar'],
                          'sub1': {'attr1_in_sub1': [100, 'www.meizu.com/foobar'],
                                   'attr2_in_sub1': [100, 'www.meizu.com/foobar'],
                                   'sub2': {'attr1_in_sub2': [100, 'www.meizu.com/foobar'], 'list_attr': [
                                       {'data': [100, 'www.meizu.com/foobar'], 'year': [100, 'www.meizu.com/foobar']},
                                       {'data': [100, 'www.meizu.com/foobar'], 'year': [100, 'www.meizu.com/foobar']},
                                       {'data': [100, 'www.meizu.com/foobar'], 'year': [100, 'www.meizu.com/foobar']},
                                       {'data': [50, 'www.baidu.com'], 'year': [50, 'www.baidu.com']},
                                       {'data': [50, 'www.baidu.com'], 'year': [50, 'www.baidu.com']}]}}},
                         merged_trace)

        # 6. sort append unique merge with wildcard site
        doc_old = {'name': 'zhan old', 'sub1': {'attr1_in_sub1': 456, 'attr2_in_sub1': 'dong old',
                                                'sub2': {'attr1_in_sub2': 'hello world old',
                                                         'list_attr': [{'year': '2014-1-2', 'data': 'DDD'},
                                                                       {'year': '2012-1-3', 'data': 'BBB'},
                                                                       {'year': '2015-1-1', 'data': 'VVV'}]}}}
        old_time_trace = make_time_trace_tree(doc_old, 50, "www.baidu.com")
        put_to_dict_path(self.single_merge.topic_site_rule_dict, [567, '*'] + ['.'.join(path)],
                         SingleSrcMergeRule.make_test_rule('*', 567, '.'.join(path), content))
        merged_doc, merged_trace, new_topic_meta, data_changed = self.single_merge.merge_fragment("www.meizu.com", 567,
                                                                                                  doc_old,
                                                                                                  old_time_trace,
                                                                                                  old_topic_meta,
                                                                                                  doc_new, 100,
                                                                                                  "www.meizu.com/foobar",
                                                                                                  plugin_merge_funcs, [])
        print str(merged_doc)
        print str(merged_trace)
        self.assertEqual({'_doc_merged_times' : 1, 'name': 'zhan new', 'sub1': {'attr1_in_sub1': 123, 'attr2_in_sub1': 'dong new',
                                                       'sub2': {'attr1_in_sub2': 'hello world new',
                                                                'list_attr': [{'data': 'TTT', 'year': '2012-1-1'},
                                                                              {'data': 'AAA', 'year': '2012-1-2'},
                                                                              {'data': 'BBB', 'year': '2012-1-3'},
                                                                              {'data': 'DDD', 'year': '2014-1-2'},
                                                                              {'data': 'VVV', 'year': '2015-1-1'}]}}},
                         merged_doc)
        self.assertEqual({'name': [100, 'www.meizu.com/foobar'],
                          'sub1': {'attr1_in_sub1': [100, 'www.meizu.com/foobar'],
                                   'attr2_in_sub1': [100, 'www.meizu.com/foobar'],
                                   'sub2': {'attr1_in_sub2': [100, 'www.meizu.com/foobar'], 'list_attr': [
                                       {'data': [100, 'www.meizu.com/foobar'], 'year': [100, 'www.meizu.com/foobar']},
                                       {'data': [100, 'www.meizu.com/foobar'], 'year': [100, 'www.meizu.com/foobar']},
                                       {'data': [100, 'www.meizu.com/foobar'], 'year': [100, 'www.meizu.com/foobar']},
                                       {'data': [50, 'www.baidu.com'], 'year': [50, 'www.baidu.com']},
                                       {'data': [50, 'www.baidu.com'], 'year': [50, 'www.baidu.com']}]}}},
                         merged_trace)

        # 7. sort append unique merge : all new field
        doc_old = {'name': 'zhan old', 'sub1': {'attr1_in_sub1': 456, 'attr2_in_sub1': 'dong old',
                                                'sub2': {'attr1_in_sub2': 'hello world old'}}}
        old_time_trace = make_time_trace_tree(doc_old, 50, "www.baidu.com")

        merged_doc, merged_trace, new_topic_meta, data_changed = self.single_merge.merge_fragment("www.meizu.com", 567,
                                                                                                  doc_old,
                                                                                                  old_time_trace,
                                                                                                  old_topic_meta,
                                                                                                  doc_new, 100,
                                                                                                  "www.meizu.com/foobar",
                                                                                                  plugin_merge_funcs, [])
        print str(merged_doc)
        print str(merged_trace)
        self.assertEqual({'_doc_merged_times' : 1, 'name': 'zhan new', 'sub1': {'attr1_in_sub1': 123, 'attr2_in_sub1': 'dong new',
                                                       'sub2': {'attr1_in_sub2': 'hello world new',
                                                                'list_attr': [{'data': 'TTT', 'year': '2012-1-1'},
                                                                              {'data': 'AAA', 'year': '2012-1-2'},
                                                                              {'data': 'BBB', 'year': '2012-1-3'}]}}},
                         merged_doc)
        self.assertEqual({'name': [100, 'www.meizu.com/foobar'],
                          'sub1': {'attr1_in_sub1': [100, 'www.meizu.com/foobar'],
                                   'attr2_in_sub1': [100, 'www.meizu.com/foobar'],
                                   'sub2': {'attr1_in_sub2': [100, 'www.meizu.com/foobar'], 'list_attr': [
                                       {'data': [100, 'www.meizu.com/foobar'], 'year': [100, 'www.meizu.com/foobar']},
                                       {'data': [100, 'www.meizu.com/foobar'], 'year': [100, 'www.meizu.com/foobar']},
                                       {'data': [100, 'www.meizu.com/foobar'],
                                        'year': [100, 'www.meizu.com/foobar']}]}}}
                         , merged_trace)

        # 8. default merge rule for list : union
        doc_new = {'name': 'zhan new', 'sub1': {'attr1_in_sub1': 123, 'attr2_in_sub1': 'dong new',
                                                'sub2': {'attr1_in_sub2': 'hello world new',
                                                         'list_attr': [{'year': '2012-1-2', 'data': 'AAA'},
                                                                       {'year': '2012-1-3', 'data': 'BBB'},
                                                                       {'year': '2012-1-1', 'data': 'TTT'}],
                                                         'not_configured_list' : [
                                                             {'aaa': 'fff', 'bbb': 132},
                                                             {'bb': 'fff', 'ddd': "555"},
                                                             {'a': 'fff', 'jjj': "dfvf"}
                                                         ]}}}
        doc_old = {'name': 'zhan old', 'sub1': {'attr1_in_sub1': 456, 'attr2_in_sub1': 'dong old',
                                                'sub2': {'attr1_in_sub2': 'hello world old',
                                                         'list_attr': [{'year': '2014-1-2', 'data': 'DDD'},
                                                                       {'year': '2011-1-3', 'data': 'CCC'},
                                                                       {'year': '2015-1-1', 'data': 'VVV'}],
                                                         'not_configured_list': [
                                                             {'hh': 'fff', 'bbb': 2342},
                                                             {'ee': 'fff', 'ddd': "ghn"},
                                                             {'lll': 'fff', 'jjj': "45567"}
                                                         ]
                                                         }}}
        old_time_trace = make_time_trace_tree(doc_old, 50, "www.baidu.com")
        path = ['sub1', 'sub2', 'list_attr']
        content = '{"type":"append", "unique":true, "sort_keys":[["year","time"],["data","default"]]}'
        put_to_dict_path(self.single_merge.topic_site_rule_dict, [567, 'www.meizu.com'] + ['.'.join(path)],
                         SingleSrcMergeRule.make_test_rule('www.meizu.com', 567, '.'.join(path), content))
        merged_doc, merged_trace, new_topic_meta, data_changed = self.single_merge.merge_fragment("www.meizu.com", 567,
                                                                                                  doc_old,
                                                                                                  old_time_trace,
                                                                                                  old_topic_meta,
                                                                                                  doc_new, 100,
                                                                                                  "www.meizu.com/foobar",
                                                                                                  plugin_merge_funcs, [])
        print str(merged_doc)
        print str(merged_trace)

        self.assertEqual({'_doc_merged_times' : 1, 'name': 'zhan new', 'sub1': {'attr1_in_sub1': 123, 'attr2_in_sub1': 'dong new',
                                                       'sub2': {'attr1_in_sub2': 'hello world new',
                                                                'not_configured_list': [{'a': 'fff', 'jjj': 'dfvf'},
                                                                                        {'aaa': 'fff', 'bbb': 132},
                                                                                        {'bb': 'fff', 'ddd': '555'},
                                                                                        {'hh': 'fff', 'bbb': 2342},
                                                                                        {'ee': 'fff', 'ddd': 'ghn'},
                                                                                        {'jjj': '45567', 'lll': 'fff'}],
                                                                'list_attr': [{'data': 'CCC', 'year': '2011-1-3'},
                                                                              {'data': 'TTT', 'year': '2012-1-1'},
                                                                              {'data': 'AAA', 'year': '2012-1-2'},
                                                                              {'data': 'BBB', 'year': '2012-1-3'},
                                                                              {'data': 'DDD', 'year': '2014-1-2'},
                                                                              {'data': 'VVV', 'year': '2015-1-1'}]}}},
                         merged_doc)
        self.assertEqual({'name': [100, 'www.meizu.com/foobar'],
                          'sub1': {'attr1_in_sub1': [100, 'www.meizu.com/foobar'],
                                   'attr2_in_sub1': [100, 'www.meizu.com/foobar'],
                                   'sub2': {'attr1_in_sub2': [100, 'www.meizu.com/foobar'], 'not_configured_list': [
                                       {'a': [100, 'www.meizu.com/foobar'], 'jjj': [100, 'www.meizu.com/foobar']},
                                       {'aaa': [100, 'www.meizu.com/foobar'], 'bbb': [100, 'www.meizu.com/foobar']},
                                       {'bb': [100, 'www.meizu.com/foobar'], 'ddd': [100, 'www.meizu.com/foobar']},
                                       {'hh': [50, 'www.baidu.com'], 'bbb': [50, 'www.baidu.com']},
                                       {'ee': [50, 'www.baidu.com'], 'ddd': [50, 'www.baidu.com']},
                                       {'jjj': [50, 'www.baidu.com'], 'lll': [50, 'www.baidu.com']}], 'list_attr': [
                                       {'data': [50, 'www.baidu.com'], 'year': [50, 'www.baidu.com']},
                                       {'data': [100, 'www.meizu.com/foobar'], 'year': [100, 'www.meizu.com/foobar']},
                                       {'data': [100, 'www.meizu.com/foobar'], 'year': [100, 'www.meizu.com/foobar']},
                                       {'data': [100, 'www.meizu.com/foobar'], 'year': [100, 'www.meizu.com/foobar']},
                                       {'data': [50, 'www.baidu.com'], 'year': [50, 'www.baidu.com']},
                                       {'data': [50, 'www.baidu.com'], 'year': [50, 'www.baidu.com']}]}}}
                         , merged_trace)

    pass

    def test_integration(self):
        import pickle
        import cProfile
        from i_util.pyhbase.HBaseConnection import HBaseThrift2Connection
        from i_util.tools import crawler_basic_path
        from common.record_lock_manager import RecordLockManager

        singletons[RecordLockManager.__name__] = RecordLockManager(self.conf, prefix="single-src",
                                                                backlog_path=os.path.join(crawler_basic_path,
                                                                                                'ee_backlogs'),
                                                                ttl_sec=600)


        db = DBBackendFactory.get_backend(self.conf)
        # hbase = HBaseThrift2Connection(conf=self.conf)
        db.drop_table("enterprise_data_gov_single_src")
        db.create_table("enterprise_data_gov_single_src")

        dump_path = os.path.join(os.path.dirname(__file__), 'big.dump')
        with open(dump_path, 'rb') as ifile:
            bodies = pickle.load(ifile)

        dump_path = os.path.join(os.path.dirname(__file__), 'no_topo.dump')
        with open(dump_path, 'rb') as ifile:
            bodies.extend(pickle.load(ifile))

        dump_path = os.path.join(os.path.dirname(__file__), 'chongqing_48_v2.dump')
        with open(dump_path, 'rb') as ifile:
            bodies.extend(pickle.load(ifile))


        import random
        random.shuffle(bodies)

        bodies = bodies[:1000]

        with open(os.path.join(os.path.dirname(__file__), 'random_mixed.dump'), "w") as r:
            pickle.dump(bodies, r)

        print len(bodies)

        import sys

        profiler = cProfile.Profile()
        count_none = 0
        count_empty = 0
        count_out = 0

        seen_rec_ids = set([])
        singletons['ids'] = set([])
        singletons['ids_md5'] = set([])
        singletons['rks'] = set([])
        singletons['companies'] = set([])

        ofile = open("enterprise_data_gov_single_src.json", "wb")
        jsons = []

        for body in bodies:
            parse_info = PageParseInfo()
            tMemory_o = TMemoryBuffer(body)
            tBinaryProtocol_o = TBinaryProtocol(tMemory_o)
            parse_info.read(tBinaryProtocol_o)
            if 'gov' not in parse_info.base_info.site:
                continue
            profiler.enable()
            msg_list = self.processor.do_task(body)
            profiler.disable()
            if msg_list is None:
                count_none +=1
            else:
                # print len(msg_list)
                if len(msg_list) == 0:
                    count_empty += 1
                else:
                    from bdp.i_crawler.i_entity_extractor.ttypes import EntityExtractorInfo, EntitySource
                    entity_info = EntityExtractorInfo()
                    tMemory_o = TMemoryBuffer(msg_list[0])
                    tBinaryProtocol_o = TBinaryProtocol(tMemory_o)
                    entity_info.read(tBinaryProtocol_o)
                    jsons.append(json.loads(entity_info.entity_data))
                    count_out += 1

        profiler.dump_stats(os.path.join(os.path.dirname(__file__), 'ee.prof'))

        jres = json.dumps(jsons).decode("utf-8").encode("utf-8").decode('unicode-escape')
        ofile.write(jres)
        ofile.close()
        # print jres
        # print json.dumps(jsons[-1]).decode("utf-8").encode("utf-8").decode('unicode-escape')
        print "total input count = %d" % len(bodies)
        print "empty output count = %d" % count_empty
        print "schema valid output count = %d" % count_out
        # print len(singletons['ids']), len(singletons['ids_md5']), len(singletons['rks'])


        # print sorted(list(singletons['companies']))

        # hbase.drop_table("enterprise_data_gov_single_src")
        # hbase.create_table("enterprise_data_gov_single_src")

        # profiler.enable()
        # t1 = time.time() * 1000
        # for body in bodies:
        #     self.processor.do_task(body)
        # t2 = time.time() * 1000
        # profiler.disable()


        # profiler.dump_stats("single_src.prof")

        # t2 = time.time() * 1000
        # print "Merging %d docs costs %d ms" % (len(bodies), t2 - t1)
        # res_row = hbase.get("enterprise_data_gov_single_src", "bb8928f4c03e2ade3353f5c5d968859693ce15ccd0ba0a2c577f1e18a2d4c31d")
        # res = res_row['latest']
        # meta = res_row['meta']
        # from expect_res import xizang12_expect_res
        # from expect_res import xizang12_expect_trace
        #
        # # latest_merged_time cannot be compared
        # del meta['latest_merged_time']
        # del xizang12_expect_trace['latest_merged_time']
        #
        # self.maxDiff = 99999999
        # self.assertEqual(res, xizang12_expect_res)
        # self.assertEqual(meta, xizang12_expect_trace)
        # # hbase.drop_table("enterprise_data_gov_single_src")


    def test_hbase(self):
        import pickle
        import cProfile
        from i_util.pyhbase.HBaseConnection import HBaseThrift2Connection

        hbase = HBaseThrift2Connection(conf=self.conf)
        rows = hbase.scan('enterprise_data_gov_single_src', 1000)
        for row in rows:
            print row



def casesuite():
    suite = unittest.TestSuite()
    suite.addTest(EntityExtractorUnitTest("test_integration"))
    unittest.TextTestRunner().run(suite)
