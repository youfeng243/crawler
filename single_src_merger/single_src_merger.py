# coding=utf8
import sys

sys.path.append('..')
from bdp.i_crawler.i_extractor.ttypes import ExStatus
import json
import traceback
import time
import copy
from single_src_merger_impl import SingleSourceMergerImpl
from common.topic_manager import TopicManager
from common.validate_manager import ValidateManager, ServerType
from common.log import log
from i_util.global_defs import HBaseDefs, MetaFields

from common.singleton_holder import singletons
from common.record_lock_manager import RecordLockManager
from i_util.tools import get_md5, get_metaless_doc, lookup_dict_path, TopoInfoGenerator, get_record_id_new



class SingleSourceMerger(object):


    def __init__(self, conf):
        self.conf = conf
        self.log = log
        self.topic_manager = TopicManager(conf)
        self.single_src_merger_impl = SingleSourceMergerImpl(self.topic_manager, conf)
        self.count = 0

    def reload(self, topic_id):
        self.topic_manager.reload(topic_id)
        self.single_src_merger_impl.reload_merge_rule()
        table_name = self.topic_manager.get_table_name_by_id(topic_id)
        if table_name:
            self.single_src_merger_impl.conn.create_table(table_name+"_single_src")
        return True

    #
    # Currently this code is not used.
    # We keep it around only to be a reminder of how to use the TOPO_TOKEN mechanism !
    #
    #
    # def before_merge(self, doc, new_page_meta):
    #     from i_util.tools import TopoInfoGenerator
    #     from i_util.global_defs import MetaFields
    #     topo_info = None
    #
    #     if MetaFields.TOPO_TOKEN in doc:
    #         topo_info = TopoInfoGenerator(token=doc[MetaFields.TOPO_TOKEN])
    #
    #         doc[MetaFields.TOPO_TOKEN_RAW] = doc[MetaFields.TOPO_TOKEN]
    #         doc[MetaFields.TOPO_TOKEN] = topo_info.get_match_token()
    #         doc[MetaFields.TOPO_TOKEN_INCOMMING] = topo_info.get_match_token()
    #         doc[MetaFields.UUID] = topo_info.get_uuid()
    #
    #         def preprocess_topo_info(doc, existing_attr, tag):
    #             if existing_attr in doc:
    #                 if topo_info.tag == 'base_info':
    #                     topo_info_new = TopoInfoGenerator(tag=tag)
    #                     topo_info_new.uuid = topo_info.uuid
    #                     topo_info_new.add_dim()
    #                     topo_info_new.add_dim()
    #                     topo_info_new.set_index_dim(0, 1, 2) # the 2 here is not used
    #                 else:
    #                     topo_info_new = copy.deepcopy(topo_info)
    #                     topo_info_new.add_dim()
    #                 len = doc[existing_attr]
    #                 for i, single_record in enumerate(doc[existing_attr]):
    #                     topo_info_new.set_index_dim(1, i + 1, len)
    #                     single_record[MetaFields.TOPO_TOKEN] = topo_info_new.get_match_token()
    #                     single_record[MetaFields.TOPO_TOKEN_LOCAL] = topo_info_new.get_match_token()
    #
    #         def unpack_list_first_element(single_record, existing_attr):
    #             if existing_attr in single_record:
    #                 if type(single_record[existing_attr]) == list and len(single_record[existing_attr]) == 1:
    #                     single_record[existing_attr + "_detail"] = single_record[existing_attr][0]
    #                 del single_record[existing_attr]
    #
    #         if topo_info.get_tag() == 'base_info':
    #             preprocess_topo_info(doc, 'changerecords', 'change_info')
    #             preprocess_topo_info(doc, 'contributor_information', 'contributive_info')
    #         else:
    #             def preprocess_topo_info_1(doc, attr):
    #                 preprocess_topo_info(doc, attr, topo_info.get_tag())
    #             if len(topo_info.indexes) == 1:
    #                 preprocess_topo_info_1(doc, 'changerecords')
    #                 preprocess_topo_info_1(doc, 'contributor_information')
    #             elif len(topo_info.indexes) == 2:
    #                 unpack_list_first_element(doc, 'changerecords')
    #                 unpack_list_first_element(doc, 'contributor_information')
    #
    #     if topo_info is not None and not topo_info.is_base_doc():
    #         if 'company' in doc:
    #             del doc['company']
    #
    #     is_baseinfo_page = doc.get('_is_baseinfo_page', False)
    #
    #     # Other pages may not contain these attrs
    #     if not is_baseinfo_page:
    #         if 'city' in doc:
    #             del doc['city']
    #         if 'province' in doc:
    #             del doc['province']
    #
    #     new_page_meta[GsxxExtractor.GSXX_KEY_PRIO] = {}
    #     if topo_info is None or topo_info.is_base_doc():
    #         for k in doc:
    #             if type(doc[k]) not in [list, dict]:
    #                 new_page_meta[GsxxExtractor.GSXX_KEY_PRIO][k] = \
    #                     GsxxExtractor.BASE_INFO_PAGE_PRIORITY if is_baseinfo_page else GsxxExtractor.NORMAL_PAGE_PRIORITY
    #     if topo_info is not None and topo_info.get_tag() is not None and (topo_info.dims is None or len(topo_info.dims) < 2):
    #         # Force list page to be merged into base info
    #         topo_info.set_tag('base_info')
    #         topo_info.dims = []
    #         topo_info.indexes = []
    #         doc[MetaFields.TOPO_TOKEN] = topo_info.get_match_token()

    def get_schedule_unique_id(self, page_info):
        return str(json.loads(page_info.scheduler)['schedule_time'])

    def process_single_entity_data(self, parse_info, base_info, download_time, extract_data, topic_id, resp, num_extract_data):
        topo_token_based = False
        topo_token = '[not topo_token based]'
        if MetaFields.TOPO_TOKEN in extract_data:
            topo_token = extract_data[MetaFields.TOPO_TOKEN]
            topo_token_based = True

        site_record_id = '[not present]'
        if MetaFields.SITE_RECORD_ID in extract_data:
            site_record_id = extract_data[MetaFields.SITE_RECORD_ID]
        else:
            log.error("Got doc without _site_record_id! url = %s, download time = %d", base_info.url, download_time)
            return

        log.debug("Begin process_single_entity_data : topo_token = %s, site_record_id = %s", topo_token, site_record_id)

        try:
            # 1. Preliminary extract result is extract_data, which contains incomming fragment.
            # 2. In after_entity_extract, we do some formatting and etc, the result is a doc
            #    that complies with schema. However, this doc may lack some fields, which may
            #    (or may not) be compensated with single source merging. These docs are called
            #    'doc fragments'.
            # 3. Single Source Merging. This step merges newly generated doc fragment with the
            #    previous ones, which are loaded from HBase. The result is stored in HBase and
            #    passed to down-stream with MQ. In this step, a site-wise sequence number is
            #    generated. It is used during multi source merging.


            self.single_src_merger_impl.before_merge(extract_data, parse_info)

            # Do the single source merging
            merged_doc_fragment, existing_doc_fragment, key, meta, data_changed, table_name \
                = self.single_src_merger_impl.process(download_time, base_info.site, base_info.url, extract_data,
                                                      topic_id, {})

            if merged_doc_fragment == None:
                log.debug("topo_token = %s not merged", topo_token)
                return
            output_doc = get_metaless_doc(merged_doc_fragment)
            output_check = get_metaless_doc(output_doc, set())
            existing_check = get_metaless_doc(existing_doc_fragment, set())
            same_doc_flag = output_check == existing_check
            if same_doc_flag:
                # No changes made, save the trouble
                log.debug("topo_token = %s no changes made", topo_token)

            # final_data的record_id 不需要依赖schema, 因而计算record_id 时不取schema,这里的record_id 不能传到entity_extractor
            # 如果有site_record_id 就会取site_record_id + site进行计算record_id, 详情看record_id计算算法实现
            # 否则取url来计算
            #
            if MetaFields.RECORD_ID not in output_doc:
                output_doc[MetaFields.RECORD_ID] = \
                    get_record_id_new([], base_info.url, output_doc)
                merged_doc_fragment[MetaFields.RECORD_ID] = output_doc[MetaFields.RECORD_ID]

            if topo_token_based:
                topo_info = TopoInfoGenerator(token=topo_token)
                output_doc[MetaFields.DOC_UNIQUE_ID] = topo_info.get_uuid()
            else:
                output_doc[MetaFields.DOC_UNIQUE_ID] = "MULTI_VERSION_NOT_ENABLED"
            meta[HBaseDefs.SINGLE_SRC_CURRENT_OUTPUT_DOC_UNIQUE_ID] = output_doc[MetaFields.DOC_UNIQUE_ID]

            resp["LIST"].append(self.pack_result(base_info, output_doc, topic_id, url=base_info.url,
                                                 download_time=download_time))
            resp["MSG"] += " %s extract_data in extract_data_list parser success" % num_extract_data


        except Exception, e:
            print traceback.format_exc()
            self.log.error("extract_error\tmsg:%s" % (traceback.format_exc()))
            resp["MSG"] += " %s extract_data in extract_data_list error, ret:[%s] " % (
                num_extract_data, traceback.format_exc())
            resp["CODE"] = -10000

    def process_backlog(self, resp, count=10):
        lock_mgr = singletons[RecordLockManager.__name__]
        for i in xrange(count):
            backlog_task = lock_mgr.choose_backlog_and_lock_it()
            if backlog_task is not None:
                log.debug("Process backlog " + backlog_task.pickle_name)
                # restore context
                base_info = backlog_task.saved_context.base_info
                extract_info = backlog_task.saved_context.extract_info
                topic_id = extract_info.topic_id
                download_time = backlog_task.saved_context.crawl_info.download_time
                # extractor = self.route.get_extractor(topic_id)
                self.process_single_entity_data(backlog_task.saved_context, base_info, download_time, backlog_task.data,
                                                topic_id, resp, i)
                lock_mgr.commit_task(backlog_task)

    def process_data_list(self, extract_info):
        extract_data_list = []
        root_node = json.loads(extract_info.extract_data)

        if root_node and root_node.has_key("datas"):

            if (not isinstance(root_node["datas"], basestring)) and isinstance(root_node["datas"], list) and root_node[
                "datas"]:

                for data in root_node["datas"]:
                    for key, value in root_node.items():
                        if key != "datas" and (not data.has_key(key)):
                            data[key] = value
                    extract_data_list.append(data)
            else:
                self.log.info("datas_error, extract_data:%s" % root_node)
                return extract_data_list

        if len(extract_data_list) == 0:
            extract_data_list.append(root_node)
        return extract_data_list

    def result(self):
        resp = {
            "CODE": 10000,
            "MSG": "",
            "LIST": [],
        }
        return resp

    def do_single_src_merge(self, parse_info):
        resp = self.result()
        # Backlog function is currently turned off
        # self.process_backlog(resp, 10)
        try:
            if parse_info is None:
                # None is used to trigger backlog processing
                pass
            else:
                begin_time = time.time()
                base_info = parse_info.base_info
                extract_info = parse_info.extract_info
                ex_status = extract_info.ex_status
                topic_id = extract_info.topic_id
                download_time = parse_info.crawl_info.download_time

                if topic_id and topic_id != -1:
                    topic_id = int(topic_id)
                    resp['TOPIC_ID'] = topic_id
                else:
                    resp['CODE']   = -10000
                    resp['MSG']    = 'topic_id error, topic_id: %s' % topic_id
                    self.log.warning('topic_id_error, topic_id: %s url : %s' % (topic_id, base_info.url))
                    return resp

                extract_data_len = len(extract_info.extract_data) if extract_info.extract_data else 0

                if ex_status != ExStatus.kEsSuccess or extract_data_len == 0:
                    resp['CODE'] = -10000
                    resp['MSG'] = "extract_status fail or extract_data_len = 0"
                else:
                    extract_data_list = self.process_data_list(extract_info)
                    lock_mgr = singletons[RecordLockManager.__name__]
                    saved_entity_data_context = None
                    for i, extract_data in enumerate(extract_data_list):

                        site = base_info.site

                        if MetaFields.SITE_RECORD_ID not in extract_data:
                            log.error("doc doesn't have _site_record_id topic_id = %d, site = %s, url = %s : %s"
                                      % (topic_id, site, base_info.url, json.dumps(base_info.url)))
                            continue

                        lock_name = unicode(site + '-' + extract_data[MetaFields.SITE_RECORD_ID]).upper()
                        task = lock_mgr.make_task(lock_name, extract_data)
                        try:
                            lock_mgr.try_lock(task, block=True)
                            if task.lock is not None:
                                log.debug("Process non-backlog " + task.pickle_name)
                                self.process_single_entity_data(parse_info, base_info, download_time,
                                                                extract_data, topic_id, resp, i)
                                lock_mgr.commit_task(task)
                            else:
                                # Backlog function is currently turned off
                                log.debug("Store backlog " + task.pickle_name)
                                if saved_entity_data_context is None:
                                    # We need to save context as well, in order to restore when processing backlog
                                    saved_entity_data_context = copy.deepcopy(parse_info)
                                    saved_entity_data_context.extract_info.extract_data = None
                                lock_mgr.store_backlog(task, saved_entity_data_context)
                        except Exception, e:
                            self.log.error("uncaught exception\t%s\tmsg:%s" % (str(e), traceback.format_exc()))
                        finally:
                            lock_mgr.commit_task(task)
                end_time = time.time()
                self.log.info("finish_entity_extract\turl:%s\ttopic_id:%s\ttimecost:%.2f" % (base_info.url, topic_id, (end_time - begin_time) * 1000))
        except Exception, e:
            self.log.error("extract_error\t%s\tmsg:%s" % (str(e), traceback.format_exc()))
        return resp

    def pack_result(self, base_info, entity_data_json, topic_id, url=None, download_time=None):
        if url is None:
            url = base_info.url
        if "_topic_id" not in entity_data_json:
            entity_data_json['_topic_id'] = topic_id
        entity_data_json[MetaFields.SRC] = [{"site":base_info.site, "download_time":download_time, "url":url}]
        return json.dumps(entity_data_json)
