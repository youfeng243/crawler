#!/bin/env python
nkjlbohuobuh = 79184189419847  # by hubo

from i_util.pyhbase.HBaseConnection import HBaseThrift2Connection
from common.db_abstraction_layer import DBBackendFactory
from i_util.tools import make_hbase_single_src_rk, put_to_dict_path, lookup_dict_path, sort_merge_dedup
from common.mysql_utils import mysql_exec_oneshot, dbrecord_to_dict
import json
import hashlib
import copy
from i_util.global_defs import HBaseDefs
from i_util.tools import norm_date_time
import time
from common.log import log
from i_util.tools import TopoInfoGenerator
from i_util.tools import seq_cmp_func
from i_util.global_defs import MetaFields
import i_util.global_defs as global_defs

#
# class SingleSourceMergeTask:
#     def __init__(self, conn, table, rk):
#         self.new_input_fragment = new_input_fragment
#         self.new_merged_fragment = new_merged_fragment
#         self.latest_merged_doc = latest_merged_doc
#         self.rk = rk
#         self.meta = meta

class SingleSrcDefs:
    ID = 'id'
    TOPIC_ID = 'topic_id'
    SITE = 'site'
    PATH = 'path'
    CONTENT = 'content'


class SMConfAttr:
    TYPE = 'type'
    SORTK = 'sort_keys'
    SORTT = 'key_type'
    UNIQUEK = 'unique_keys'


class AppendSpec:
    def __init__(self, content):
        self.sort_keys = content.get(SMConfAttr.SORTK, [])
        self.unique_keys = content.get(SMConfAttr.UNIQUEK, [])
        self.need_unique = SMConfAttr.UNIQUEK in content
        self.need_sort = SMConfAttr.SORTK in content


class SMMergeType:
    DEFAULT = 'default'
    APPEND = 'append'

class PathUtil:
    @staticmethod
    def get_human_readable_path(path):
        str_path = '.'.join([str("[%d]" % i if isinstance(i, int) else i) for i in path])
        str_path = str_path.replace('.[', '[')
        return str_path

    @staticmethod
    def get_match_path(path):
        str_path = '.'.join([str("[]" if isinstance(i, int) else i) for i in path])
        str_path = str_path.replace('.[', '[')
        return str_path


def date_cmp(a, b):
    # TODO : Move norm_date_time to preprocess saves about 5% processing cpu time
    return cmp(norm_date_time(a), norm_date_time(b))

def topo_token_cmp(a, b):
    token1 = TopoInfoGenerator(token=a)
    token2 = TopoInfoGenerator(token=b)
    assert len(token1.indexes) == len(token2.indexes)
    return cmp(token1.indexes, token2.indexes)



cmp_dict = {
    'time': date_cmp,
    'topo_token': topo_token_cmp
}

def get_cmp_func(t):
    global cmp_dict
    if t in cmp_dict:
        return cmp_dict[t]
    else:
        return cmp


def make_list_initial_trace(input, input_ts, input_url):
    return [(input_ts, input_url) for i in input]


def get_new_trace(input, input_ts, input_url, k):
    if type(input[k]) == list:
        return make_list_initial_trace(input[k], input_ts, input_url)
    else:
        return [input_ts, input_url]

def make_time_trace_tree(input, ts, url):
    tree = copy.deepcopy(input)
    if type(tree) != dict:
        return [ts, url]
    def walk_tree(root, ts, url):
        for k in root:
            if type(root[k]) == list:
                root[k] = [make_time_trace_tree(i, ts, url) for i in root[k]]
            elif type(root[k]) == dict:
                walk_tree(root[k], ts, url)
            else:
                root[k] = [ts, url]
    walk_tree(tree, ts, url)
    return tree


# def primirive_merge(existing, time_trace, input, input_ts, input_url, k, current_path, existing_meta,
#                    input_meta=new_page_meta, spec=None):


def default_merge(
        existing, time_trace, input, input_ts, input_url, k, current_path=None, existing_meta=None, input_meta=None, spec=None, merger=None):
    # Only list and primitive value will come into this func
    # Default merge func : new value overwrites old one

    data_changed = False
    meta_changed = False
    # existing_trace cannot be None, it should have been set to default
    existing_trace = time_trace.get(k)
    assert existing_trace is not None
    if existing[k] != input[k]:
        if isinstance(input[k], list):
            # Hack: if we meet list, overwrite directly. This is temporary solution required by jingsheng
            existing[k] = input[k]
            time_trace[k] = get_new_trace(input, input_ts, input_url, k)
            data_changed = True
            meta_changed = True
        else:
            if input_ts >= existing_trace[0]:
                # Input is newer, need overwrite
                existing[k] = input[k]
                time_trace[k] = get_new_trace(input, input_ts, input_url, k)
                data_changed = True
                meta_changed = True
    else:
        # values are the same, only change the time stamp
        if isinstance(input[k], list):
            # Hack: if we meet list, overwrite directly. This is temporary solution required by jingsheng
            time_trace[k] = get_new_trace(input, input_ts, input_url, k)
        else:
            if input_ts >= existing_trace[0]:
                time_trace[k] = get_new_trace(input, input_ts, input_url, k)
        meta_changed = True
    return data_changed  # , meta_changed


def append_merge(
        existing, time_trace, input, input_ts, input_url, k, current_path=None, existing_meta=None, input_meta=None, spec=None):
    # append requires that both items are list
    if type(input[k]) != list:
        raise Exception("input item is not list : %s, %s" % (json.dumps(input), str(k)))
    if k in existing and type(existing[k]) != list:
        raise Exception("existing item is not list : %s, %s" % (json.dumps(input), str(k)))
    existing.setdefault(k, [])
    existing[k].extend(input[k])
    time_trace[k].extend([make_time_trace_tree(i, ts=input_ts, url=input_url) for i in input[k]])


def sort_append_unqiue_merge(
        existing, time_trace, input, input_ts, input_url, k, current_path=None, existing_meta=None, input_meta=None, spec=None, merger=None):
    # append requires that both items are list
    data_changed = False
    meta_changed = False

    log.debug("appending %s" % k)

    if len(input[k]) != 0:
        data_changed = True
        log.debug("before append \ninput=\n%s\nexisting=\n%s" % (json.dumps(input[k], indent=2, separators=(',', ': ')),
                                                                 json.dumps(existing[k], indent=2,
                                                                            separators=(',', ': '))))
        append_merge(existing, time_trace, input, input_ts, input_url, k)
        log.debug("after append \nexisting=\n%s" % (json.dumps(existing[k], indent=2, separators=(',', ': '))))

    token_based = existing_meta.get(HBaseDefs.SINGLE_SRC_IS_TOPO_TOKEN_BASED, None)
    assert token_based is not None

    sort_list = zip(existing[k], time_trace[k])
    if spec.need_unique:

        def dedup_sort_func(a, b, unique_keys):
            # Here we are doing whole-value dedup, use the default cmp func is OK
            # First compare attr, then the merged times of this doc
            # In this way, all elements with same value are grouped together, with merged times in ascending order
            if type(a[0]) != dict or type(b[0]) != dict:
                res = cmp(a[0], b[0])
            else:
                if len(unique_keys) == 0:
                    res = cmp(a[0], b[0])
                else:
                    res = seq_cmp_func(a[0], b[0], unique_keys)
                if 0 == res:
                    res = cmp(a[0].get(MetaFields.DOC_MERGED_TIMES, 0), b[0].get(MetaFields.DOC_MERGED_TIMES, 0))
            return res

        def dedup_eq_func(a, b, unique_keys):
            # only dedup on real attr, not trace
            if type(a[0]) != dict or type(b[0]) != dict:
                return a[0] == b[0]
            else:
                if len(unique_keys) == 0:
                    return a[0] == b[0]
                else:
                    return seq_cmp_func(a[0], b[0], unique_keys) == 0

        if len(existing[k]) > 1:
            unique_keys = copy.deepcopy(spec.unique_keys)
            # assert type(unique_keys) == list
            # if not token_based:
            #     unique_keys = filter(lambda a : a != MetaFields.TOPO_TOKEN, unique_keys)
            if type(existing[k][0]) == dict:
                # dict is unhashable, we are down to sort_merge_dedup
                # but we cannot use this ordering for output, because dedup require full-ordering, while
                # the user may require ordering on specified attrs

                # Algorithm : sort-merge dedup, keep the latest timestamp for each value

                real_sort_func = lambda a, b : dedup_sort_func(a, b, unique_keys=unique_keys)
                real_eq_func = lambda a, b: dedup_eq_func(a, b, unique_keys=unique_keys)
            else:
                real_sort_func = lambda a, b : cmp(a[0], b[0])
                real_eq_func = lambda a, b : cmp(a[0], b[0]) == 0

            value_after_unique = [v[0] for v in sort_list]
            log.debug("before dedup \nvalue_list=\n%s" % (json.dumps(value_after_unique, indent=2, separators=(',', ': '))))
            sort_list = sort_merge_dedup(sort_list, sort_func=real_sort_func, eq_func=real_eq_func)
            value_after_unique = [v[0] for v in sort_list]
            log.debug("after dedup \nvalue_list=\n%s" % (json.dumps(value_after_unique, indent=2, separators=(',', ': '))))

            trace_after_unique = [v[1] for v in sort_list]
            existing[k] = value_after_unique
            time_trace[k] = trace_after_unique
            meta_changed = True
            data_changed = True

    if spec.need_sort:

        def sort_func(a, b, keys):
            if type(a[0]) != dict or type(b[0]) != dict:
                return cmp(a[0], b[0])
            else:
                skip_count = 0
                if len(keys) != 0:
                    res = 0
                    for key in keys:
                        name = key[0]
                        ttype = key[1]
                        if name not in a[0] and name in b[0]:
                            res = -1
                        elif name in a[0] and name not in b[0]:
                            res = 1
                        elif name not in a[0] and name not in b[0]:
                            skip_count += 1
                            continue
                        else:
                            res = 0
                        if 0 == res:
                            cmp_func = get_cmp_func(ttype)
                            res = cmp_func(a[0][name], b[0][name])
                        if res != 0:
                            break
                    if skip_count == len(keys):
                        res = cmp(a[0], b[0])
                    # if 0 == res:
                    #     res = -1
                    return res
                else:
                    return cmp(a[0], b[0])

        if len(existing[k]) > 1:
            sort_keys = copy.deepcopy(spec.sort_keys)
            # assert type(sort_keys) == list
            # if not token_based:
            #     sort_keys = filter(lambda a: a[0] != MetaFields.TOPO_TOKEN, sort_keys)
            if type(existing[k][0]) == dict:
                doc_cmp_func = lambda x, y: sort_func(x, y, sort_keys)
            else:
                doc_cmp_func = lambda a, b : cmp(a[0], b[0])
            sort_list = sorted(sort_list, doc_cmp_func)
            value_after_sort = [v[0] for v in sort_list]
            trace_after_sort = [v[1] for v in sort_list]
            existing[k] = value_after_sort
            time_trace[k] = trace_after_sort

            meta_changed = True
            data_changed = True
    return data_changed  # , meta_changed

class SingleSrcMergeRule:
    def __init__(self, single_conf):
        self.id = single_conf['id']
        self.topic_id = single_conf[SingleSrcDefs.TOPIC_ID]
        self.site = single_conf[SingleSrcDefs.SITE]
        self.path = single_conf[SingleSrcDefs.PATH].strip().split('.')
        self.content = json.loads(single_conf[SingleSrcDefs.CONTENT])
        self.type = self.content.get(SMConfAttr.TYPE, 'default')
        self.spec = None
        if self.type == 'append':
            self.spec = AppendSpec(self.content)

    @staticmethod
    def make_test_rule(site, topic, path, content):
        mock_row = {
            SingleSrcDefs.ID : 12306,
            SingleSrcDefs.TOPIC_ID : topic,
            SingleSrcDefs.SITE : site,
            SingleSrcDefs.PATH : path,
            SingleSrcDefs.CONTENT : content
        }
        return SingleSrcMergeRule(mock_row)

def find_target_subdoc(doc, key, value, path=None):
    path = [] if path is None else path
    def walk_tree(doc, key, value, path):
        if type(doc) == dict and doc.get(key) == value:
            return doc
        if type(doc) == dict:
            for k, v in doc.items():
                if type(v) in [dict, list]:
                    path.append(k)
                    found = walk_tree(v, key, value, path)
                    if found:
                        return found
                    path.pop()
        elif type(doc) == list:
            for i, v in enumerate(doc):
                if type(v) in [dict, list]:
                    path.append(i)
                    found = walk_tree(v, key, value, path)
                    if found:
                        return found
                    path.pop()
        return None
    return walk_tree(doc, key, value, path), path



class SingleSourceMergerImpl:

    initial_meta = {HBaseDefs.SINGLE_SRC_META_MERGED_TIMES: 0,
                    HBaseDefs.SINGLE_SRC_META_TOTAL_OUTPUT_TIMES: 0,
                    HBaseDefs.SINGLE_SRC_META_SEEN_LATEST_DL_TIME: 0,
                    HBaseDefs.SINGLE_SRC_META_LATEST_MERGED_TIME: 0,
                    HBaseDefs.SINGLE_SRC_ATTR_TIME_TRACE: {},
                    HBaseDefs.SINGLE_SRC_ATTR_TOPIC_META: {},
                    HBaseDefs.SINGLE_SRC_DANGLING_FRAGMENTS: [],
                    HBaseDefs.SINGLE_SRC_SEEN_TOPO_TOKENS: [],
                    HBaseDefs.SINGLE_SRC_CURRENT_UUID: "",
                    HBaseDefs.SINGLE_SRC_IS_TOPO_TOKEN_BASED: None,
                    HBaseDefs.SINGLE_SRC_SEQ: 0}


    def __init__(self, topic_manager, conf):
        self.conf = conf
        # self.conn = HBaseThrift2Connection(conf=conf)
        self.conn = DBBackendFactory.get_backend(conf)
        self.conn.set_auto_decode_json(True)
        self.topic_manager = topic_manager
        self.topic_site_rule_dict = {}
        self.output_meta_fields_exception = {'_single_src_seq'}

        self.reload_merge_rule()

        self.merge_func_dict = {
            SMMergeType.DEFAULT: default_merge,
            SMMergeType.APPEND: sort_append_unqiue_merge
        }
        existing_tables = self.conn.list_table()
        topic_tables = [table + '_single_src' for table in self.topic_manager.get_table_names()]
        for topic_table in topic_tables:
            if topic_table not in existing_tables:
                self.conn.create_table(topic_table)


    def reload_merge_rule(self):
        new_dict = {}
        # Default merge rule for list : set union
        put_to_dict_path(new_dict, [list], SingleSrcMergeRule.make_test_rule("*", "*", "*",
            '{"type":"default"}'))
        put_to_dict_path(new_dict, ['*', '*', '_src'], SingleSrcMergeRule.make_test_rule("*", "*", "_src",
            '{"type":"append", "unique_keys":["site"]}'))
        single_src_config_rows = mysql_exec_oneshot(self.conf, "select * from single_src_config")
        for record in single_src_config_rows:
            single_rule = SingleSrcMergeRule(record)
            put_to_dict_path(new_dict, [single_rule.topic_id, single_rule.site] + single_rule.path, single_rule)
        self.topic_site_rule_dict = new_dict



    def before_merge(self, doc_fragment, parse_info):
        # if '_src' not in doc_fragment:
            def get_src_json(parse_info):
                from bdp.i_crawler.i_entity_extractor.ttypes import EntitySource
                from thrift.protocol.TJSONProtocol import TSimpleJSONProtocol
                from thrift.transport.TTransport import TMemoryBuffer
                entity_source = EntitySource(url=parse_info.base_info.url, site_id=parse_info.base_info.site_id,
                                             site=parse_info.base_info.site,
                                             download_time=parse_info.crawl_info.download_time)
                t_mem_buf = TMemoryBuffer()
                t_json_protocol = TSimpleJSONProtocol(t_mem_buf)
                entity_source.write(t_json_protocol)
                return json.loads(t_mem_buf.getvalue())

            doc_fragment[global_defs.FIELDNAME_SOURCE] = [get_src_json(parse_info)]

    def process(self, download_time, site, url, doc_fragment, topic_id, plugin_merge_funcs):
        from i_util.global_defs import MetaFields
        site_record_id = doc_fragment['_site_record_id']
        rk = make_hbase_single_src_rk(site, doc_fragment, topic_id)
        table_name = self.topic_manager.get_table_name_by_id(topic_id) + "_single_src"
        latest_merged_doc, meta = self.fetch_existing_fragment(table_name, rk)
        latest_merged_doc_copy = copy.deepcopy(latest_merged_doc)

        def give_up():
            return None, latest_merged_doc_copy, rk, meta, False, table_name

        # if MetaFields.TOPO_TOKEN_RAW in doc_fragment:
        #     if doc_fragment[MetaFields.TOPO_TOKEN_RAW] in meta[HBaseDefs.SINGLE_SRC_SEEN_TOPO_TOKENS]:
        #         log.warning("This topo token has been seen before : " + doc_fragment[MetaFields.TOPO_TOKEN_RAW])
        #         return give_up()
        #     else:
        #         meta[HBaseDefs.SINGLE_SRC_SEEN_TOPO_TOKENS].append(doc_fragment[MetaFields.TOPO_TOKEN_RAW])

        do_merge = False
        token_based = False
        clear_existing = False
        new_merge = False
        merge_base = None
        merge_base_path = None
        incomming_uuid = ""
        incomming_topo_token = doc_fragment.get(MetaFields.TOPO_TOKEN, None)
        if incomming_topo_token is not None:
            token_based = True

        if meta[HBaseDefs.SINGLE_SRC_IS_TOPO_TOKEN_BASED] not in [None, token_based]:
            log.debug("token_based changed, drop old data and restart merging")
            clear_existing = True

        if not clear_existing:

            if token_based:
                topo_info = TopoInfoGenerator(token=incomming_topo_token)
                # Topo token based merge, find the base doc to merge the incomming fragment
                incomming_uuid = topo_info.get_uuid()
                if meta[HBaseDefs.SINGLE_SRC_CURRENT_UUID] == '':
                    log.debug(
                        "Process uuid = %s for the first time, site_record_id = %s" % (incomming_uuid, site_record_id))
                    new_merge = True
                    meta[HBaseDefs.SINGLE_SRC_CURRENT_UUID] = incomming_uuid
                    meta[HBaseDefs.SINGLE_SRC_IS_TOPO_TOKEN_BASED] = True
                elif incomming_uuid != meta[HBaseDefs.SINGLE_SRC_CURRENT_UUID]:
                    # We got a fragment from a doc different from the current one
                    # Start merging from beginning
                    log.debug("Got a fragment from a different crawling process of this doc, clear existing fragments. "
                              "new uuid = %s old uuid = %s, site_record_id = %s"
                              % (incomming_uuid, meta[HBaseDefs.SINGLE_SRC_CURRENT_UUID], site_record_id))
                    clear_existing = True

                if topo_info.is_base_doc():
                    merge_base, merge_base_path = latest_merged_doc, []
                else:
                    merge_base, merge_base_path = find_target_subdoc(latest_merged_doc, MetaFields.TOPO_TOKEN, incomming_topo_token)

                if merge_base is not None:
                    do_merge = True
                else:
                    # We meet a new dangling fragment that cannot be merged by now, save it
                    log.debug("Got a fragment that cannot be merged by now, save it as dangling. uuid = %s, topo token = %s, "
                              "site_record_id = %s" % (incomming_uuid, incomming_topo_token, site_record_id))
                    meta[HBaseDefs.SINGLE_SRC_DANGLING_FRAGMENTS].append((doc_fragment, download_time, url))
                    do_merge = False

            else:
                # not a topo token based fragment, merge it to the root doc
                log.debug("Got a fragment without topo token, merge it to the base doc, site_record_id = %s" % (site_record_id))
                token_based = False
                do_merge = True
                merge_base = latest_merged_doc
                merge_base_path = []
                meta[HBaseDefs.SINGLE_SRC_IS_TOPO_TOKEN_BASED] = False

        if clear_existing:
            latest_merged_doc = {}
            new_meta = copy.deepcopy(SingleSourceMergerImpl.initial_meta)
            new_meta[HBaseDefs.SINGLE_SRC_SEQ] = meta[HBaseDefs.SINGLE_SRC_SEQ]
            meta[HBaseDefs.SINGLE_SRC_IS_TOPO_TOKEN_BASED] = token_based
            meta[HBaseDefs.SINGLE_SRC_CURRENT_UUID] = incomming_uuid
            merge_base = latest_merged_doc
            merge_base_path = []
            meta = new_meta
            do_merge = True

        log.debug("Operations for incomming doc: do_merge = %s, new_merge = %s, "
                  "clear_existing = %s,  token_based = %s, merge_base_path = %s" %
                  (str(do_merge), str(new_merge), str(clear_existing), str(token_based),
                   json.dumps(PathUtil.get_human_readable_path(merge_base_path))))

        if do_merge:
            trace_base, exist = lookup_dict_path(meta[HBaseDefs.SINGLE_SRC_ATTR_TIME_TRACE], merge_base_path)
            assert exist
            if meta[HBaseDefs.SINGLE_SRC_META_SEEN_LATEST_DL_TIME] < download_time:
                meta[HBaseDefs.SINGLE_SRC_META_SEEN_LATEST_DL_TIME] = download_time
            meta[HBaseDefs.SINGLE_SRC_META_MERGED_TIMES] += 1

            new_merged_fragment, \
            merged_trace, \
            meta,\
            data_changed = \
                self.merge_fragment(site, topic_id, merge_base, trace_base,
                                    meta, doc_fragment, download_time, url,
                                    plugin_merge_funcs, base_path=merge_base_path)

            while True:
                if len(meta[HBaseDefs.SINGLE_SRC_DANGLING_FRAGMENTS]) == 0:
                    log.debug("No dangling fragment to process")
                    break
                else:
                    merge_happened = False
                    for i, (dangling, download_time_d, url_d) in enumerate(meta[HBaseDefs.SINGLE_SRC_DANGLING_FRAGMENTS]):
                        dangling_topo_token = dangling[MetaFields.TOPO_TOKEN]
                        merge_base, merge_base_path = find_target_subdoc(latest_merged_doc,
                                                                         MetaFields.TOPO_TOKEN,
                                                                         dangling_topo_token)
                        if merge_base is not None:
                            merge_happened = True
                            msg = "dangling fragment %s can be merged, path = %s" % (dangling_topo_token, json.dumps(merge_base_path))
                            log.debug(msg)
                            # No need to check uuid, because dangling uuids must be the same with current uuid, otherwise
                            # dangling fragment will be cleared
                            trace_base, exist = lookup_dict_path(meta[HBaseDefs.SINGLE_SRC_ATTR_TIME_TRACE], merge_base_path)
                            assert exist
                            new_merged_fragment, \
                            merged_trace, \
                            meta, \
                            data_changed = \
                                self.merge_fragment(site, topic_id, merge_base, trace_base,
                                                    meta, dangling, download_time_d, url_d,
                                                    plugin_merge_funcs, merge_base_path)
                            del meta[HBaseDefs.SINGLE_SRC_DANGLING_FRAGMENTS][i]
                            break
                        else:
                            log.debug("dangling fragment %s can not be merged" % dangling_topo_token)
                    if not merge_happened:
                        log.debug("No more dangling fragment to process")
                        break

            self.put_merged_fragment(table_name, rk, latest_merged_doc, meta)
            return latest_merged_doc, latest_merged_doc_copy, rk, meta, data_changed, table_name
        else:
            self.save_meta(table_name, rk, meta)
            return give_up()

    def fetch_existing_fragment(self, table, rk):
        res = self.conn.get(table, rk)
        if HBaseDefs.SINGLE_SRC_LATEST_MERGED_DOC_COL in res:
            res[HBaseDefs.SINGLE_SRC_LATEST_MERGED_DOC_COL] = json.loads(
                res[HBaseDefs.SINGLE_SRC_LATEST_MERGED_DOC_COL],
                strict=False
            )
            doc = res[HBaseDefs.SINGLE_SRC_LATEST_MERGED_DOC_COL]
        else:
            doc = {}
        if HBaseDefs.SINGLE_SRC_META_COL in res:
            res[HBaseDefs.SINGLE_SRC_META_COL] = json.loads(
                res[HBaseDefs.SINGLE_SRC_META_COL],
                strict=False
            )
            meta = res[HBaseDefs.SINGLE_SRC_META_COL]
        else:
            meta = copy.deepcopy(SingleSourceMergerImpl.initial_meta)
        return doc, meta

    def put_merged_fragment(self, table, rk, fragment, meta):
        self.conn.put_multi(table, rk,
                            {
                                HBaseDefs.SINGLE_SRC_UTIME_COL:time.strftime("%Y-%m-%d %H:%M:%S"),
                                HBaseDefs.SINGLE_SRC_SITE_COL:fragment[MetaFields.SRC][0][MetaFields.SITE],
                                HBaseDefs.SINGLE_SRC_LATEST_MERGED_DOC_COL: json.dumps(fragment),
                                HBaseDefs.SINGLE_SRC_META_COL: json.dumps(meta),
                             },
                            )

    def save_meta(self, table, rk, meta):
        self.conn.put_multi(table, rk,
                            {HBaseDefs.SINGLE_SRC_META_COL: json.dumps(meta)})

    def save_meta_2(self, key, topic_id, meta):
        table_name = self.topic_manager.get_table_name_by_id(topic_id) + "_single_src"
        self.conn.put_multi(table_name, key,
                            {HBaseDefs.SINGLE_SRC_META_COL: json.dumps(meta)})

    def put_merged_doc(self, key, doc, topic_id, meta):
        table_name = self.topic_manager.get_table_name_by_id(topic_id) + "_single_src"
        meta[HBaseDefs.SINGLE_SRC_META_LATEST_MERGED_TIME] = int(time.time())
        self.conn.put_multi(table_name, key,
                            {HBaseDefs.SINGLE_SRC_LATEST_MERGED_DOC_COL: json.dumps(doc),
                             HBaseDefs.SINGLE_SRC_META_COL: json.dumps(meta)})



    def lookup_merge_func(self, site, topic_id, path, ttype, plugin_merge_funcs):
        # 1. Lookup with specific site info
        # 2. Lookup with topic only
        # 2. Lookup with path only
        # 3. Lookup with attribute type
        # 4. Use the plugin default merge rule
        # 5. Use default merge rule : the new one overrides the old one



        str_path = PathUtil.get_match_path(path)

        if '_src' in str_path:
            a =1

        rule, exist = lookup_dict_path(self.topic_site_rule_dict, [topic_id, site] + [str_path])
        # exist is used to determine whether 'None' is explicitly put in the dict
        if rule is None:
            rule, exist = lookup_dict_path(self.topic_site_rule_dict, [topic_id, '*'] + [str_path])
        if rule is None:
            rule, exist = lookup_dict_path(self.topic_site_rule_dict, ['*', '*'] + [str_path])
        if rule is None:
            rule, exist = lookup_dict_path(self.topic_site_rule_dict, [ttype])
        if rule is None:
            ttype = SMMergeType.DEFAULT
            spec = None
        else:
            ttype = rule.type
            spec = rule.spec

        merge_func = plugin_merge_funcs.get(ttype)
        if merge_func is None:
            merge_func = self.merge_func_dict.get(ttype)
        if merge_func is None:
            raise Exception("single src merge func not registed")

        return lambda old, old_trace, input, input_ts, input_url, k, path, existing_meta : \
            merge_func(old, old_trace, input, input_ts, input_url, k, path, existing_meta, spec=spec)

    def merge_fragment(self, site, topic_id, old, old_trace, topic_meta, input, input_ts, input_url, plugin_merge_funcs, base_path):
        # New and old doc HAVE TO BE of same structure
        # Algorithm : traverse two trees synchronously and merge data along the way
        #             However, we cannot make any assumption on how each topic will use topic_meta,
        #             so no traversal logic on topic_meta

        def walk_tree(site, topic_id, old, old_trace, input, input_ts, input_url, path):
            data_changed = False
            for k in input:
                if k not in old:
                    # got a new key, copy corresponding sub tree and make a time trace subtree
                    data_changed = True
                    if type(input[k]) in [dict]:
                        # new subdoc, this node in trace tree is also a doc
                        old[k] = {}
                        old_trace[k] = {}
                    elif type(input[k]) in [list]:
                        # new list, this node in trace tree should contain:
                        # a list containing time and url for each element
                        old[k] = []
                        old_trace[k] = []
                    else:
                        # new primitive attr
                        # put a impossible value here as place holder
                        old[k] = "__THIS_IS_A_HIGHLY_UNLIKELY_VALUE__"
                        old_trace[k] = [input_ts, "https://www.fakeurl.com/shangshandalaohu?foo=bar"]
                if type(input[k]) == dict:
                    # meet a sub doc, corresponding node in old tree must also be a sub doc, so
                    # is the corresponding node in old trace, traverse them.
                    data_changed |= walk_tree(site, topic_id, old[k], old_trace[k], input[k], input_ts, input_url, path + [k])
                else:
                    # meet an attribute (primitive, list), find the configured rule and do the merging
                    merge_func = self.lookup_merge_func(site, topic_id, path + [k], type(input[k]), plugin_merge_funcs)
                    data_changed |= merge_func(old, old_trace, input, input_ts, input_url, k, path + [k], topic_meta)
            return data_changed
        data_changed = walk_tree(site, topic_id, old, old_trace, input, input_ts, input_url, base_path + [])
        old.setdefault(MetaFields.DOC_MERGED_TIMES, 0)
        old[MetaFields.DOC_MERGED_TIMES] += 1
        return old, old_trace, topic_meta, data_changed








