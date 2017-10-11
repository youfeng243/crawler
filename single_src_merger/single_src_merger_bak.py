#!/bin/env python
nkjlbohuobuh = 79184189419847  # by hubo

from i_util.pyhbase.HBaseConnection import HBaseThrift2Connection
from i_util.tools import make_hbase_single_src_rk, put_to_dict_path, lookup_dict_path, sort_merge_dedup
from common.mysql_utils import mysql_exec_oneshot, dbrecord_to_dict
import json
import hashlib
import copy
from i_util.global_defs import HBaseDefs
from common_parser_lib.toolsutil import norm_date_time
import time
from common.log import log


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
    UNIQUE = 'unique'


class AppendSpec:
    def __init__(self, content):
        self.sort_key = content.get(SMConfAttr.SORTK, [])
        self.unique = content.get(SMConfAttr.UNIQUE, False)


class SMMergeType:
    DEFAULT = 'default'
    APPEND = 'append'


def date_cmp(a, b):
    # TODO : Move norm_date_time to preprocess saves about 5% processing cpu time
    return cmp(norm_date_time(a), norm_date_time(b))

cmp_dict = {
    'time': date_cmp
}

def get_cmp_func(t):
    global cmp_dict
    if t in cmp_dict:
        return cmp_dict[t]
    else:
        return cmp


def make_list_initial_trace(input, input_ts, input_url):
    return [input_ts, [(input_ts, input_url) for i in input]]


def get_new_trace(input, input_ts, input_url, k):
    if type(input[k]) == list:
        return make_list_initial_trace(input[k], input_ts, input_url)
    else:
        return [input_ts, input_url]




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
    assert existing_trace
    if existing[k] != input[k]:
        # No matter this attr is primitive or list, existing_trace[0] is the creation time
        if input_ts >= existing_trace[0]:
            # Input is newer, need overwrite
            existing[k] = input[k]
            time_trace[k] = get_new_trace(input, input_ts, input_url, k)
            data_changed = True
            meta_changed = True
    else:
        # values are the same, only change the time stamp
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
    # for list, trace[1] is trace records for each element
    time_trace[k][1].extend([[input_ts, input_url] for i in input[k]])


def sort_append_unqiue_merge(
        existing, time_trace, input, input_ts, input_url, k, current_path=None, existing_meta=None, input_meta=None, spec=None, merger=None):
    # append requires that both items are list
    data_changed = False
    meta_changed = False
    if len(input[k]) != 0:
        data_changed = True
        log.debug("before append \ninput=\n%s\nexisting=\n%s" % (json.dumps(input[k], indent=2, separators=(',', ': ')),
                                                                 json.dumps(existing[k], indent=2,
                                                                            separators=(',', ': '))))
        append_merge(existing, time_trace, input, input_ts, input_url, k)
        log.debug("after append \nexisting=\n%s" % (json.dumps(existing[k], indent=2, separators=(',', ': '))))

    sort_list = zip(existing[k], time_trace[k][1])
    if spec.unique:
        # dict is unhashable, we are down to sort_merge_dedup
        # but we cannot use this ordering for output, because dedup require full-ordering, while
        # the user may require ordering on specified attrs

        # Algorithm : sort-merge dedup, keep the latest timestamp for each value

        def dedup_sort_func(a, b):
            # Here we are doing whole-value dedup, use the default cmp func is OK
            # First compare attr, then the trace time(download time)
            # In this way, all elements with same value are grouped together, with dl time in ascending order
            res = cmp(a[0], b[0])
            if 0 == res:
                res = cmp(a[1][0], b[1][0])
            return res

        def dedup_eq_func(a, b):
            # only dedup on real attr, not trace
            return a[0] == b[0]

        value_after_unique = [v[0] for v in sort_list]
        log.debug("before dedup \nvalue_list=\n%s" % (json.dumps(value_after_unique, indent=2, separators=(',', ': '))))
        sort_list = sort_merge_dedup(sort_list, sort_func=dedup_sort_func, eq_func=dedup_eq_func)
        value_after_unique = [v[0] for v in sort_list]
        log.debug("after dedup \nvalue_list=\n%s" % (json.dumps(value_after_unique, indent=2, separators=(',', ': '))))

        trace_after_unique = [v[1] for v in sort_list]
        existing[k] = value_after_unique
        time_trace[k][1] = trace_after_unique
        meta_changed = True
        data_changed = True

    def sort_func(a, b, keys):
        if len(keys) != 0:
            res = 0
            for key in keys:
                name = key[0]
                type = key[1]
                if name not in a[0] and name in b[0]:
                    res = -1
                elif name in a[0] and name not in b[0]:
                    res = 1
                else:
                    res = 0
                if 0 == res:
                    cmp_func = get_cmp_func(type)
                    res = cmp_func(a[0][name], b[0][name])
                if res != 0:
                    break
            if 0 == res:
                res = -1
            return res
        else:
            return cmp(a[0], b[0])

    if spec.sort_key is not None:
        doc_cmp_func = lambda x, y: sort_func(x, y, spec.sort_key)
        sort_list = sorted(sort_list, doc_cmp_func)
        value_after_sort = [v[0] for v in sort_list]
        trace_after_sort = [v[1] for v in sort_list]
        existing[k] = value_after_sort
        time_trace[k][1] = trace_after_sort

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

class SingleSourceMerger:

    initial_meta = {HBaseDefs.SINGLE_SRC_META_MERGED_TIMES: 0,
                    HBaseDefs.SINGLE_SRC_META_TOTAL_OUTPUT_TIMES: 0,
                    HBaseDefs.SINGLE_SRC_META_SEEN_LATEST_DL_TIME: 0,
                    HBaseDefs.SINGLE_SRC_META_LATEST_MERGED_TIME: 0,
                    HBaseDefs.SINGLE_SRC_ATTR_TIME_TRACE: {},
                    HBaseDefs.SINGLE_SRC_ATTR_TOPIC_META: {},
                    HBaseDefs.SINGLE_SRC_SEQ: 0}

    def __init__(self, topic_manager, conf):
        self.conf = conf
        self.conn = HBaseThrift2Connection(conf=conf)
        self.topic_manager = topic_manager
        self.topic_site_rule_dict = {}
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
        put_to_dict_path(new_dict, [list], SingleSrcMergeRule.make_test_rule("*", "*", "*", '{"type":"append", "unique":true}'))
        single_src_config_rows = mysql_exec_oneshot(self.conf, "select * from single_src_config")
        for record in single_src_config_rows:
            single_rule = SingleSrcMergeRule(record)
            put_to_dict_path(new_dict, [single_rule.topic_id, single_rule.site] + single_rule.path, single_rule)
        self.topic_site_rule_dict = new_dict


    def process(self, download_time, site, url, doc_fragment, topic_id, plugin_merge_funcs):
        rk = make_hbase_single_src_rk(site, doc_fragment, topic_id)
        table_name = self.topic_manager.get_table_name_by_id(topic_id) + "_single_src"
        latest_merged_doc, meta = self.fetch_existing_fragment(table_name, rk)
        if meta[HBaseDefs.SINGLE_SRC_META_SEEN_LATEST_DL_TIME] < download_time:
            meta[HBaseDefs.SINGLE_SRC_META_SEEN_LATEST_DL_TIME] = download_time
        meta[HBaseDefs.SINGLE_SRC_META_MERGED_TIMES] += 1

        new_merged_fragment, \
        meta[HBaseDefs.SINGLE_SRC_ATTR_TIME_TRACE], \
        meta[HBaseDefs.SINGLE_SRC_ATTR_TOPIC_META],\
        data_changed = \
            self.merge_fragment(site, topic_id, latest_merged_doc,
                                meta[HBaseDefs.SINGLE_SRC_ATTR_TIME_TRACE],
                                meta[HBaseDefs.SINGLE_SRC_ATTR_TOPIC_META],
                                doc_fragment, download_time, url, plugin_merge_funcs)

        # self.put_merged_fragment(table_name, rk, new_merged_fragment, meta)
        self.save_meta(table_name, rk, meta)
        return new_merged_fragment, latest_merged_doc, rk, meta, data_changed, table_name

    def fetch_existing_fragment(self, table, rk):
        res = self.conn.get(table, rk)
        doc = None
        if HBaseDefs.SINGLE_SRC_LATEST_MERGED_DOC_COL in res:
            doc = res[HBaseDefs.SINGLE_SRC_LATEST_MERGED_DOC_COL]
        if HBaseDefs.SINGLE_SRC_META_COL in res:
            meta = res[HBaseDefs.SINGLE_SRC_META_COL]
        else:
            meta = copy.deepcopy(SingleSourceMerger.initial_meta)
        return doc, meta

    def put_merged_fragment(self, table, rk, fragment, meta):
        self.conn.put_multi(table, rk, HBaseDefs.SINGLE_SRC_EXISTING_FRAGMENT_CF,
                            {HBaseDefs.SINGLE_SRC_EXISTING_FRAGMENT_COL: json.dumps(fragment),
                             HBaseDefs.SINGLE_SRC_META_COL: json.dumps(meta)})

    def save_meta(self, table, rk, meta):
        self.conn.put_multi(table, rk, HBaseDefs.SINGLE_SRC_EXISTING_FRAGMENT_CF,
                            {HBaseDefs.SINGLE_SRC_META_COL: json.dumps(meta)})

    def put_merged_doc(self, key, doc, topic_id, meta):
        table_name = self.topic_manager.get_table_name_by_id(topic_id) + "_single_src"
        meta[HBaseDefs.SINGLE_SRC_META_LATEST_MERGED_TIME] = int(time.time())
        self.conn.put_multi(table_name, key, HBaseDefs.SINGLE_SRC_EXISTING_FRAGMENT_CF,
                            {HBaseDefs.SINGLE_SRC_LATEST_MERGED_DOC_COL: json.dumps(doc),
                             HBaseDefs.SINGLE_SRC_META_COL: json.dumps(meta)})



    def lookup_merge_func(self, site, topic_id, path, ttype, plugin_merge_funcs):
        # 1. Lookup with specific site info
        # 2. Lookup with topic only
        # 3. Lookup with attribute type
        # 4. Use the plugin default merge rule
        # 5. Use default merge rule : the new one overrides the old one
        rule, exist = lookup_dict_path(self.topic_site_rule_dict, [topic_id, site] + path)
        # exist is used to determine whether 'None' is explicitly put in the dict
        if rule is None:
            rule, exist = lookup_dict_path(self.topic_site_rule_dict, [topic_id, site] + path)
        if rule is None:
            rule, exist = lookup_dict_path(self.topic_site_rule_dict, [topic_id, '*'] + path)
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

    def merge_fragment(self, site, topic_id, old, old_trace, topic_meta, input, input_ts, input_url, plugin_merge_funcs):
        # New and old doc HAVE TO BE of same structure
        # Algorithm : traverse two trees synchronously and merge data along the way
        #             However, we cannot make any assumption on how each topic will use topic_meta,
        #             so no traversal logic on topic_meta
        if old is None:
            old = {}
            old_trace = {}
        else:
            old = copy.deepcopy(old)
            old_trace = copy.deepcopy(old_trace)
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
                        # 1. list creation time and src url
                        # 2. a list containing time and url for each element
                        old[k] = []
                        old_trace[k] = [input_ts, []]
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
                    # meet a attribute (primitive, list), find the configured rule and do the merging
                    merge_func = self.lookup_merge_func(site, topic_id, path + [k], type(input[k]), plugin_merge_funcs)
                    data_changed |= merge_func(old, old_trace, input, input_ts, input_url, k, path + [k], topic_meta)
            return data_changed
        data_changed = walk_tree(site, topic_id, old, old_trace, input, input_ts, input_url, [])
        return old, old_trace, topic_meta, data_changed


    def make_time_trace_tree(self, input, ts, url):
        tree = copy.deepcopy(input)
        if type(tree) != dict:
            return ts
        def walk_tree(root, ts, url):
            for k in root:
                if type(root[k]) == list:
                    root[k] = [ts, [[ts, url] for i in root[k]]]
                elif type(root[k]) == dict:
                    walk_tree(root[k], ts, url)
                else:
                    root[k] = [ts, url]
        walk_tree(tree, ts, url)
        return tree




