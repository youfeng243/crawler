# coding=utf-8
import base64
import json

from tld import get_tld

__author__ = 'fengoupeng'

import hashlib
import os
import re
import time
import urlparse
import urllib
import psutil
import itertools
import collections
import functools
import threading
from datetime import timedelta, datetime
from urllib import unquote
from HTMLParser import HTMLParser
import copy
from i_util.global_defs import MetaFields
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def multi_thread_singleton(cls, *args, **kwargs):
    instance = {}
    instance_lock = threading.Lock()
    @functools.wraps(cls)
    def _singleton(*args, **kwargs):
        if cls in instance:return instance[cls]
        with instance_lock:
            instance[cls] =cls(*args, **kwargs)
            return instance[cls]
    return _singleton

def process_url(base_url, deal_url):  # 处理url
    if deal_url is None or not isinstance(deal_url, basestring):
        return None
    if len(deal_url) == 0:
        return None
    deal_url = deal_url.strip()
    deal_url = change_ref(deal_url)
    # deal_url = deal_url.strip().split("#", 1)[0]
    # deal_url = decode_url(deal_url)
    if deal_url.startswith("http"):
        return deal_url
    elif deal_url.startswith("javascript"):
        return None
    else:
        return urlparse.urljoin(base_url, deal_url)

RAW_PATTERN = "(\\?\\\u[0-9a-zA-Z]{4})"
def change_raw(text):  # 处理unicode转义序列
    datas = re.findall(RAW_PATTERN, text)
    if len(datas) > 0:
        datas = set(datas)
        for data in datas:
            temp = data.replace("\\\\", "\\")
            if temp in [u"\\u0000", u"\\u0001", u"\\u0002", u"\\u0003", u"\\u0004"]:
                replace = ""
            else:
                replace = temp.decode("raw_unicode_escape")
            text = text.replace(data, replace)
    return text

REF_PATTERN = "(&#?[0-9a-zA-Z]{,9};)"
def change_ref(text):  # 处理网页中的转义序列
    datas = re.findall(REF_PATTERN, text)
    if len(datas) > 0:
        parser = HTMLParser()
        datas = set(datas)
        for data in datas:
            replace = parser.unescape(data)
            text = text.replace(data, replace)
    return text

UDE_PATTERN = "(%[0-9A-Za-z]{2})"
def decode_url(string):  # 处理URL编码
    if not isinstance(string, basestring) or string.find("%") == -1:
        return string
    temp = "%" + string.split("%", 1)[1]
    if re.match(UDE_PATTERN, temp) is None:
        return string
    if isinstance(string, unicode):
        temp = str(string)
    else:
        temp = string
    q = unquote(temp)
    if isinstance(q, unicode):
        return q
    p = re.compile("[0-9a-zA-Z\\+\\-\\s\p{P}]")
    l = len(p.sub("", q))
    try:
        if l % 3 == 0:
            q = q.decode("utf-8")
        elif l % 4 == 0:
            q = q.decode("gbk")
        else:
            q = q.decode("utf-8")
    except:
        try:
            q = q.decode("gbk")
        except:
            return string
    return q

def change_sql_str(string):
    return string.replace("\"", "'").replace("\\", "\\\\")

def change_sql_array(array):
    string = "["
    for item in array:
        if isinstance(item, basestring):
            string += "'" + change_sql_str(item).replace("'", "\\'") + "',"
        elif isinstance(item, int) or isinstance(item, long) or isinstance(item, float):
            string += unicode(item) + ","
        elif item is None:
            string += "NULL,"
    if string != "[":
        string = string[:-1]
    string += "]"
    return string

def get_md5(string):
    m2 = hashlib.md5()
    m2.update(string)
    return m2.hexdigest()

def get_project_path():
    project_name = "crawler"
    file_path = os.getcwdu()
    project_path = file_path[:file_path.rfind(project_name + "/") + len(project_name)]
    return project_path

def re_find_one(pattern, string):
    datas = re.findall(pattern, string)
    if len(datas) > 0:
        return datas[0]
    else:
        return None

def xpath_find_one(node, xpath_str):
    arr = node.xpath(xpath_str)
    if len(arr) > 0:
        result = arr[0].strip()
    else:
        result = None
    return result

def xpath_find_all(node, xpath_str):
    arr = node.xpath(xpath_str)
    if len(arr) > 0:
        result = "".join(arr).strip()
    else:
        result = None
    return result

def get_now_time():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def get_today():
    return time.strftime("%Y-%m-%d")

def get_date():
    return time.strftime("%Y%m%d")

def get_date_bias(count, day=None, formater='%Y%m%d'):
    if not day:
        day = time.strftime(formater)
    dateNow = datetime.strptime(day, formater)
    return (dateNow - timedelta(days=count)).strftime(formater)

def str_obj(obj, encoding='utf-8'):
    if isinstance(obj, unicode):
        return obj.encode(encoding)
    return obj

def unicode_obj(obj, encoding='utf-8'):
    if isinstance(obj, str):
        return obj.decode(encoding)
    return obj

def deal_json_content(content):
    json_start_index = content.find('{')
    list_start_index = content.find('[')
    if list_start_index > -1 and (json_start_index == -1 or list_start_index < json_start_index):
        list_finish_index = content.rfind(']')
        if list_finish_index > list_start_index:
            json_content = content[list_start_index : list_finish_index + 1]
            return json_content
    elif json_start_index > -1 and (list_start_index == -1 or json_start_index < list_start_index):
        json_finish_index = content.rfind('}')
        if json_finish_index > json_start_index:
            json_content = content[json_start_index : json_finish_index + 1]
            return json_content
    return None

def get_json_value(json_obj, path):
    array = path.split('.')
    obj = json_obj
    for key in array:
        if not isinstance(obj, dict):
            return None
        tmp = obj.get(key)
        if tmp is None:
            return None
        obj = tmp
    return obj

def timestampToTimestr(timestamp, format='%Y-%m-%d %H:%M:%S'):
    return time.strftime(format, time.localtime(timestamp))

# 返回日期数组(由小到大)
def get_date_array(start_date, finish_date, format='%Y-%m-%d'):
    if start_date > finish_date:
        date = start_date
        start_date = finish_date
        finish_date = date
    dates = [start_date]
    if start_date != finish_date:
        date = start_date
        while date < finish_date:
            date = get_date_bias(-1, date, format)
            dates.append(date)
    return dates

# 返回月份组(由大到小)
def get_month_array(start_month, finish_month):
    if start_month < finish_month:
        month = start_month
        start_month = finish_month
        finish_month = month
    months = [start_month]
    month = start_month
    while month > finish_month:
        year = int(month[0:4])
        month = int(month[4:])
        month -= 1
        if month == 0:
            year -= 1
            month = 12
        month = str(month)
        if len(month) == 1:
            month = '0' + month
        month = str(year) + month
        months.append(month)
    return months

def deal_with_spend(spend):
    spend = int(spend)
    second = spend % 60
    spend = spend / 60
    if not spend:
        return '%d秒' % second
    minute = spend % 60
    spend = spend / 60
    if not spend:
        return '%d分%d秒' % (minute, second)
    hour = spend % 24
    spend = spend / 24
    if not spend:
        return '%d时%d分%d秒' % (hour, minute, second)
    else:
        return '%d天%d时%d分%d秒' % (spend, hour, minute, second)


def params_str_to_json(params_str):
    result = {}
    for params in params_str.split('&'):
        param, value = params.split('=')
        result[param] = urllib.unquote(value)
    return result

import ctypes
def get_md5_i64(obj):
    m = hashlib.md5()
    m.update(obj)
    return ctypes.c_int64(int(m.hexdigest()[8:-8], 16)).value



def unique(list_, key=lambda x: x):
    """efficient function to uniquify a list preserving item order"""
    seen = set()
    result = []
    for item in list_:
        seenkey = key(item)
        if seenkey in seen:
            continue
        seen.add(seenkey)
        result.append(item)
    return result

def get_url_info(base_url):
    url_split = urlparse.urlsplit(base_url)
    url_info = {}
    url_info['site'] = url_split.netloc
    url_info['site'] = url_info['site'].split(':')[0]
    url_info['site_id'] = get_md5_i64(url_info['site'])
    url_info['path'] = url_split.path
    url_info['query'] = url_split.query
    url_info['fragment'] = url_split.fragment
    try:
        url_info['domain'] = get_tld(base_url)
    except Exception as e:
        url_info['domain'] = url_info['site']
    if url_info.get('domain'):
            url_info['domain_id'] = get_md5_i64(url_info['domain'])
    else:
        url_info['domain_id'] = None

    url_info['url'] = base_url
    url_info['url_id'] = get_md5_i64(base_url)
    return url_info

def make_hbase_single_src_rk(site_id, doc_fragment, topic_id):

    from common.singleton_holder import singletons
    from global_defs import SITE_RECORD_ID
    # FUCK THIS
    # company = doc_fragment['company'].strip().decode('utf-8').encode('utf-8')
    # if company == u'后藏青稞酒茶馆':
    #     a=1
    # print company
    site_record_id = doc_fragment['_site_record_id']
    rk = get_md5(site_id) + site_record_id
    if '__testing__' in singletons:
        singletons['ids'].add(site_record_id)
        singletons['rks'].add(rk)
        # singletons['companies'].add(company)

    return rk

def url_query_decode(string):
    query_info = {}
    for x in string.split('&'):
        if len(x) <= 0:
            continue
        kk = x.split('=')
        if len(kk) == 1:
            query_info[kk[0]] = ""
        elif len(kk) > 1:
            query_info[kk[0]] = urllib.unquote("=".join(kk[1:]))
    return query_info

def decode_content(string, charset=None):
    if not string or isinstance(string, unicode):
        return string, None
    if charset:
        if charset.lower() in ['gb2312', 'gbk', 'gb18030']:
            try:
                return string.decode('gb18030'), 'gb18030'
            except:
                pass
        return string.decode(charset, 'ignore'), charset
    for try_charset in ['utf-8', 'gb18030', 'gbk', 'utf-16']:
        try:
            return string.decode(try_charset), try_charset
        except Exception as e:
            pass
    return string, None


def base64_encode_json(obj):
    order_data = collections.OrderedDict()
    for k in sorted(obj.keys()):
        order_data[k] = obj[k]
    return base64.encodestring(json.dumps(order_data))

def base64_decode_json(string):
    return json.loads(base64.b64decode(string))

def url_encode(url):
    if isinstance(url, unicode):
        url = url.encode('utf-8')
    url = url.replace('\t','').replace('\n','').replace('\r','')
    url = urllib.quote(url, '!:?=/&%')
    return url

def is_index_url(url):
    prefix_list = ['/index', '/default']
    affix_list = ['.htm', '.html', '.shtml', '.stm', '.shtm', '.asp', '.aspx', '.php', '.jsp']
    index_list = [x + y for x, y in itertools.product(prefix_list, affix_list)]
    index_list.append('/')
    index_list.append('/index')
    path = urlparse.urlparse(url).path
    result = 1 if path.lower() in index_list else 0
    return True if result and ('?' not in url) else False

def build_hzpost_url(base_url, postdata):
    url_info = get_url_info(base_url)
    query_info = url_query_decode(url_info.get('query'))
    query_info['HZPOST'] = base64_encode_json(postdata)
    hz_url = base_url.split("?")[0] +'?' + urllib.urlencode(query_info)
    return hz_url

def extract_hzpost_url(base_url):
    url_info = get_url_info(base_url)
    query_info = url_query_decode(url_info.get('query'))
    postdata = None
    if isinstance(query_info, dict) and query_info.get('HZPOST'):
        hz_post = query_info.get('HZPOST')
        postdata = base64_decode_json(hz_post)
        del query_info['HZPOST']
        if len(query_info) > 0:
            base_url = base_url.split("?")[0] + '?' + urllib.urlencode(query_info)
        else:
            base_url = base_url.split("?")[0]
    return {"url":base_url, 'postdata':postdata}


crawler_basic_path = os.path.join(os.path.dirname(__file__), '../')

def put_to_dict_path(d, path, obj):
    assert type(d) == dict
    assert type(path) == list
    for item in path[:-1]:
        d.setdefault(item, {})
        d = d[item]
    d[path[-1]] = obj

# def lookup_dict_path(d, path):
#     assert type(d) == dict
#     assert type(path) == list
#     if len(path) == 0:
#         return d, True
#     for item in path[:-1]:
#         d = d.get(item, None)
#         if d is None:
#             return None, False
#     exist = path[-1] in d
#     val = d.get(path[-1], None)
#     # exist is used to determine whether 'None' is explicitly put in the dict
#     return val, exist

def lookup_dict_path(d, path):
    assert type(d) == dict
    assert type(path) == list
    if len(path) == 0:
        return d, True
    for item in path[:-1]:
        if type(d) == dict:
            d = d.get(item, None)
            if d is None:
                return None, False
        elif type(d) == list:
            if type(item) != int:
                raise Exception("Unexpected key type when traversing path : " + str(type(item)))
            if item >= len(d):
                return None, False
            d = d[item]
        else:
            raise Exception("Unexpected container type when traversing path : " + str(type(d)))
    if type(d) == dict:
        exist = path[-1] in d
        val = d.get(path[-1], None)
    elif type(d) == list:
        if type(path[-1]) != int:
            raise Exception("Unexpected key type when traversing path : " + str(type(path[-1])))
        exist = path[-1] < len(d)
        val = d[path[-1]] if path[-1] < len(d) else None
    else:
        raise Exception("Unexpected container type when traversing path : " + str(type(d)))
    # exist is used to determine whether 'None' is explicitly put in the dict
    return val, exist


def sort_merge_dedup(l, sort_func = cmp, eq_func = lambda a, b : a == b, group_size=1):
    if len(l) <= 1:
        return l
    else:
        l = sorted(l, cmp=sort_func)
        res = []
        group = []
        for i in l:
            if len(group) == 0 or not eq_func(i, group[0]):
                res.extend(group[:group_size])
                group = [i]
            else:
                if eq_func(i, group[0]):
                    if len(group) < group_size:
                        group.append(i)
                    else:
                        pass
        if len(group) > 0:
            res.extend(group[:group_size])
        return res


def seq_cmp_func(a, b, keys):
    if len(keys) == 0:
        return cmp(a, b)
    res = 0
    skip_count = 0
    for key in keys:
        if key not in a and key not in b:
            skip_count += 1
            continue
        res = cmp(a.get(key, None), b.get(key, None))
        if res != 0:
            break
    if skip_count == len(keys):
        res = cmp(a, b)
    return res

class TopoInfoGenerator:
    def __init__(self, tag=None, dims=None, token=None):
        self.match_token = None
        if token is not None:
            elements = token.split('.')
            self.uuid = elements[0]
            self.tag = None
            self.dims = []
            self.indexes = []
            if len(elements) > 1:
                self.tag = elements[1]
                if len(elements) > 2:
                    index_elements = [[int(i) for i in e.split('/')] for e in elements[2].split('|')]
                    self.indexes = [i[0] for i in index_elements]
                    if len(index_elements) > 0 and len(index_elements[0]) > 1:
                        self.dims = [i[1] for i in index_elements]
        else:
            import uuid
            self.uuid = str(uuid.uuid4())
            self.tag = tag
            self.dims = dims if dims is not None else []
            self.indexes = [0 for i in self.dims]

    def add_dim(self):
        self.dims.append(0)
        self.indexes.append(0)

    def pop_dim(self):
        self.dims.pop()
        self.indexes.pop()

    def set_tag(self, tag):
        self.tag = tag

    def set_index_dim(self, i, index, dim):
        self.indexes[i] = index
        self.dims[i] = dim

    def get_token(self):
        dim_indexes = '|'.join( ['%d/%d' % i for i in zip(self.indexes, self.dims)])
        elements = [self.uuid]
        if self.tag is not None:
            elements.append(self.tag)
        if dim_indexes != "":
            elements.append(dim_indexes)
        return '.'.join(elements)

    def get_match_token(self):
        if self.match_token is None:
            dim_indexes = '|'.join([ str(i) for i in self.indexes])
            elements = [self.uuid]
            if self.tag is not None:
                elements.append(self.tag)
            if dim_indexes != "":
                elements.append(dim_indexes)
            return '.'.join(elements)
        return self.match_token

    def get_uuid(self):
        return self.uuid

    def is_base_doc(self):
        return self.tag is None or len(self.indexes) < 2

    def get_tag(self):
        return self.tag

    def get_cur_dim(self):
        return len(self.dims)


def get_metaless_doc(doc, exception=MetaFields.SINGLE_SRC_MERGER_ALLOW_OUTPUT):
    output_doc = copy.deepcopy(doc)

    def walk_tree(doc):
        keys = doc.keys()
        for k in keys:
            if isinstance(k, basestring) and k.startswith("_"):
                if k not in exception:
                    doc.pop(k)
            elif type(doc[k]) == dict:
                walk_tree(doc[k])
            elif type(doc[k]) == list:
                for i in doc[k]:
                    if type(i) == dict:
                        walk_tree(i)

    walk_tree(output_doc)
    return output_doc

time_kv = {'一': '1', '二': '2', '三': '3', '四': '4', '五': '5', '六': '6', '七': '7',
           '八': '8', '九': '9', '十': '10', '零': '0', '〇': '0', 'O': '0','○':'0','年': '-', '月': '-', '日': ' ',
           '时': ':', '点': ':', '分': '', '.': '-', '/': '-'}


def norm_date(bulletin_date):
    '''格式化日期'''
    src_date_value = bulletin_date
    for k, v in time_kv.items():
        bulletin_date = bulletin_date.replace(k, v)
    fields = bulletin_date.split('-')
    if len(fields) != 3:
        return src_date_value
    year = fields[0].strip()
    if len(year) != 4:
        return src_date_value
    month = fields[1].strip()
    if len(month) < 2:
        month = '0' + month
    elif len(month) > 2:
        month = month[0] + month[2]
    day = fields[2].strip()
    if len(day) < 2:
        day = '0' + day
    elif len(day) == 3:
        day = day[0] + day[2]
    elif len(day) == 4:
        day = day[0] + day[3]
    bulletin_date = year + '-' + month + '-' + day
    return bulletin_date

def norm_time(time_str):
    '''格式化时间戳'''
    time_str = time_str.replace(u'上午', '').replace(u'下午', '').replace(u'整', '')
    for k, v in time_kv.items():
        time_str = time_str.replace(k, v)
    fields = time_str.split(':')
    size = len(fields)
    hour = '00'
    minute = '00'
    sec = '00'
    if 1 <= size <= 3:
        hour = fields[0].strip()
        if len(hour) == 1:
            hour = '0' + hour
        elif len(hour) == 3:
            hour = hour[0] + hour[2]

    if 2 <= size <= 3:

        minute = fields[1].strip()
        if len(minute) <= 0:
            minute = '00'
        elif len(minute) == 1:
            minute = '0' + minute
        elif len(minute) == 3:
            minute = minute[0] + minute[2]
    if size == 3:
        sec = fields[2].strip()
        if len(sec) <= 0:
            sec = '00'
        elif len(sec) == 1:
            sec = '0' + sec
        elif len(sec) == 3:
            sec = sec[0] + sec[2]

    time_str = hour + ':' + minute + ':' + sec
    return time_str

def norm_date_time(date_time):
    '''格式化日期和时间戳'''
    if not date_time:
        return date_time
    for k, v in time_kv.items():
        date_time = date_time.replace(k, v)

    date_list = date_time.split()

    if len(date_list) == 1:
        date_value = norm_date(date_time)
        if not date_value:
            return date_time
        return date_value + " 00:00:00"
    elif len(date_list) == 2:
        dates = norm_date(date_list[0])
        times = norm_time(date_list[1])
        return dates + ' ' + times
    else:
        return date_time

def make_pid_file(pid_dir_path):
    try:
        os.makedirs(pid_dir_path)
    except OSError, e:
        if e.errno != 17:
            raise
    pid_path = os.path.join(pid_dir_path, 'server.pid')
    if os.path.exists(pid_path):
        pid = None
        with open(pid_path) as fp:
            pid = fp.read()
        if pid:
            pid = int(pid)
            if psutil.pid_exists(pid):
                sys.stderr.write("process running\n")
                sys.stderr.flush()
                os._exit(0)
    with open(pid_path, 'w') as pfile:
        pfile.write(str(os.getpid()))
    return pid_path

def remove_pid_file(pid_dir_path):
    pid_path = os.path.join(pid_dir_path, 'server.pid')
    if os.path.exists(pid_path):
        os.remove(pid_path)

def process_brackets(original):
    return original.replace('(', '（').replace(')', '）')

def get_record_id_new(pks, url, entity_data):
    key_parts = []

    if isinstance(pks, list):
        if len(pks) > 0:
            for key_path in pks:
                if len(key_path) == 0:
                    raise Exception("Meet empty key path")

                key_col, exist = lookup_dict_path(entity_data, key_path)
                if not exist:
                    raise Exception("Primary key not complete, lacking : " + '.'.join(key_path))
                else:
                    # do a little trick here: convert all english brackets to chinese when calculating record_id
                    # but donot affect raw data here
                    field_name = str(key_path[-1]).strip()
                    if field_name.find('company') != -1 or field_name.find('compay') != -1:
                        key_parts.append(process_brackets(key_col))
                    else:
                        key_parts.append(key_col)
        else:
            if MetaFields.SITE_RECORD_ID in entity_data:
                key_parts.append(entity_data[MetaFields.SRC][0]['site'])
                key_parts.append(entity_data[MetaFields.SITE_RECORD_ID])
            else:
                key_parts.append(url)
    else:
        raise Exception("invalid primary key type : " + str(type(pks)))
    return get_md5("|" + "|".join(map(str, key_parts)))




def normalize_utf8_encoding(doc):
    output_doc = copy.deepcopy(doc)

    def walk_tree(doc):
        keys = doc.keys()
        for k in keys:
            if isinstance(doc[k], unicode):
               doc[k] = doc[k].encode("utf-8")
            elif type(doc[k]) == dict:
                walk_tree(doc[k])
            elif type(doc[k]) == list:
                for i in doc[k]:
                    if type(i) == dict:
                        walk_tree(i)

    walk_tree(output_doc)
    return output_doc