# coding=utf-8

import sys

reload(sys)
sys.setdefaultencoding('utf8')
import hashlib
import random
import re
import time
import urlparse

from datetime import timedelta, datetime, date
from urllib import unquote
from HTMLParser import HTMLParser
import pandas


def utf8_decode(string):
    return string.decode('utf8', 'ignore')


def utf8_encode(string):
    return string.decode('utf8', 'ignore').encode('utf8', 'ignore')


def get_cur_time():
    time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    return time_str


def get_cur_date():
    time_str = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    return time_str


def get_yes_date():
    today = date.today()
    yesterday = today - timedelta(days=1)
    yesterday = str(yesterday)
    return yesterday


def gen_primary_key(values):
    value = '\t'.join(values)
    md5_code = hashlib.md5(value).hexdigest().upper()
    return md5_code


def gen_site_id(url):
    url = url.replace('https//', '').replace('http://', '')
    idx = url.find('/')
    site = url
    if idx != -1:
        site = url[:idx]
    return site


def my_lstrip(content, strip_list):
    while True:
        found = False
        for word in strip_list:
            if content.startswith(word):
                content = content.replace(word, '')
                found = True
        if not found:
            break
    return content


def my_rstrip(content, strip_list):
    while True:
        found = False
        for word in strip_list:
            if content.endswith(word):
                idx = content.rfind(word)
                if idx >= 0:
                    content = content[:idx]
                found = True
        if not found:
            break
    return content


def my_strip(content, strip_list):
    content = my_lstrip(content, strip_list)
    content = my_rstrip(content, strip_list)
    return content


def my_split(content, seps):
    content = utf8_encode(content)
    for sep in seps:
        sep = utf8_encode(sep)
        content = content.replace(sep, 'special_char')
    tokens = content.split('special_char')

    tokens = [x for x in tokens if x]
    return tokens


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


REF_PATTERN = '(&#?[0-9a-zA-Z]{,9};)'


def change_ref(text):  # 处理网页中的转义序列
    is_unicode = False
    if isinstance(text, unicode):
        text = text.encode('utf-8')
        is_unicode = True
    datas = re.findall(REF_PATTERN, text)
    while len(datas) > 0:
        parser = HTMLParser()
        datas = set(datas)
        for data in datas:
            replace = parser.unescape(data).encode('utf-8')
            text = text.replace(data, replace)
        datas = re.findall(REF_PATTERN, text)
    if is_unicode:
        text = text.decode('utf-8')
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


def get_md5(string):
    m2 = hashlib.md5()
    if isinstance(string, unicode):
        string = string.encode('utf-8')
    m2.update(string)
    return m2.hexdigest()


def re_find_one(pattern, string):
    datas = re.findall(pattern, string)
    if len(datas) > 0:
        return datas[0]
    else:
        return None

def re_findone(regex, string):
    datas = regex.findall(string)
    if len(datas) > 0:
        return datas[0]
    else:
        return None


def re_find_all(pattern, string):
    datas = re.findall(pattern, string)
    if len(datas) > 0:
        return datas
    else:
        return None

def re_findall(regex, string):
    datas = regex.findall(string)
    if len(datas) > 0:
        return datas
    else:
        return None



def get_now_time():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def get_date_bias(count, day=None, formater='%Y%m%d'):
    if not day:
        day = time.strftime(formater)
    date_now = datetime.strptime(day, formater)
    return (date_now - timedelta(days=count)).strftime(formater)


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
            json_content = content[list_start_index: list_finish_index + 1]
            return json_content
    elif json_start_index > -1 and (list_start_index == -1 or json_start_index < list_start_index):
        json_finish_index = content.rfind('}')
        if json_finish_index > json_start_index:
            json_content = content[json_start_index: json_finish_index + 1]
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


def timestamp_to_timestr(timestamp, formater='%Y-%m-%d %H:%M:%S'):
    return time.strftime(formater, time.localtime(timestamp))


# 返回日期数组(由小到大)
def get_date_array(start_date, finish_date, formater='%Y-%m-%d'):
    if start_date > finish_date:
        date = start_date
        start_date = finish_date
        finish_date = date
    dates = [start_date]
    if start_date != finish_date:
        date = start_date
        while date < finish_date:
            date = get_date_bias(-1, date, formater)
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
    spend /= 60
    if not spend:
        return '%d秒' % second
    minute = spend % 60
    spend /= 60
    if not spend:
        return '%d分%d秒' % (minute, second)
    hour = spend % 24
    spend /= 24
    if not spend:
        return '%d时%d分%d秒' % (hour, minute, second)
    else:
        return '%d天%d时%d分%d秒' % (spend, hour, minute, second)


def random_numstr(length):
    res = ''
    for i in range(length):
        res += str(random.randint(0, 9))
    return res


def random_date(start, end, formater='%Y-%m-%d'):
    if start == end:
        return start
    start_date = time.strptime(start, formater)
    start_time = time.mktime(start_date)
    end_date = time.strptime(end, formater)
    end_time = time.mktime(end_date)
    diff = int((end_time - start_time) / (24 * 60 * 60))
    if diff == 0:
        return start
    count = random.randint(0, diff)
    return get_date_bias(-count, start, formater)


def random_array_prob(array):
    prob = random.random()
    for i in range(len(array)):
        val = array[i][0]
        pro = array[i][1]
        if pro >= prob:
            if isinstance(val, list):
                val = random.choice(val)
            return val
        else:
            prob -= pro
    val = array[-1][0]
    if isinstance(val, list):
        val = random.choice(val)
    return val


def change_array_to_str(array):
    string = '['
    for item in array:
        if not item:
            item = str(item)
        string += item + ', '
    if string.endswith(', '):
        string = string[:-2]
    string += ']'
    return string




special_time_kv = {u'十月':'10-',u'十一月':'11-',u'十二月':'12-',u'十日':'10 ',u'十一日':'11 ',u'十二日':'12 ',u'十三日':'13 ',
                   u'十四日':'14 ',u'十五日':'15 ',u'十六日':'16 ',u'十七日':'17 ',u'十八日':'18 ',u'十九日':'19 ',u'二十日':'20 ',
                   u'三十日':'30 ',u'十时':'10:',u'十一时':'11:',u'十二时':'12:',u'十分':'10:',u'十一分':'11:',u'十二分':'12:',u'十三分':'13:',
                   u'十四分':'14:',u'十五分':'15:',u'十六分':'16:',u'十七分':'17:',u'十八分':'18:',u'十九分':'19:',
                   u'二十分':'20:',u'三十分':'30:',u'四十分':'40:',u'五十分':'50:'}
time_kv = {u'元':'01',u'一': '1', u'二': '2', u'三': u'3', u'四': '4', u'五': '5', u'六': '6', u'七': '7',
           u'八': '8', u'九': '9', u'十': '', u'零': '0', 'Ｏ':'0','〇': '0', 'O': '0','Ο':'0','○':'0',u'年': '-', u'月': '-', u'日': ' ',
           u'时': ':', u'点': ':', u'分': '', '.': '-', '/': '-'}


def norm_date(bulletin_date):
    '''格式化日期'''
    src_date_value = bulletin_date
    bulletin_date  = unicode(bulletin_date)

    for k, v in special_time_kv.items():
        bulletin_date = bulletin_date.replace(k, v)
    for k, v in time_kv.items():
        bulletin_date = bulletin_date.replace(k, v)

    if not re_find_one('\d{1,2}-\d{1,2}-\d{1,2}',bulletin_date):
        return src_date_value
    fields = bulletin_date.split('-')

    if len(fields) == 1 and len(fields[0]) == 8:
        bulletin_date = fields[0][:4] + '-' + fields[0][4:6] + '-' + fields[0][6:8]
        return bulletin_date

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
    time_str = unicode(time_str)

    time_str = time_str.replace(u'上午', '').replace(u'下午', '').replace(u'整', '')

    for k, v in special_time_kv.items():
        time_str = time_str.replace(k, v)
    for k, v in time_kv.items():
        time_str = time_str.replace(k, v)

    if not re_find_one('\d{1,2}:\d{1,2}:\d{1,2}|\d{1,2}:\d{1,2}|\d{1,2}',time_str):
        return time_str

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
    for k, v in special_time_kv.items():
        date_time = date_time.replace(k, v)
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


def result():
    '''实体解析返回结构体'''
    resp = {
        "CODE": 10000,
        "MSG": "",
        "LIST": [],
    }
    return resp


def strQ2B(ustring):
    """全角转半角"""
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 12288:  # 全角空格直接转换
            inside_code = 32
        elif (inside_code >= 65281 and inside_code <= 65374):  # 全角字符（除空格）根据关系转化
            inside_code -= 65248

        rstring += unichr(inside_code)
    return rstring


def strB2Q(ustring):
    """半角转全角"""
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 32:  # 半角空格直接转化
            inside_code = 12288
        elif inside_code >= 32 and inside_code <= 126:  # 半角字符（除空格）根据关系转化
            inside_code += 65248

        rstring += unichr(inside_code)
    return rstring

def html2excel(html):
    '''解析网页表格'''
    df = []
    if html == "":
        return

    if html.find('thead') != -1:
        thead_first_found = True
        tbody_first_found = True
        html_list = []
        for line in html.split('\n'):
            if line.find('thead') != -1 and thead_first_found:
                line = line.replace('thead', 'tbody')
                html_list.append(line)
                thead_first_found = False
                continue
            if line.find('thead') != -1 and not thead_first_found:
                continue
            if line.find('tbody') != -1 and tbody_first_found:
                tbody_first_found = False
                continue
            if line.find('tbody') != -1 and not tbody_first_found:
                html_list.append(line)
                continue
            html_list.append(line)

        html = ''.join(html_list)
        df = pandas.read_html(html, encoding='utf-8')
    else:
        df = pandas.read_html(html, encoding='utf-8')

    if len(df) >= 1:
        return df[0]

    return None


# 删除子串,根据字符串长度排序
def dedup(data_map):
    norm_data = set()
    for k, v in sorted(data_map.items(), lambda x, y: cmp(x[1], y[1]), reverse=True):
        found = False
        for data in norm_data:
            if k in data:
                found = True
                break
        if not found:
            norm_data.add(k)
    return list(norm_data)

def etl_sub_str(var_str,start,length):
    if len(var_str)<=start+length:
        return var_str
    return var_str[start:start+length]

def str2datetime(date_string,format):
    if date_string==None or date_string=='':
        return None
    try:
        return datetime.strptime(date_string,format)
    except:
        return None

if __name__ == "__main__":
    date_time = '3二○一○年元月二十一日'
    # print "date:", get_cur_time(), type(get_yes_date())
    '''source_dir = '/Users/zhangsl/pycharmProjects/crawler/i_entity_extractor/extractors/template/'
    target_dir = '/Users/zhangsl/pycharmProjects/crawler/i_entity_extractor/extractors/ssgs_shareholder/'
    ret = copy_files(source_dir, target_dir)'''

    url = "http://www.gdcourts.gov.cn:80/ecdomain/framework/gdcourt/hnohoambadpabboeljehjhkjkkgjbjie/dllpekfeaoobbboejmmogmdlofcghili.do?isfloat=1&disp_template=pchlilmiaebdbboeljehjhkjkkgjbjie&fileid=49834617&moduleIDPage=dllpekfeaoobbboejmmogmdlofcghili&siteIDPage=gdcourt&infoChecked=null"
    print norm_date_time("2015-02-06")




