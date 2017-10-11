#!/usr/bin/env
# -*- coding:utf-8 -*-
import HTMLParser
import re
import time
from datetime import datetime

import dateparser

chs_date_pattern = re.compile(
    u'[0-9一二三四五六七八九零〇oO]{4}[年\-/\.][0-9一二三四五六七八九十零〇oO]{1,3}[月|\-/\.][0-9一二三四五六七八九十零〇oO]{1,3}日?',re.S|re.I)
chs_time_pattern = re.compile(
    u'[0-9一二三四五六七八九零〇oO]{4}[年|\-|\.][0-9一二三四五六七八九十零〇oO]{1,3}[月|\-|\.][0-9一二三四五六七八九十零〇oO]{1,3}日?(?:\s*[0-9]{1,2}:[0-9]{1,2})?',
    re.I | re.S)

def datetime2timestamp(dt):
    try:
        t = dt.timetuple()
        timeStamp = int(time.mktime(t))
        return timeStamp
    except Exception as e:
        return 0

html_parser = HTMLParser.HTMLParser()

def unescape(string):
    if not isinstance(string, unicode):
        string = string.decode('utf-8')
    return html_parser.unescape(string).encode('utf-8')

#depredicate
def translate_chs_date(ss):
    if not isinstance(ss, unicode):
        ss = ss.decode('utf-8', errors='ignore')
    ss = ss.strip()
    date_map = {u'○':u'0',
                u'O':u'0',
                u'〇':u'0',
                u'一':u'1',
                u'二':u'2',
                u'三':u'3',
                u'四':u'4',
                u'五':u'5',
                u'十':u'十',
                u'六':u'6',
                u'七':u'7',
                u'八':u'8',
                u'九':u'9',
                u'年':u'-',
                u'月':u'-',
                u'日':u'',
                u'0':u'0',
                u'1':u'1',
                u'2':u'2',
                u'3':u'3',
                u'4':u'4',
                u'5':u'5',
                u'6':u'6',
                u'7':u'7',
                u'8':u'8',
                u'9':u'9',
                u'-' :u'-',
                u'.' :u'-',
                u'/' :u'-',
                }
    res = ''.join(map(date_map.get, ss))
    y, m, d = res.split('-')
    if m.find(u'十') != -1:
        m = unicode(m)
        if len(m) == 1:
            m = '10'
        else:
            m = m.replace(u'十', '1')
    if d.find(u'十') != -1:
        d = unicode(d)
        if len(d) == 3:
            d = d.replace(u'十', '')
        elif len(d) == 1:
            d = '10'
        elif len(d) == 2:
            if d[0] == '十':
                d = d.replace(u'十', '1')
            else:
                d = d.replace(u'十', '0')
    return str(datetime(*map(int, (y, m, d))))

date_parser = dateparser.DateDataParser(languages=['en', 'zh'])
def parser_chs_date(string):
    rets = date_parser.get_date_data(string)['date_obj']
    return rets

def string2timestamp(string):
    return datetime2timestamp(dateparser.parse(string,languages=['en', 'zh']))

def timestamp2normal_date(ts):
    #秒级时间
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))



if __name__ == "__main__":
    print string2timestamp('0经')
    print translate_chs_date(chs_date_pattern.findall(u"来源： 作者： 日期：2014年11/11 点击：")[0])
    print dateparser.parse(u"2016年11月02日 14:12")
    print datetime.fromtimestamp(string2timestamp(u"是2016年11月02 14:12"))