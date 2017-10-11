#!/usr/bin/env python
# -*- coding:utf-8 -*-

import re

from lxml import etree
from lxml.html.clean import Cleaner

import parser_tool

ns = etree.FunctionNamespace(None)

def _collect_string_content(x):
    value = ""
    if isinstance(x, etree._Element):
        value = x.xpath("string(.)")
    elif isinstance(x, basestring):
        value = unicode(x)
    elif isinstance(x, list):
        for lx in x:
            if isinstance(lx, etree._Element):
                value = value + lx.xpath('string(.)')
            elif isinstance(x, basestring):
                value = value + lx
    return value

def __cartesian_product(strings, depth, multi_strings, result_set, max_depth):
    if depth == max_depth:
        result_set.add(''.join(strings))
        return
    for x in multi_strings[depth]:
        value = _collect_string_content(x)
        strings.append(value)
        __cartesian_product(strings, depth+1, multi_strings, result_set, max_depth)
        strings.pop()

def list_concat(context, *args):
    multi_strings = [x if isinstance(x, list) else [x] for x in args]
    result_set = set()
    __cartesian_product([], 0, multi_strings, result_set, max_depth=len(multi_strings))
    return list(result_set)

def join_string(context, strings, sp="\t"):
    return sp.join(map(_collect_string_content, strings))

def list_zip_concat(context, *args):
    not_meet_length = 999999
    min_length = not_meet_length
    str_pos = []
    args = list(args)
    for i in range(len(args)):
        if isinstance(args[i], list):
            min_length = min(min_length, len(args[i]))
        else:
            str_pos.append(i)
    result_set = set()
    if min_length == not_meet_length:
        result_set.add("".join(args))
        return result_set
    for i in str_pos:
        args[i] = [args[i]] * min_length
    multi_zip = zip(*args)
    for strings in multi_zip:
        result_set.add("".join(map(_collect_string_content, strings)))
    return list(result_set)

def list_substring(context, strings, start, end = None):
    start = int(start) - 1
    if end:
        end = int(end)
    if not isinstance(strings, list):
        strings = [strings]
    strings = [_collect_string_content(x) for x in strings]
    res = []
    for s in strings:
        try:
            res.append(s[start:end])
        except Exception as e:
            pass
    return res

def list_substring_after(context, strings, pattern):
    if not isinstance(strings, list):
        strings = [strings]
    strings = [_collect_string_content(x) for x in strings]
    res = []
    for s in strings:
        try:
            res.append(''.join(s.split(pattern)[-1:]))
        except:
            pass
    return res


def list_substring_before(context, strings, pattern):
    if not isinstance(strings, list):
        strings = [strings]
    strings = [_collect_string_content(x) for x in strings]
    res = []
    for s in strings:
        try:
            res.append(''.join(s.split(pattern)[:1]))
        except:
            pass
    return res

DIGITS = re.compile(ur'(\d+[\.]?\d*)')
def normal_date(context, string, t_type="datetime"):
    """
    :param context:
    :param string:
    :param t_type: datetime, timestamp
    :return:
    """
    string = _collect_string_content(string)
    if t_type == "datetime":
        ds = parser_tool.chs_time_pattern.findall(string)
        if len(ds) < 1:
            return string
        m_ds = ds[0]
        for d in ds[1:]:
            if len(m_ds) < len(d):
                m_ds = d
        try:
            dts = parser_tool.parser_chs_date(m_ds)
            if dts is not None:
                return str(dts)
            dts = parser_tool.translate_chs_date(string)
            if dts is not None:
                return str(dts)
            return string
        except Exception as e:
            return string

    elif t_type == "timestamp":
        string = _collect_string_content(string)
        digits = DIGITS.findall(string)
        if len(digits) < 1:
            return string
        m_digits = digits[0]
        for s in digits[1:]:
            if len(s) > len(m_digits):
                m_digits = s
        m_digits = int(m_digits[:10])
        return str(parser_tool.timestamp2normal_date(m_digits))
    return string


def normal_chs_date(context, string):
    string = _collect_string_content(string)
    o = parser_tool.chs_date_pattern.findall(string)
    if len(o) < 1:
        return string
    else:
        try:
            return parser_tool.translate_chs_date(o[0])
        except Exception as e:
            return string

def format_string(context, pattern, *args):
    strings = [_collect_string_content(x) for x in args]
    return pattern.format(*strings)

def replace(context, string, old, new):
    string = _collect_string_content(string)
    return string.replace(old, new)

def list_replace(context, strings, old, new):
    if not isinstance(strings, list):
        strings = [strings]
    strings = [_collect_string_content(x) for x in strings]
    res = []
    for s in strings:
        try:
            res.append(s.replace(old, new))
        except:
            pass
    return res

_replace_line = re.compile("(<br>)|(</p>)|(</li>|</div>)", re.I|re.S)
_replace_blank = re.compile("""(?:<.*?>)|\r|\t""", re.I|re.S)
_remove_multi_line = re.compile('\n+', re.I|re.S)
_special_char = re.compile(r"&(#?[xX]?(?:[0-9a-fA-F]+|\w{1,8}));")
_html_cleaner = Cleaner(style=True)


def _unescape( s):
    if '&' not in s:
        return s

    def replaceEntities(s):
        s = s.groups()[0]
        special_char_dict = {
            "amp": "&",
            "lt": "<",
            "gt": ">",
            "nbsp": " ",
        }
        try:
            if s[0] == "#":
                s = s[1:]
                if s[0] in ['x', 'X']:
                    c = int(s[1:], 16)
                else:
                    c = int(s)
                return unichr(c)
            else:
                return special_char_dict.get(s,s)

        except ValueError:
            return '&#' + s + ';'
    return _special_char.sub(replaceEntities, s)


def _clean_html(element):
    text = element
    if not isinstance(element, basestring):
        text = etree.tostring(element, encoding='utf-8', pretty_print=True)
    if not isinstance(text, unicode):
        text = text.decode('utf-8')
    text = _html_cleaner.clean_html(text)
    text = _replace_line.sub('\n',text)
    text = _replace_blank.sub('',text)
    text = _remove_multi_line.sub('\n', text)
    text = _unescape(text).strip()
    return text

def normal_string(context, elements):
    if isinstance(elements, list):
        return "\n".join(map(_clean_html, elements))
    else:
        return _clean_html(elements)



def url_find(context, string):
    base_url = context.context_node.base
    try:
        base_url = context.context_node.xpath("//ori_url/@url")[0]
    except Exception as e:
        pass
    return "".join(re.findall(string, base_url))

ns['list-concat'] = list_concat
ns['list-zip-concat'] = list_zip_concat
ns['normal-chs-date'] = normal_chs_date
ns['list-substring'] = list_substring
ns['list-substring-after'] = list_substring_after
ns['list-substring-before'] =  list_substring_before
ns['format-string'] = format_string
ns['replace'] = replace
ns['list-replace'] = list_replace
ns['url-find'] = url_find
ns['normal-string'] = normal_string
ns['join-string'] = join_string
ns['normal-date'] = normal_date

if __name__ == "__main__":
    ss = """<html><head><base href="/baidu.com" /></head><body>
    <ul>
    <li><a>1</a></li>
    <li><a>2</a></li>
    <li><a>3</a></li>
    </ul>
    </body></html>
    """
    #from elementtree.ElementTree import Element
    test = ["http://", '/']
    print "pattern:{},{}".format(*[1,2])
    doc = etree.HTML(ss, base_url="http://baidu.com")
    doc.attrib["base"] = "fuck"
    doc.append(etree.Element("ori_url", {"url":"baidu.com"}))
    print etree.tostring(doc)
    print doc.xpath('normal-string(.)')
    print doc.xpath("url-find('baidu')")
    print doc.tag
    print doc.base
    print doc.xpath(u"""list-substring(list-concat(//li/a,"20131210"), 1, 4)""")
    print doc.xpath(u"""join-string(//li/a)""")
    print format_string('pattern:{},{}', "1","2")
    print normal_date(None, "1482723983", 'ts')
    print "date",doc.xpath(u'normal-chs-date("本院定于20161210到15:00在第三审判庭公开开庭审理(2015)渝铁法民初字第00154号原告重庆市华廷建玲物流有限公司诉被告柯圣鹏挂靠经营合同纠纷一案。\r\n\t\t\t\t\t重庆市铁路运输法院 承办人：饶琳惠")')