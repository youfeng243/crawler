#!/usr/bin/env/python
#-*- coding:utf-8 -*-
import json
import traceback
from urlparse import urljoin

from jsonpath_rw import parse
from lxml import etree

from bdp.i_crawler.i_extractor.ttypes import Link
from extract_exception import ParserErrorException, RequireException
from html_extractor import HTMLExtractor, etree2string, _collect_string_content,ParseType
from i_extractor.libs.json_lxml import Json2HTML, Domtree2Json
from i_util.tools import get_md5_i64
from reextends import ReExtends


class ValueType():
    link="link"
    plain_text="plain_text"
    array="array"
    html="html"
    recursion="recursion"

class Json2XmlExtractor(HTMLExtractor):
    def __init__(self, json_obj, base_url="", debug=False):
        self.base_url = base_url
        self.json_obj = None
        if not isinstance(json_obj, dict) and not isinstance(json_obj, list):
            try:
                self.json_obj = json.loads(json_obj)
            except Exception as e:
                if debug:
                    self.result = {"init_error": traceback.format_exc()}
                raise Exception("text can't convert to json")
        else:
            self.json_obj = json_obj
        try:
            html_text = Json2HTML.convert(self.json_obj)
            self.xdoc = etree.HTML(html_text, base_url = base_url)
        except Exception as e:
            if debug:
                self.result = {"init_error":str(traceback.format_exc())}
            raise Exception("html_text can't convert to etree.Element")
        self.result = {}
        self.base_url = base_url
        self.debug = debug

    def extract(self, parse_rule,  all=True):
        if self.result:
            return self.result
        #custom data must extract firtst
        data = self._extract_multi(self.xdoc, parse_rule)
        if not all:return data
        #common_data = self._extract_common_data(self.xdoc)
        if not parse_rule:
            data = self.json_obj
        links = self._extract_links()
        return {"data":data,"common_data":{}, "links":links}

    def _extract_single(self, x, r):
        result = ""
        value_type = r.get("$value_type")
        parse_method = r.get("$parse_method")
        parse_rule = r.get("$parse_rule")
        if not etree.iselement(x):
            raise Exception("Invalid etree element.")
        if value_type == ValueType.recursion:
            result = []
            if r.get("$each"):
                if parse_method == ParseType.path:
                    for ex in x.xpath(parse_rule):
                        result.append(self._extract_multi(ex, r.get("$each")))
                elif parse_rule == ParseType.regex:
                    raise Exception("RegEx can not be allowed to recursive.")
        elif value_type == ValueType.array:
            if parse_method == ParseType.path:
                result = [o.strip() if isinstance(o, basestring) else _collect_string_content(o) for o in x.xpath(parse_rule)]
            elif parse_method == ParseType.regex:
                result = [o.strip() for o in ReExtends(etree2string(x)).parse(parse_rule.encode('utf-8'))]
        elif value_type == ValueType.plain_text:
            if parse_method == ParseType.path:
                f = x.xpath(parse_rule)
                if isinstance(f, basestring):
                    result = f.strip()
                elif isinstance(f, list):
                    result = "\t".join([o.strip()
                                        if isinstance(o, basestring)
                                        else o.xpath('string(.)')
                                        for o in f
                                        ])
                    result = result.strip()
                else:
                    raise Exception("type {} can't convert to plain_text.".format(type(f)))
            elif parse_method == ParseType.regex:
                result = "\t".join([o.strip() for o in ReExtends(etree2string(x)).parse(parse_rule.encode('utf-8'))])
        elif value_type == ValueType.html:
            if parse_method == ParseType.path:
                result = Domtree2Json.convert(x.xpath(parse_rule))
            elif parse_method == ParseType.regex:
                result = "\t".join(ReExtends(etree2string(x)).parse(parse_rule.encode('utf-8')))
        elif value_type == ValueType.link:
            result = []
            links = []
            if parse_method == ParseType.path:
                f = x.xpath(parse_rule)
                if isinstance(f, basestring):
                    links.append(f.strip().split('#')[0])

                else:
                    links.extend([o.strip().split('#')[0]
                             if isinstance(o, basestring)
                              else o.xpath('string(.)')
                             for o in f
                             ])
            elif parse_method == ParseType.regex:
                links.extend(o.strip().split('#')[0]for o in ReExtends(etree2string(x)).parse(parse_rule.encode('utf-8')))
            for l in links:
                if l.startswith('javascript:'):continue
                l = urljoin(self.base_url, l)
                result.append(Link(url=l, url_id=get_md5_i64(l), anchor=""))
        return result

#不建议使用JsonExtractor, Json2XmlExtractor可以使用语义性更强和规则通配置
class JsonExtractor(object):
    def __init__(self, json_obj, base_url="", debug=False):
        self.json_obj = None
        self.base_url = base_url
        if not isinstance(json_obj, dict) or isinstance(json_obj, list):
            try:
                self.json_obj = json.loads(json_obj)
            except Exception as e:
                if debug:
                    self.result = {"init_error": traceback.format_exc()}
                raise Exception("text can't convert to json")
        else:
            self.json_obj = json_obj
        self.result = {}
        self.debug = debug

    def extract(self, parse_rule, all=True):
        if self.result:
            return self.result
        common_data = {}
        links = []
        data = self._extract_multi(self.json_obj, parse_rule)
        if not all:
            return data
        return {"data":data,"common_data":common_data, "links":links}

    def _extract_multi(self, json_obj, parse_rule):
        result = {}
        if not isinstance(parse_rule, list):
            raise ParserErrorException()
        for item in parse_rule:
            try:
                result[item['$name']] = self._extract_single(json_obj, item)
                if not result[item['$name']] or len(result[item['$name']]) == 0:
                    raise RequireException()
            except RequireException as e:
                if item['$value_type'] == ValueType.link:
                    result[item['$name']] = []
                elif item.get('$require', 'false') == 'false':
                    pass
                elif self.debug:
                    result[item['$name']] = "ERROR: {}".format(str(e))
                else:
                    raise RequireException()
            except Exception as e:
                if self.debug:
                    result[item['$name']] = "ERROR: {}".format(str(e))
                else:
                    if item.get('$require', 'false') == 'false':
                        pass
                    else:
                        raise ParserErrorException()

        return result
    def _extract_single(self, x, r):
        result = ""
        value_type = r.get("$value_type")
        parse_method = r.get("$parse_method")
        parse_rule = r.get("$parse_rule")

        if not isinstance(x, list) and not isinstance(x, dict):
            raise Exception("{} type is {}, not list or dict".format(str(x), type(x)))
        if value_type == ValueType.recursion:
            result = []
            if r.get("$each"):
                if parse_method == ParseType.jsonpath:
                    tmp = parse(parse_rule).find(x)
                    if len(tmp) and isinstance(tmp[0].value, list):
                        for ex in tmp[0].value:
                            result.append(self._extract_multi(ex, r.get("$each")))
                    elif len(tmp) and isinstance(tmp[0].value, dict):
                        result.append(self._extract_multi(tmp[0].value, r.get("$each")))
                elif parse_rule == ParseType.regex:
                    raise Exception("RegEx can not be allowed to recursive.")
        elif value_type == ValueType.array:
            result = []
            if parse_method == ParseType.jsonpath:
                tmp = parse(parse_rule).find(x)
                if len(tmp) > 0:
                    if isinstance(tmp[0].value, list):
                        for each in tmp:
                            result.extend(each.value)
                    else:
                        for each in tmp:
                            result.append(each.value)
            elif parse_method == ParseType.regex:
                ss = json.dumps(x)
                tmp = ReExtends(ss).parse(parse_rule)
                if not isinstance(tmp, list):
                    tmp = [tmp]
                result.extend(tmp)
        elif value_type == ValueType.plain_text :
            result = []
            if parse_method == ParseType.jsonpath:
                tmp = parse(parse_rule).find(x)
                if len(tmp) > 0:
                    if not isinstance(tmp[0].value, basestring):
                        result = "\t".join([json.dumps(each.value) for each in tmp])
                    else:
                        result = "\t".join([each.value for each in tmp])
            elif parse_method == ParseType.regex:
                ss = json.dumps(x)
                tmp = ReExtends(ss.decode('utf-8')).parse(parse_rule)
                if not isinstance(tmp, list):
                    tmp = [tmp]
                result = "\t".join(tmp)
        elif value_type == ValueType.link:
            result = []
            links = []
            if parse_method == ParseType.jsonpath:
                tmp = parse(parse_rule).find(x)
                if len(tmp)>0:
                    if isinstance(tmp[0].value, basestring):
                        links = [each.value for each in tmp]
            elif parse_method == ParseType.regex:
                ss = json.dumps(x)
                links = ReExtends(ss).parse(parse_rule)
                if not isinstance(links, list):
                    links = [links]
            for l in links:
                if l.startswith('javascript:'):continue
                l = urljoin(self.base_url, l)
                result.append(Link(url=l, url_id=get_md5_i64(l), anchor=u''))
        return result
