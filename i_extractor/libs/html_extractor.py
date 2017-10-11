#!/usr/bin/env python
# -*- coding:utf-8 -*-
import traceback

import six
from lxml import etree, html
from six.moves.urllib.parse import urljoin

import lxml_function_extends
from bdp.i_crawler.i_extractor.ttypes import Link
from i_extractor.libs import parser_tool
from i_extractor.libs.extract_exception import ParserErrorException, RequireException
from i_util.tools import get_md5_i64
from reextends import ReExtends
from sf_extractor import SFExtractor
ns = lxml_function_extends.ns
class ParseType(object):
    path = "path"
    regex = "regex"

class ValueType(object):
    html="html"
    plain_text="plain_text"
    link="link"
    array="array"
    recursion="recursion"

XHTML_NAMESPACE = "http://www.w3.org/1999/xhtml"
def etree2string(xdoc, pretty_print=True):
    string = html.tostring(xdoc, encoding='utf-8', pretty_print = pretty_print)
    if string.startswith("<html><body><p>"):
        string = string.replace("<html><body><p>", "").replace("</p></body></html>", "")
    return string

_collect_string_content = etree.XPath("string()")


def _nons(tag):
    if isinstance(tag, six.string_types):
        if tag[0] == '{' and tag[1:len(XHTML_NAMESPACE)+1] == XHTML_NAMESPACE:
            return tag.split('}')[-1]
    return tag

class LinkExtractor(object):

    def __init__(self, tag="a", attr="href", process=None, unique=True):
        self.scan_tag = tag if callable(tag) else lambda t: t == tag
        self.scan_attr = attr if callable(attr) else lambda a: a == attr
        self.process_attr = process if callable(process) else lambda v: v
        self.unique = unique

    def _iter_links(self, document):
        for el in document.iter(etree.Element):
            if not self.scan_tag(_nons(el.tag)):
                continue
            attribs = el.attrib
            for attrib in attribs:
                if not self.scan_attr(attrib):
                    continue
                yield (el, attrib, attribs[attrib])

    def _extract_links(self, selector, base_url, response_encoding='utf-8', links =[]):
        # hacky way to get the underlying lxml parsed document
        links_set = {}
        for link in links:
            links_set[link.url] = link
        for el, attr, attr_val in self._iter_links(selector):
            # pseudo lxml.html.HtmlElement.make_links_absolute(base_url)
            try:
                attr_val = urljoin("", attr_val)
            except ValueError:
                continue # skipping bogus links
            else:
                url = self.process_attr(attr_val)
                if url is None:
                    continue

            if url.startswith("javascript"):continue #skip javascript
            # to fix relative links after process_value
            url = urljoin(base_url, url)
            url = url.split('#')[0]
            anchor = _collect_string_content(el) or ''
            if isinstance(anchor, unicode):
                anchor = anchor.encode('utf-8')
            anchor = anchor.replace('\r', '').replace('\n', '').replace('\t', " ").strip()
            link = Link(url=url, url_id=get_md5_i64(url), anchor=anchor,
                        )
            links_set[link.url] = link
            #links.append(link)
        return links_set.values()

    def extract_links(self, response, base_url, links = []):
        return self._extract_links(response, base_url, links=links)

    def _process_links(self, links):
        return self._deduplicate_if_needed(links)

    def _deduplicate_if_needed(self, links):
        f_links = [links[0]]
        pre_link = links[0]
        for l in links[1:]:
            if l.url.startswith(pre_link.url):
                continue
            else:
                pre_link = l
                f_links.append(l)
        return f_links



class HTMLExtractor(object):
    def __init__(self, html_text, base_url, debug=False):
        self.result = {}
        try:
            if etree.iselement(html_text):
                self.xdoc = html_text
            else:
                self.xdoc = etree.HTML(html_text, base_url=base_url)
        except Exception, e:
            if debug:
                self.result = {"init_error":str(traceback.format_exc())}
            raise Exception("html_text can't convert to etree.Element")
        self.result = {}
        html_base_url = self.xdoc.xpath("//head/base/@href")
        self.base_url = base_url
        self.xdoc.append(etree.Element("ori_url", {"url":base_url}))
        if len(html_base_url) > 0:
            self.base_url = html_base_url[0]
        self.debug = debug

    def _extract_common_data(self, xdoc):
        result = {}
        try:
            result = SFExtractor().extract(etree2string(xdoc, pretty_print=True).decode(encoding='utf-8'))
        except Exception:
            pass
        lang = xdoc.xpath('string(//html/@lang)')
        if isinstance(lang, unicode):
            lang = lang.encode('utf-8')
        if lang == '':  # default zh-ch
            lang = 'zh-ch'
        result['lang'] = lang
        result['content'] = result.get('content', '').encode('utf-8')
        result['title'] = result.get('title', '').encode('utf-8')
        result['content_finger'] = get_md5_i64(result.get('content', ''))
        try:
            result['public_time'] = parser_tool.string2timestamp(result.get('public_time', ''))
            if result['public_time'] > 2147483648 or result['public_time'] < -2147483648:
                result['public_time'] = None
        except Exception as e:
            result['public_time'] = None
        return result

    def _extract_links(self):
        linkex = LinkExtractor()
        links = linkex.extract_links(self.xdoc, self.base_url, links=[])
        return links

    def extract(self, parse_rule, all=True, extract_content=False):
        if self.result:
            return self.result
        #custom data must extract firtst
        data = self._extract_multi(self.xdoc, parse_rule)
        if not all:return data
        if extract_content:
            common_data = self._extract_common_data(self.xdoc)
        else:common_data = {}
        links = self._extract_links()
        return {"data":data,"common_data":common_data, "links":links}

    def _extract_multi(self, xdoc, parse_rule):
        result = {}
        if not isinstance(parse_rule, list):
            raise ParserErrorException()
        for item in parse_rule:
            try:
                result[item['$name']] = self._extract_single(xdoc, item)
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
                    result[item['$name']] = "ERROR: {}".format(str(e.message))
                else:
                    raise ParserErrorException()
        return result

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
                elif parse_method == ParseType.regex:
                    for ex in ReExtends(etree2string(x)).parse(parse_rule.encode('utf-8')):
                        result.append(self._extract_multi(etree.HTML(ex), r.get("$each")))
                    #raise Exception("RegEx can not be allowed to recursive.")
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
                result = "\t".join((etree2string(o) if o != None else "") for o in x.xpath(parse_rule))
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


