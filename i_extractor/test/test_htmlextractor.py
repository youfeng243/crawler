#!/usr/bin/env python
#-*- coding:utf-8 -*-
import json
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
from lxml import etree

from i_extractor.libs.html_extractor import HTMLExtractor

parse_rule = [
    {
        "$value_type": "link",
        "$name": "penalty_id",
        "$each": [
            {
                "$value_type": "html",
                "$name": "inv",
                "$each": [
                ],
                "$require": "false",
                "$parse_method": "regex",
                "$parse_rule": "investor.inv = \"(.*?)\";"
            },
            {
                "$value_type": "html",
                "$name": "conDate",
                "$each": [
                ],
                "$require": "false",
                "$parse_method": "regex",
                "$parse_rule": "invt.conDate = '(.*?)';"
            },
            {
                "$value_type": "html",
                "$name": "acConAm",
                "$each": [
                ],
                "$require": "false",
                "$parse_method": "regex",
                "$parse_rule": """
                        join-string(list-concat(re('invtActl.acConAm =  "(.*?)";'), '万元'), "sp")
                """
            }
        ],
        "$require": "false",
        "$parse_method": "path",
        "$parse_rule": u"re('investor.inv[\s\S]*?list.push\(investor\);')"
    }
]
parse_rule =[{
                u'$value_type': u'plain_text',
                u'$name': u'test',
                u'$each': [],
                u'$require': u'false',
                u'$parse_method': u'path',
                u'$parse_rule': u'list-concat(list-concat("www.fangdd.com/",re("fangdd.com/(\w+)")),"/loupan/pg1")'}]
def test_extract():
    html = ""
    with open("./data/jsvar_html.html", "r") as fp:
        html = fp.read()
    html = html.decode('utf-8')
    html_x = etree.HTML(html)
    #print etree.tostring(html_x, pretty_print=True, encoding='utf-8')
    her = HTMLExtractor(html_x,base_url="http://120.24.17.155/courtweb/front/gzxxList/JD0-qbcx-up-3-", debug=False)
    print json.dumps(her.extract(parse_rule, all = False), ensure_ascii=False)


test_extract()