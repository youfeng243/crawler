#!/usr/bin/env python
# -*- coding:utf-8 -*-

from i_extractor import conf
from i_extractor.libs.future.config_handle import ConfigHandler
from i_extractor.libs.future.models import ParseConfig

conf_handle = ConfigHandler(conf=conf)
data_rules = {
    "ul": {
            "$value_type": "recursion",
            "$parse_method": "xpath",
            "$parse_rule": "//*[@id=\"list\"]/table/tbody/tr",
            "$require":"false",
            "$each": {
                    "href":{
                            "$value_type": "plain_text",
                            "$parse_method": "regex",
                            "$parse_rule": 'href="(.*?)"',
                            "$each": {},
                            },
                    "link_name":{
                        "$value_type": "plain_text",
                        "$parse_method": "xpath",
                        "$parse_rule": "./text()",
                        "$require":"false",
                        "$each": {},
                        }
                    }
            },
    "title":{
        "$value_type": "plain_text",
        "$parse_method": "xpath",
        "$parse_rule": "//title",
        "$require": "false",
        "$each":{}
        }
    }
follow_rule = [{"url":"http:baidu.com", "parser_name":None}]
ch = ConfigHandler(conf)
def test_upsert():
    extr_conf = ParseConfig()
    extr_conf.data_rules = data_rules
    extr_conf.rule_name = """百""度"""
    extr_conf.url_format="baidu.com/index.html"
    extr_conf.datas
    ch.upsert(extr_conf)

def test_get_config_by_name(name=u'http://baidu.com'):
    print ch.get_config_by_id(name)

def test_get_config_by_url(url="http://baidu.com/index.html"):
    print ch.get_config_by_url(url)
#test_upsert()
#test_upsert()
test_get_config_by_name()
test_get_config_by_url()