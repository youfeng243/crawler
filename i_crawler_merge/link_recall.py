# coding:utf-8

import sys

sys.path.append('..')

from i_util.tools import get_url_info
import json, re, os
from bdp.i_crawler.i_extractor.ttypes import Link

dic_path = os.path.dirname(os.path.realpath(__file__)) + '/dic/'
if not os.path.exists(dic_path):
    os.makedirs(dic_path)
dic_file = dic_path + 'rule.dic'

rule_list = []


def read_dic():
    if os.path.exists(dic_file):
        try:
            with open(dic_file) as f:
                for line in f.xreadlines():
                    rule = json.loads(line)
                    rule_list.append(rule)
        except Exception, e:
            del rule_list[:]
            raise e


def load_rule():
    if rule_list:
        return rule_list
    else:
        read_dic()


def reload_rule():
    del rule_list[:]
    read_dic()


def is_matched(url, rule):
    site_prefix = rule.get('site_prefix')
    url_format = rule.get('url_format')
    url_type = rule.get('url_type')
    site = get_url_info(url).get('site')
    if isinstance(site_prefix, basestring):
        m_site = re.match(site_prefix, site)
        if m_site and isinstance(url_format, basestring):
            m_url = re.match(url_format, url)
            if m_url and isinstance(url_type, int):
                return True
    return False


def recall(link):
    try:
        load_rule()
        url = link.url
        for rule in rule_list:
            if is_matched(url, rule):
                link.type = rule.get('url_type')
    except Exception, e:
        raise e
    return link


if __name__ == '__main__':
    load_rule()

    link = Link()
    link.url = 'http://www.baidu.com/url2'
    link.type = 0
    link = recall(link)
    print link
