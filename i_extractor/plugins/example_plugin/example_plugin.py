#!/usr/bin/env python
# -*- coding:utf-8 -*-
from bdp.i_crawler.i_extractor.ttypes import Link


def extract(url, content, results):
    """
    :param url: 网页链接
    :param content: 网页正文
    :param results:results["data"] = {key1:[{sub_key:value}], key2:value}
                  results["links"] = [LinkLink(url=url, type=LinkType, anchor="", parse_extends={})]
    :return: results
    """
    results['data'].update({"key1":[{"sub_key":"value"}], "key2":"value"})
    Links = results["links"]
    Links.append(Link(url="http://plugin.test", type=0))
    return results