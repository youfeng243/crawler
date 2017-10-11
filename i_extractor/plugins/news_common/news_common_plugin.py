#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author  gikieng.li
# @Date    2017-03-08 18:40
import time


def extract(url, content, results):
    data = results['data']
    common_data = results['common_data']
    if not data.has_key("href"):
        data["href"] = url
        data["content"] = common_data.get('content', "")
        data["title"] = common_data.get("title", "")
        if common_data.get("public_time"):
            try:
                data["publish_time"] = time.strftime("%Y-%m-%d %H:%M:%S",
                                                             time.localtime(float(common_data['public_time'])))
            except:
                data['publish_time'] = common_data.get('public_time', "")
        data['_download_time'] = time.time()
    return results