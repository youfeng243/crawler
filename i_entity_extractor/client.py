#coding=utf8
# !/usr/bin/env python

import sys

sys.path.append('./gen-py')


from bdp.i_crawler.i_extractor.ttypes import BaseInfo, CrawlInfo, ExtractInfo, PageParseInfo

from thrift import Thrift

import json
from i_util.i_crawler_services import ThriftEntityExtractor

def main():
    try:
        client = ThriftEntityExtractor(port=12500)

        with open('extractors/owx/宝应.txt', 'r') as f:
            html_table = f.read()

        print "client - start"
        extractor_info = {"topic_id": 155, "target_dir_name": "test", "extractor_name": "测试"}
        primary_keys = json.dumps([["title"]])
        schema = json.dumps({"type": "object", "title": "百度搜索", "description": "百度搜索",
                             "properties": {"keyword": {"type": "string", "title": "搜索公司"},
                                            "title": {"type": "string", "title": "标题"},
                                            "url": {"type": "string", "title": "来源url"},
                                            "abstract": {"type": "string", "title": "抽象"}}})
        topic_info = {"id": 155, "name": "测试动态加载解析器", "schema": schema, "primary_keys": primary_keys, "table_name":"test"}
        topic_info = json.dumps(topic_info)
        resp = client.add_topic(topic_info)
        # print resp.msg
        #
        extractor_info = json.dumps(extractor_info)
        resp = client.add_extractor(extractor_info)
        resp = client.reload(155)

        extract_data = {}

        extract_data = json.dumps(extract_data)
        base_info = BaseInfo(url="", site_id=1)
        extract_info = ExtractInfo(ex_status=2, extract_data=extract_data, topic_id=155)

        crawl_info = CrawlInfo(download_time=1474547589)
        req = PageParseInfo(base_info=base_info, crawl_info=crawl_info, extract_info=extract_info, scheduler="a",
                            parse_extends="b", data_extends="c")


        resp = client.entity_extract(req)
        print resp.entity_data_list


    # 捕获异常
    except Thrift.TException, ex:
        print "%s" % (ex.message)


if __name__ == '__main__':
    main()