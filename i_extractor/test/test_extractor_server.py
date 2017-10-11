#!/usr/bin/env python
# -*- coding:utf-8 -*-

from i_util.i_crawler_services import ThriftExtractor
ex_server = ThriftExtractor(host="127.0.0.1", port=17301)
print ex_server.reload_parser_config("-1")