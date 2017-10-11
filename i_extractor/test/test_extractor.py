#!/usr/bin/env
#-*- coding:utf-8 -*-
import HTMLParser
import base64
import json
import re
import urllib

from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer

from bdp.i_crawler.i_downloader.ttypes import DownLoadRsp
from bdp.i_crawler.i_extractor.ttypes import PageParseInfo
#from i_extractor.extractor import Extractor
from lxml import etree
#_ElementUnicodeResult
import sys
ss = "L2UnbnJUQ3QjHxZcbDQBTlZFIwlqMXYeTDwoZxplOCMIelU9BxASGF4dBgoyZS5tLSYyPxUDNFB%2BNnBhTwUjEh47KFUQKFJcKwNnOi9BE1MSRkADNi5JckwGIjosagcsYBISIiBrKFZyLSJ6ZAcVZmhnIGRxCydraw4EZxdmFyt7FVIVZFUMADdVBQlzBTgJVgBjWyp4C0swaxcQPzUnaSA7FWhRKxBAfh1aCmUbFgkoJ3EGLX84DTovJmMmEBAAJ2EmQTZUAxkjH01NWAUiUAURElFifjkcKz8ZdVZlBT0WB2Q%3D"
ss = "%2B"


exit(1)
html = ""
with open('douban_detail.html') as fp:
    html = fp.read()
print re.findall("语言:</span> (.*?)<br/>", html)
exit(1)
#html = ''
CHARSET_PATTERN = re.compile('<meta.*?(?:charset|CHARSET)=["\']?([a-zA-Z0-9\\-]+)["\']?.*?>')
#html = CHARSET_PATTERN.sub("", html)
drsp = DownLoadRsp()
drsp.content = html
#print txt
drsp.status = 0
#drsp.parse_extends = json.dumps({'debug':True})
drsp.content_type = "text/html"
drsp.url = "https://movie.douban.com/tag/"
#drsp.url = "http://www.baidu.com"
drsp.redirect_url = "https://movie.douban.com/tag/"
drsp.parse_extends = json.dumps({"parser_id":-1,"debug":True})
#print html
#from i_extractor.extractor import Extractor
from i_extractor import conf
from i_util.i_crawler_services import ThriftExtractor
#extractor = Extractor(conf)
extractor = ThriftExtractor(host="127.0.0.1", port=12300)

for i in xrange(2):
    ex_rsp = extractor.extract(drsp)
    print ex_rsp.extract_info
#print ex_rsp
