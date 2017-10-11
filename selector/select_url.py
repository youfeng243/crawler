#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Author  gikieng.li
# @Date    2017-06-01 09:56
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import pymongo
import click
from config import mongodb
client = pymongo.MongoClient(mongodb['host'], mongodb['port'])
final_data = client[mongodb['database']]
if mongodb['username'] and mongodb['password']:
    final_data.authenticate(mongodb['username'], mongodb['password'])

@click.command()
@click.option("-t", "--table", required=True, help="表名")
@click.option("-u", "--url_pattern", required=True, help="对_src.url的正则筛选")
@click.option("-d", "--download_type", default="simple",type=click.Choice(['simple', 'phantom']), help="下载类型")
@click.option("-p", "--parser_id", required=False, default="-1", help="解析规则id")
@click.option("-m", "--http_method", required=False, default="get", type=click.Choice(['get', 'post']), help="http请求方式")
def run(**options):
    if options['download_type'] not in ['simple', 'phantom']:
        print "no support download type: %s" % options['download_type']
    with open("%s.txt" %(options['table']), "w") as fp:
        for item in final_data[options['table']].find({"_src.url":{"$regex":options['url_pattern']}},{"_id":0, "_src":1}):
            for src in item['_src']:
                fp.write("%s\t%s\t%s\t%s\n" %(
                    src['url'],
                    options["download_type"],
                    options["parser_id"],
                    options['http_method']))
        fp.flush()

run()