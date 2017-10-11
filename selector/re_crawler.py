# coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import os
import json
import traceback
import time
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer

sys.path.append('..')
from i_util.pybeanstalk import PyBeanstalk
from bdp.i_crawler.i_downloader.ttypes import DownLoadReq
from i_util.logs import LogHandler
import config

log = LogHandler('re_crawler', console_out=True)
beanstalk_conf = config.beanstalk_conf
beanstalk_client = PyBeanstalk(beanstalk_conf['host'], beanstalk_conf['port'])


def req_to_string(req):
    str_req = ""
    try:
        tMemory_b = TMemoryBuffer()
        tBinaryProtocol_b = TBinaryProtocol(tMemory_b)
        req.write(tBinaryProtocol_b)
        str_req = tMemory_b.getvalue()
    except:
        log.error('crawled_failt\terror:%s' % (traceback.format_exc()))
    return str_req


def create_download_req(url, method='simple', parser_id="-1", http_method="get"):
    download_req = DownLoadReq()
    download_req.url = url
    download_req.post_data = {}
    download_req.src_type = 'linkbase'
    download_req.download_type = method
    download_req.parse_extends = json.dumps({"parser_id":parser_id})
    download_req.method = http_method
    scheduler_info = {}
    scheduler_info["schedule_time"] = time.time()
    download_req.scheduler = json.dumps(scheduler_info)
    return download_req


def crawler(filename):
    with open(filename, 'r') as file:
        for datas in file.readlines():
            try:
                data = datas.split()
                data = map(lambda x:x.strip(), data)
                if len(data) == 0:
                    raise Exception('file error')
                if data[1] not in ['simple', "phantom"]:
                    log.error("not support download type")
                    continue
                download_req = create_download_req(data[0], data[1], data[2], data[3])
                str_req = req_to_string(download_req)
                beanstalk_client.put(beanstalk_conf['output_tube'], str_req)
                log.info('%s\t%s\t%s' % ('ok', data[0], data[1]))
            except Exception as e:
                print e

if __name__ == '__main__':
    import click
    @click.command()
    @click.option("-p", "--path", required=True, help="重抓链接列表文件路路径")
    def run(**options):
        crawler(options['path'])
    run()