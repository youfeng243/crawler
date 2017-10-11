#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

sys.path.append('..')

sys.path.append('../..')
from i_downloader.test.prourl import province as pro
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from bdp.i_crawler.i_downloader import DownloadService
from bdp.i_crawler.i_downloader.ttypes import DownLoadReq
from bdp.i_crawler.i_downloader.ttypes import SessionCommit
from bdp.i_crawler.i_downloader.ttypes import Proxy


#获取客户端
def getclient():
    try:
        transport = TSocket.TSocket('127.0.0.1', 12200)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = DownloadService.Client(protocol)
        transport.open()
        return client,transport
    except Exception as e:
        print e.message
#关闭transport
def closetransport(transport):
    try:
        transport.close()
    except Exception as e:
        print e.message
a='''hz:hz@115.28.36.11:7777
hz:hz@115.28.151.173:7777
hz:hz@114.215.153.166:7777
hz:hz@115.28.19.87:7777
hz:hz@114.215.146.101:7777
hz:hz@115.28.149.130:7777
hz:hz@115.28.144.145:7777
hz:hz@115.28.81.76:7777
hz:hz@115.28.56.239:7777
hz:hz@115.28.56.141:7777
hz:hz@115.28.170.179:7777
hz:hz@115.28.38.153:7777'''
#获取去头
def gethead():
    return {
        'Cookie': '_user_identify_=37dd9784-9545-3312-83ec-06c6ca74d9c4; JSESSIONID=aaa55rwMt-k2gevTIjTGv; Hm_lvt_37854ae85b75cf05012d4d71db2a355a=1477897607,1477965597,1478162497,1478226222; Hm_lpvt_37854ae85b75cf05012d4d71db2a355a=1478313130',
        'Referer': 'http://www.innotree.cn/allProjects',
    }
#获取代理
def getproxy():
    host='115.28.81.76'
    port=7777
    user= 'hz'
    password= 'hz'
    type='http'
    return Proxy(type=type,host=host,port=port,user=user,password=password)
def get_searchname():
    with open('company', 'rb') as f:
        for i in f.readlines():
            yield i[:-1]
#得到请求

def getreq(proa='',file=''):
    province=pro.get(proa)
    searchname=province.get('searchname')
    url=province.get('url')
    req = DownLoadReq(url=url, method='get', download_type='simple')
    req.use_proxy=True
    req.scheduler='sdfsdfsdds'
    req.parse_extends='sdfsdfsdf'
    req.data_extends='sdfsdfsdfsdfdsfsdfs'
    # req.http_header=gethead()
    req.proxy=getproxy()
    req.retry_times=1
    req.time_out=30
    kw=province.get('kw')
    post_data=province.get('post_data')
    if kw:
        session=SessionCommit(**kw)
        req.session_commit = session
    if province.get('need_identifying'):
        session.need_identifying=province.get('need_identifying')

    req.post_data=post_data
    for i in get_searchname():
        if searchname:
            req.post_data[searchname]=i
        yield  req
def control():
    client,transport=getclient()
    for i in getreq(proa='tianyancha', file='company'):
        try:
            res=client.download(i)
            print res.url
            print res.content
        except Exception as e:
            print e.message
    closetransport(transport)
def testone():
    client,transport=getclient()
    for i in getreq(proa='gudong', file='company'):
        try:
            res=client.download(i)
            print res.url
            print res.content

            sys.exit(0)
        except Exception as e:
            print e.message
    closetransport(transport)
def commit_task():
    client, transport = getclient()
    while True:
        for i in getreq(proa='gudong', file='company'):
            try:

                res=client.commit_download_task(i)
                print res.status
                print res
                sys.exit(1)
            except Exception as e:
                print e.message
    closetransport(transport)

#主函数
if __name__=='__main__':
    testone()