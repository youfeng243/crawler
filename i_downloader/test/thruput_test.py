#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import time

from thrift.protocol.TBinaryProtocol import TBinaryProtocol as TBinaryServerProtocol
from thrift.transport.TTransport import TMemoryBuffer

from i_util.pybeanstalk import PyBeanstalk

sys.path.append('..')

sys.path.append('../..')
from i_downloader.test.test_url12 import test_modul as pro
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from bdp.i_crawler.i_downloader import DownloadService
from bdp.i_crawler.i_downloader.ttypes import DownLoadReq
from bdp.i_crawler.i_downloader.ttypes import Proxy


#获取客户端
def getclient():
    try:
        transport = TSocket.TSocket('localhost', 12200)
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
#转换为str
def to_string(page_info):
    str_page_info = None
    try:
        tMemory_b = TMemoryBuffer()
        tBinaryProtocol_b = TBinaryServerProtocol(tMemory_b)
        page_info.write(tBinaryProtocol_b)
        str_page_info = tMemory_b.getvalue()
    except EOFError, e:
        pass
    return str_page_info
#获取去头
def gethead():
    return {'Connection': 'close','User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36'}
#获取代理
def getproxy():
    host='120.41.252.211'
    port=8088
    user= 'kzp2'
    password= 'kzp2'
    return Proxy(host=host,port=port,user=user,password=password)
def get_searchname():
    with open('company', 'rb') as f:
        for i in f.readlines():
            yield i[:-1]
#得到请求

def getreq(proa='',file=''):
    province=pro.get(proa)
    url=province.get('url')
    req = DownLoadReq(url=url, method='get', download_type='simple',use_proxy=True)
    post_data=province.get('post_data')
    req.post_data=post_data
    req.retry_times=1
    req.time_out=30
    return req
def control():
    client,transport=getclient()
    cnt=0
    start=time.time()
    for i in pro.keys():
        try:
            req=getreq(proa=i)
            res=client.download(req=req)
            cnt+=1
            print res.content_type
            print res.elapsed
        except Exception as e:
            print e.message
    print cnt
    print ('usetime:{}'.format(time.time()-start))
    closetransport(transport)
def commit_task():
    client,transport=getclient()
    cnt=0
    start=time.time()
    for i in pro.keys():
        try:
            req=getreq(proa=i)
            client.commit_download_task(req=req)
            cnt+=1
            print cnt
        except Exception as e:
            print e.message
    print cnt
    print ('usetime:{}'.format(time.time()-start))
    closetransport(transport)
def thrput_task():
    input_tube='download_req'
    beanstalk = PyBeanstalk('101.201.102.37', 11300)
    client,transport=getclient()
    cnt=0
    start=time.time()
    suma=100
    while suma:
        suma-=1
        for i in pro.keys():
            try:
                req=getreq(proa=i)
                str_page_info = to_string(req)
                beanstalk.put(input_tube, str_page_info)
                cnt+=1
            except Exception as e:
                print e.message
        print ('usetime:{}'.format(time.time()-start))



    closetransport(transport)
#主函数
if __name__=='__main__':
    thrput_task()