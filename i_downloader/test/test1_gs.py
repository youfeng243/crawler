#!/usr/bin/env python
# -*- coding: utf-8 -*-
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
import signal, time, sys, multiprocessing, traceback, os, threading
sys.path.append('..')
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from bdp.i_crawler.i_downloader import DownloadService
from bdp.i_crawler.i_downloader.ttypes import DownLoadReq
from bdp.i_crawler.i_downloader.ttypes import SessionCommit
from bdp.i_crawler.i_downloader.ttypes import Proxy
import threading
import random
from i_downloader.conf import log as logger
try:
    transport = TSocket.TSocket('localhost', 12200)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = DownloadService.Client(protocol)
    transport.open()
    company=[]
    with open('company','rb') as f:
        for i in f.readlines():
            company.append(i)
    http_header={'Connection': 'close','User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36'}
    host='120.41.252.211'
    port=8088
    user= 'kzp2'
    password= 'kzp2'
    proxy=Proxy(host=host,port=port,user=user,password=password)
    # kw={
    #     'refer_url':'http://wsgs.fjaic.gov.cn/creditpub/home',
    #     'session_msg':{
    #            'session.token':'session.token'
    #     },
    # }
    # url = 'http://wsgs.fjaic.gov.cn/creditpub/search/ent_info_list'
    # post_data = {
    #     'searchType': '1',
    #     'captcha': ''}
    kw={
        'refer_url':'http://www.ahcredit.gov.cn/search.jspx',
        'check_body':u"/businessPublicity.jspx?id=",
        'identifying_code_url':'http://www.ahcredit.gov.cn/validateCode.jspx',
        'session_msg':{

        },
    }
    url = 'http://www.ahcredit.gov.cn/searchList.jspx'
    post_data={  'checkNo': '' ,}
    session_commit=SessionCommit(**kw)
    req = DownLoadReq(url=url,method='get')
    req.download_type='simple'
    #req.proxy=proxy
    req.priority=0
    req.time_out=30
    req.http_header=http_header
    req.retry_times=1
    req.session_commit=session_commit
    for i in range(100):
        cnt=0
        try:
            post_data['entName'] = company[random.randint(1,len(company)-1)]
            req.post_data = post_data
            res=client.download(req)
            print res.content

        except Exception as e:
            cnt+=1
            logger.error('test error %s'%e.message)
        if cnt%10==0:
            print cnt
    transport.close()
#iutl改了log   i——config改了mysql密码,,phantomjs改了log
except Thrift.TException, tx:
    print '%s' % (tx.message)