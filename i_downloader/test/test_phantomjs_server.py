#!/usr/bin/env python
# coding:utf-8
import sys
sys.path.append("..")
from i_downloader.downloader.phantomjs_server import PhantomjsServer
import logging
conf = {
    "log":logging,
    "phantomjs_path":"/Users/haizhi/git/phantomjs/bin/phantomjs",
    }
def test_restart():
    conf['phantomjsrestart'] = 1
    test_conf = dict(conf)
    phantomjs_server = PhantomjsServer(conf)
    import time
    time.sleep(10)
    phantomjs_server.stop()
test_restart()