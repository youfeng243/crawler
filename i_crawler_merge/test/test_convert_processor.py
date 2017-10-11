#!/usr/bin/env python
# -*- coding:utf-8 -*-
import getopt
import pickle
import sys

import pytoml

from i_crawler_merge.server import get_mongo_conf

sys.path.append("..")
#from i_util.logs import LogHandler
from convert_processor import ConvertProccessor
import logging
def get_conf(file_path='../crawler_merge.toml'):
    try:
        opt, args = getopt.getopt(sys.argv[1:], 'f:', ['help'])
        for name, value in opt:
            if name == "-f":
                file_path = value
            elif name in ("-h", "--help"):
                print ''
                sys.exit()
            else:
                assert False, "unhandled option"
        with open(file_path, 'rb') as config:
            config = pytoml.load(config)
        log_name = config['server'].get('name') + str(config['server'].get('port'))
        config['log'] = logging#LogHandler(log_name)
        return config
    except:
        sys.exit()
conf = get_conf()
mongo_conf = get_mongo_conf(conf)
print mongo_conf
obj = None
with open("parserinfo.data", "rb") as fp:
    obj = pickle.load(fp)

convert = ConvertProccessor(conf.get('log'), mongo_conf)
convert.start_convert(obj)