#!/usr/bin/env python
# -*- coding:utf-8 -*-
import sys
sys.path.append('..')
sys.path.append("../..")
from bdp.i_crawler.i_crawler_merge.ttypes import LinkAttr
linkattr = LinkAttr()

import pickle
with open("llink_attr") as fp:
    print pickle.load(fp)