#!/usr/bin/env python
# -*- coding:utf-8 -*-

import re
import os
import sys
import toolsutil

reload(sys)
sys.setdefaultencoding('utf8')

class LitigantUtil:
    def __init__(self, stopword_conf):
        self.stopwords = self.load_stopword(stopword_conf)

    def load_stopword(self, conf):
        priority_kv = dict()
        terms = list()
        for line in open(conf):
            line = line.strip()
            if len(line) <= 0:
                continue
            term = toolsutil.utf8_encode(line)
            priority_kv[term] = len(term)
        for k, v in sorted(priority_kv.items(), lambda x, y: cmp(x[1], y[1]), reverse=True):
            terms.append(toolsutil.utf8_encode(k))
        return terms

    def norm_litigant(self, litigants):
        norm_litigants = list()
        for litigant in litigants:
            litigant = litigant.replace('(', '（').replace(')', '）')
            litigant = toolsutil.utf8_encode(litigant)
            if '代理' in litigant or '委托' in litigant or '代表' in litigant:
                continue
            while True:
                found = False
                for word in self.stopwords:
                    if litigant.startswith(word):
                        litigant = litigant.replace(word, '')
                        found = True
                if not found:
                    break
            if litigant.endswith('）'):
                idx = litigant.rfind('（')
                if idx != -1:
                    litigant = litigant[:idx]
            if litigant.startswith('（'):
                idx = litigant.find('）')
                if idx != -1:
                    litigant = litigant[idx + len('）'):]
            norm_litigants.append(litigant)
        return norm_litigants


if __name__ == '__main__':
    litigant_tool = LitigantUtil('../conf/litigant.conf')
    litigants = [u'被告腾讯有限公司', u'百度有限公司', u'（其他东西）阿里巴巴有限公司', '申请被执行人马化腾']
    norm_litigants = litigant_tool.norm_litigant(litigants)
    print ','.join(norm_litigants)
