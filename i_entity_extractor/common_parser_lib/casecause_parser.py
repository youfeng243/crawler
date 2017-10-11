# coding=utf-8
__author__ = 'luojianbo'
import json
import re, traceback
import toolsutil
import sys

reload(sys)
sys.setdefaultencoding('utf8')


class CaseCauseParser:
    def __load_char(self, conf):
        causes_regex = set()
        for line in open(conf):
            cause = line.strip()
            if len(cause) <= 0:
                continue
            cause = toolsutil.utf8_encode(cause)
            causes_regex.add(re.compile(cause))
        return list(causes_regex)

    def __init__(self, conf):
        self.cause_regex_list = self.__load_char(conf)

    # 删除子串,根据字符串长度
    def __dedup(self, cause_kv):
        norm_causes = set()
        for k, v in sorted(cause_kv.items(), lambda x, y: cmp(x[1], y[1]), reverse=True):
            found = False
            for cause in norm_causes:
                if k in cause:
                    found = True
                    break
            if not found:
                norm_causes.add(k)
        return list(norm_causes)

    # 从正文提取所有案由
    def get_case_causes(self, content):
        results = list()
        content = toolsutil.utf8_encode(content)
        cause_kv = dict()
        for cause_regex in self.cause_regex_list:
            ret = toolsutil.re_findone(cause_regex,content)
            if ret:
                cause_kv[ret] = len(ret)
        causes = self.__dedup(cause_kv)
        for k, v in sorted(cause_kv.items(), lambda x, y: cmp(x[1], y[1]), reverse=True):
            if k in causes:
                results.append(k)
        return results

    # 获取最有可能的案由
    def get_case_cause(self, content):
        cause = ''
        causes = self.get_case_causes(content)
        if len(causes) > 0:
            cause = causes[0]
        return cause


if __name__ == '__main__':
    parser = CaseCauseParser('../dict/casecause.conf')
    content = "裁判文书\t\t王生\t\t\t\t王生：本院受理原告贺喜在诉王生买卖合同纠纷一案，已审理终结。现依法向你公告送达（2016）内0123民初1166号民事判决书。自公告之日起，60日内来本院领取民事判决书，逾期则视为送达。如不服本判决，可在公告期满后15日内，向本院递交上诉状及副本，上诉于内蒙古呼和浩特市中级人民法院。逾期本判决即发生法律效力。\t\t\t\t[内蒙古]和林格尔县人民法院\t\t刊登版面：G90\t\t刊登日期：2017-05-04\t\t上传日期：2017-05-04\t\t\t\t\t\t下载打印本公告\t\t(公告样报已直接寄承办法官)"
    print parser.get_case_cause(content)

