#!/usr/bin/python
# coding=utf8
import json
import random
import os
import re

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class BusinessRuleIndustryParser:
    def __init__(self, industry_conf_path):
        self.categories = []
        list_category = True
        tmp_category = None
        sign = '+'
        tmp_words = []
        self.category_dict = {}
        with open(industry_conf_path, 'r') as f:
            for line in f: 
                try:
                    line = line.decode('utf-8')
                except:
                    pass
                if line.strip() == '':
                    list_category = False
                if list_category:
                    self.categories.append(line.replace(u'，', '').strip())
                else:
                    if line.strip() in self.categories:
                        tmp_category = line.strip()
                        self.category_dict[tmp_category] = [[], [], []]
                        sign = ''
                    elif line.strip().startswith('='):
                        continue
                    elif line.strip().startswith('+'):
                        if len(tmp_words) > 0 and sign == '+':
                            self.category_dict[tmp_category][0].append(tmp_words)
                        tmp_words = re.split(u',|，', line.strip()[1:])
                        tmp_words = [x.replace('"','').replace(u'“','').replace(u'”','').strip() for x in tmp_words]
                        tmp_words = [x for x in tmp_words if len(x) > 0]
                        sign = '+'
                    elif line.strip().startswith('-'):
                        if len(tmp_words) > 0 and sign == '+':
                            self.category_dict[tmp_category][0].append(tmp_words)
                        elif len(tmp_words) > 0 and sign == '-':
                            self.category_dict[tmp_category][1] = self.category_dict[tmp_category][1] + tmp_words
                        tmp_words = re.split(u',|，', line.strip()[1:])
                        tmp_words = [x.replace('"','').replace(u'“','').replace(u'”','').strip() for x in tmp_words]
                        tmp_words = [x for x in tmp_words if len(x) > 0]
                        sign = '-'
                    elif (not line.strip().startswith('+')) and (not line.strip().startswith('-')) and (not line.strip().startswith('@')):
                        tmp_words_ = re.split(u',|，', line.strip())
                        tmp_words_ = [x.replace('"','').replace(u'“','').replace(u'”','').strip() for x in tmp_words_]
                        tmp_words_ = [x for x in tmp_words_ if len(x) > 0]
                        tmp_words = tmp_words + tmp_words_
                    elif line.strip().startswith('@'):
                        if len(tmp_words) > 0 and sign == '+':
                            self.category_dict[tmp_category][0].append(tmp_words)
                        elif len(tmp_words) > 0 and sign == '-':
                            self.category_dict[tmp_category][1] = self.category_dict[tmp_category][1] + tmp_words
                        tmp_words = re.split(u',|，', line.strip()[1:])
                        tmp_words = [x.replace('"','').replace(u'“','').replace(u'”','').strip() for x in tmp_words]
                        tmp_words = [x for x in tmp_words if len(x) > 0]
                        self.category_dict[tmp_category][2] = self.category_dict[tmp_category][2] + tmp_words
                        sign = '@'
        self.gram_sep = u',|，|。|、|\s|;|；|/|\t|\n|\r'
        #print '\t'.join(categories)
        for x in self.categories:
            self.category_dict[x][0] = [set(t) for t in self.category_dict[x][0]]
            self.category_dict[x][1] = set(self.category_dict[x][1])
            self.category_dict[x][2] = set(self.category_dict[x][2])
            if x in self.category_dict[x][2]:
                self.category_dict[x][2].remove(x)

        self.manufacturing = [
            u'农副食品加工业',
            u'食品制造业',
            u'酒、饮料和精制茶制造业',
            u'烟草制品业',
            u'纺织业',
            u'纺织服装、服饰业',
            u'皮革、毛皮、羽毛及其制品和制鞋业',
            u'木材加工和木、竹、藤、棕、草制品业',
            u'家具制造业',
            u'造纸和纸制品业',
            u'文教、工美、体育和娱乐用品制造业',
            u'石油加工、炼焦和核燃料加工业',
            u'化学原料和化学制品制造业',
            u'医药制造业',
            u'化学纤维制造业',
            u'橡胶和塑料制品业',
            u'非金属矿物制品业',
            u'黑色金属冶炼和压延加工业',
            u'有色金属冶炼和压延加工业',
            u'金属制品业',
            u'专用设备制造业',
            u'电气机械和器材制造业',
            u'计算机、通信和其他电子设备制造业',
            u'仪器仪表制造业',
            u'其他制造业', ]

    def similar(self, a, b):
        return len(a & b) / ((len(a)+0.0001) * (len(b)+0.0001))

    def ngram(self, sentence, n=2):
        sentence = sentence.replace('(', u'（').replace(')', u'）')
        if u'（' in sentence:
            if sentence.count(u'（')==sentence.count(u'）'):
                sentence = re.sub(u'（[^>]*?）', u'', sentence)
            else:
                sentence.replace(u'（', '').replace(u'）', '')
        snipplets = re.split(self.gram_sep, sentence)
        if len(snipplets) == 0:
            return []
        elif len(snipplets) == 1:
            if len(snipplets[0]) == 0:
                return []
            elif len(snipplets[0]) == 1:
                return snipplets
            else:
                return [snipplets[0][i:i+n] for i in xrange(len(snipplets[0])-n+1)]
        else:
            n_grams = []
            for snipplet in snipplets:
                n_grams += self.ngram(snipplet, n)
            return n_grams

    def upto_ngram(self, sentence, n=4, not1=False):
        if not1:
            gram = []
        else:
            gram = list(set(sentence))
        for i in xrange(2, n+1):
            gram += self.ngram(sentence, i)
        #print '/'.join(list(gram))
        return set(gram)

    def set_list(self, candidate_classes):
        predict_classes = []
        for x in candidate_classes:
            if (x not in predict_classes and x in self.categories):
                predict_classes.append(x)
        return predict_classes

    def predict(self, business_scopes, company_name, business_scope_c):
        is_sale = False
        business_grams = self.upto_ngram(company_name)
        for category in [u'批发业', u'零售业']:
            cond1 = [len(x & business_grams) for x in self.category_dict[category][0]]
            cond1 = (len(cond1)>0) and (all(cond1))
            cond2 = len(business_grams & self.category_dict[category][1])
            if cond1 > 0 and cond2 == 0:
                return [category, '#']

        candidate_classes_name = []
        #company_names = re.split(u'公司|研究院|火车站|医院', company_name)[::-1]
        company_names = []
        start = 0
        for i in xrange(1,len(company_name)+1):
            for postfix in [u'公司', u'研究院', u'火车站', u'医院', u'集团', u'企业']:
                if company_name[start:i].endswith(postfix) or i == len(company_name):
                    company_names.append(company_name[start:i])
                    start = i
                    break
        company_names = company_names[::-1]
        for cn in company_names:
            business_grams = self.upto_ngram(cn, n=4, not1=True)

            for category in [x for x in self.categories if x not in self.manufacturing]+self.manufacturing:
                if category in [u'批发业', u'零售业']:
                    continue
                mustin = self.category_dict[category][0]
                if category in self.manufacturing and len(mustin) > 1:
                    mustin[0] = set(list(mustin[0]) + [u'厂', u'技术', u'科技', u'公司'])
                elif category not in self.manufacturing and len(mustin) > 1 and category not in [u'汽车制造业', u'铁路、船舶、航空航天和其他运输设备制造业',u'通用设备制造业', u'印刷和记录媒介复制业', u'煤炭开采和洗选业', u'石油和天然气开采业', u'黑色金属矿采选业', u'有色金属矿采选业', u'非金属矿采选业', u'开采辅助活动', u'其他采矿业', u'电力、热力生产和供应业', u'燃气生产和供应业', u'水的生产和供应业', u'房屋建筑业', u'土木工程建筑业', u'建筑安装业', u'建筑装饰和其他建筑业', u'批发业', u'铁路运输业', u'水上运输业', u'管道运输业', u'装卸搬运和运输代理业', u'仓储业', u'保险业', u'房地产业', u'研究和试验发展', u'科技推广和应用服务业', u'机动车、电子产品和日用产品修理业', u'其他服务业', u'广播、电视、电影和影视录音制作业', u'文化艺术业', u'公共设施管理业', u'生态保护和环境治理业']:
                    mustin[0] = set(list(mustin[0]) + [u'服务', u'公司'])
                cond1 = [len(x & business_grams) for x in mustin]
                cond1 = (len(cond1)>0) and (all(cond1))
                cond2 = len(business_grams & self.category_dict[category][1])
                if cond1 > 0 and cond2 == 0:
                    candidate_classes_name.append(category)

        candidate_classes = []
        for business_scope in business_scopes:
            if len(business_scope) <= 1:
                continue
            business_grams = self.upto_ngram(business_scope)
            scores = []
            category_set = []
            for category in self.categories:
                cond1 = [len(x & business_grams) for x in self.category_dict[category][0]]
                cond1 = (len(cond1)>0) and (all(cond1))
                cond2 = len(business_grams & self.category_dict[category][1])
                if cond1 > 0 and cond2 == 0:
                    category_set.append(category)

            if len(category_set) == 0 and u'服务' not in company_name and u'实业' not in company_name and u'销售' not in business_scope_c and u'零售' not in business_scope_c and u'批发' not in business_scope_c:
                business_grams.add(u'生产')
                for category in self.categories:
                    cond1 = [len(x & business_grams) for x in self.category_dict[category][0]]
                    cond1 = (len(cond1)>0) and (all(cond1))
                    cond2 = len(business_grams & self.category_dict[category][1])
                    if cond1 > 0 and cond2 == 0:
                        category_set.append(category)
            candidate_classes_ = category_set
            candidate_classes = candidate_classes + candidate_classes_

        candidate_classes = self.set_list(candidate_classes_name) +[u'#']+ self.set_list(candidate_classes)

        if len(candidate_classes) == 1 and u'实业' in company_name:
            candidate_classes = [u'批发业']

        predict_classes = candidate_classes
        return predict_classes

    def bus_predict(self, company_name, business_scope) :
        if not company_name and not business_scope:
            return []
        business_scope_c = business_scope
        business_scope = re.sub(u'([^>]*?)', '', business_scope)
        business_scope = re.sub(u'（[^>]*?）', '', business_scope)
        business_scope = re.sub(u'【[^>]*?】', '', business_scope)
        business_scope = re.sub(u'《[^>]*?》', '', business_scope)
        business_scope = business_scope.replace(u'许可经营项目', '').replace(u'一般经营项目', '').replace(u'经营项目', '').replace(
            u'*', '')
        business_scope = business_scope.replace(u'，', '；').replace(u',', '；')
        if u'；' not in business_scope and u';' not in business_scope and u'，' not in business_scope and u',' not in business_scope:
            business_scope = business_scope.replace(u'、', '；')
        business_scopes = re.split(u'；|;|。|\n', business_scope)
        classes = self.predict(business_scopes, company_name, business_scope_c)
        return classes
        # print company_name + '\t' + ' '.join(classes)


if __name__ == '__main__':
    rules_classfier = INDUSTRY('../dict_data/industry.conf') #/grail
    for line in sys.stdin:
        if len(line) < 10 :
            continue
        try:
            try:
                line = line.decode('utf-8')
            except:
                pass
            try:
                company_name = line.split("\t")[0]
                business_scope = line.split('\t')[1]
            except:
                continue
            classes = []
            business_scope_c = business_scope
            business_scope = re.sub(u'([^>]*?)', '', business_scope)
            business_scope = re.sub(u'（[^>]*?）', '', business_scope)
            business_scope = re.sub(u'【[^>]*?】', '', business_scope)
            business_scope = re.sub(u'《[^>]*?》', '', business_scope)
            business_scope = business_scope.replace(u'许可经营项目', '').replace(u'一般经营项目', '').replace(u'经营项目', '').replace(u'*', '')
            business_scope = business_scope.replace(u'，', '；').replace(u',', '；')
            if u'；' not in business_scope and u';' not in business_scope and u'，' not in business_scope and u',' not in business_scope:
                business_scope = business_scope.replace(u'、', '；')
            business_scopes = re.split(u'；|;|。|\n', business_scope)
            company_type = ''
            classes = rules_classfier.predict(business_scopes, company_name,  business_scope_c)
            # print company_name + '\t' + '/'.join(classes) + '\t' + business_scope
            print company_name + '\t' + ' '.join(classes)
        except:
            continue

