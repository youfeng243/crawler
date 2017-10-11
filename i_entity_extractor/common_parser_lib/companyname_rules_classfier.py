#!/usr/bin/python
# coding=utf8
import json
import random
import os
import re

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class CompanyNameIndustryParser:
    def __init__(self, industry_conf_path, postfix_deterministic_path, postfix_deterministic2_path):
        self.gram_sep = u',|，|。|、|\s|;|；|/|\t|\n|\r'
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
        for x in self.categories:
            self.category_dict[x][0] = [set(t) for t in self.category_dict[x][0]]
            self.category_dict[x][1] = set(self.category_dict[x][1])
            self.category_dict[x][2] = set(self.category_dict[x][2])
            if x in self.category_dict[x][2]:
                self.category_dict[x][2].remove(x)

        self.industry_postfix = {}
        tmp_category = None
        with open(postfix_deterministic_path, 'r') as f:
            for line in f: 
                try:
                    line = line.decode('utf-8')
                except:
                    pass
                line = line.strip()
                if line in self.category_dict:
                    tmp_category = line
                    self.industry_postfix[tmp_category] = [[], [], []]
                if not line.startswith('@') and (u'：' in line or u':' in line):
                    line = re.split(u':|：', line)[1]
                    line = re.split(u',|，', line)
                    line = [x.strip() for x in line if len(x.strip())>0]
                    for x in line:
                        if '-' not in x and '+' not in x:
                            self.industry_postfix[tmp_category][0].append(x)
                        elif x.count('-') == 1:
                            self.industry_postfix[tmp_category][1].append(x.split('-'))
                        elif x.count('+') == 1:
                            self.industry_postfix[tmp_category][2].append(x.split('+'))

            for category in self.industry_postfix:
                #print category, '/'.join(self.industry_postfix[category][0])
                self.industry_postfix[category][0] = set(self.industry_postfix[category][0])

        self.kv = {}
        with open(postfix_deterministic2_path, 'r') as f:
            for line in f:
                try:
                    line = line.decode('utf-8')
                except:
                    pass
                line = line.strip()
                line = line.split()
                if len(line) == 2:
                    self.kv[line[0].strip()] = line[1].strip()



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
                #print snipplets[0], xrange(len(snipplets[0])-1)
                #print n,'*'.join([snipplets[0][i:i+n] for i in xrange(len(snipplets[0])-1)])
                return [snipplets[0][i:i+n] for i in xrange(len(snipplets[0])-n+1)]
        else:
            n_grams = []
            for snipplet in snipplets:
                n_grams += self.ngram(snipplet, n)
            return n_grams

    def not_produce(self, company_name):
        if not company_name.endswith(u'厂') and u'生产' not in company_name and u'制造' not in company_name and u'加工' not in company_name:
            return True
        else:
            return False

    def predict(self, company_name):

        if len(company_name) < 6 :
            return ''
        if u'供应站' in company_name:
            company_name = re.sub(u'供应站[^>]*?站', u'供应站', company_name)
        if u'营业部' in company_name:
            company_name = re.sub(u'营业部[^>]*?部', u'营业部', company_name)
        company_name = re.sub('[\w|.]+', '', company_name)
        classes = []
        postfixes = set([company_name[-1:], company_name[-2:], company_name[-3:], company_name[-4:], company_name[-5:], company_name[-6:]])
        #print '/'.join(list(postfixes))
        for category in self.industry_postfix:
            #print '*', category, '/'.join(self.industry_postfix[category][0])
            if len(self.industry_postfix[category][0] & postfixes)>0:
                classes.append(category)
            for x in self.industry_postfix[category][1]:
                if company_name.endswith(x[0]) and all([t.strip() not in company_name for t in x[1].split('/')]):
                    classes.append(category)
            for x in self.industry_postfix[category][2]:
                if company_name.endswith(x[0]) and x[1] in company_name:
                    classes.append(category)

        if company_name[-5:] in self.kv:
            classes.append(self.kv[company_name[-5:]])
        elif company_name[-4:] in self.kv:
            classes.append(self.kv[company_name[-4:]])
        elif company_name[-3:] in self.kv:
            classes.append(self.kv[company_name[-3:]])
        elif company_name[-2:] in self.kv:
            classes.append(self.kv[company_name[-2:]])

        if len(classes) == 0 and company_name.endswith(u'公司'):
            return ''
        if len(classes) == 0 and u'公司' in company_name:
            company_name = company_name.split(u'公司')[1]
        business_1grams = list(set(company_name))
        business_2grams = self.ngram(company_name)
        business_3grams = self.ngram(company_name, 3)
        business_4grams = self.ngram(company_name, 4)
        business_grams = set(business_1grams + business_2grams + business_3grams + business_4grams)

        if len(classes) == 0 and self.not_produce(company_name):
            inters = []
            indexing =                      [   0,       0,       0,       0,        0,         0,        0,          0,      0,                  0,                    0,       0,        0]
            for i,category in zip(indexing, [u'农业', u'畜牧业', u'渔业', u'批发业', u'零售业', u'仓储业', u'邮政业', u'住宿业', u'餐饮业', u'机动车、电子产品和日用产品修理业', u'教育', u'卫生', u'居民服务业']):
                #inters = inters + list(self.category_dict[category][0][i] & business_grams if len(self.category_dict[category][0])>0 else [])
                if len(self.category_dict[category][0])>0 and len(self.category_dict[category][0][i] & business_grams)>0:
                    inters.append(category)

            #return list(set(inters))
            classes = inters

            if len(classes) == 0 and (company_name.endswith(u'店') or company_name.endswith(u'门市部') or company_name.endswith(u'门市')):
                classes = [u'零售业']
        if len(classes) <= 0 :
            return ''
        return list(set(classes))[0]


if __name__ == '__main__':
    industry_classifier = INDUSTRY('../dict_data/industry.conf', '../dict_data/postfix_deterministic.conf',
                                   '../dict_data/postfix_deterministic2.conf')  # /grail
    # for line in sys.stdin:
    #     if len(line) < 10:
    #         continue
    #     try:
    #         line = line.decode('utf-8')
    #         fields = line.split()
    #         company_name = fields[0].strip()
    #         line = company_name
    #     except:
    #         pass
    #     #print 'industry:',industry_classifier.predict(line.strip())
    #     print line.strip(), '\t', '/'.join(industry_classifier.predict(line.strip()))

    print industry_classifier.predict("ktv")




