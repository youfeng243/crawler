# -*- coding: utf-8 -*-
# !/usr/bin/env python
import urllib, urllib2
import xmltodict
import json
import sys


class LtpClient:
    def __init__(self, org_dedup=False):
        #self.uri_base = "http://101.201.102.37:12345/ltp"
        self.uri_base = "http://172.18.180.226:18889/ltp"
        # self.uri_base = "http://171.31.6.21:12345/ltp"
        self.data = {'s': 'text', 'x': 'n', 't': 'ner'}
        self.org_dedup = org_dedup

    def parse_xml(self, doc):
        json_result = list()
        json_obj = xmltodict.parse(doc)
        json_obj = json_obj.get('xml4nlp', dict()).get('doc', dict()).get('para', dict()).get('sent', dict())
        item_list = list()
        if type(json_obj) == list:
            item_list = json_obj
        else:
            item_list.append(json_obj)
        for item in item_list:
            item_dict = dict()
            sentence = item.get('@cont', '')
            words = item.get('word', list())
            if len(sentence) <= 0 or len(words) <= 0:
                continue
            item_dict['sentence'] = sentence
            if type(words) != list:
                words = [words]
            word_list = list()
            for word in words:
                term = word.get('@cont', '')
                pos = word.get('@pos', '')
                netag = word.get('@ne', '')
                word_item = dict()
                word_item['term'] = term
                word_item['pos'] = pos
                word_item['ne'] = netag
                word_list.append(word_item)
            item_dict['words'] = word_list
            json_result.append(item_dict)
        return json_result

    def post_request(self, text):
        self.data['s'] = text
        try:
            request = urllib2.Request(self.uri_base)
            params = urllib.urlencode(self.data)
            response = urllib2.urlopen(request, params)
            res = response.read().strip()
        except:
            res = ''
        return res

    def get_response(self, text):
        text = text.replace('\r\n', '\t')
        text = text.replace('\n', '\t')
        res = self.post_request(text)
        json_obj = self.parse_xml(res)
        seg_list = self.get_wordseg(json_obj)
        entity_kv = self.get_wordner(json_obj)
        result_json = dict()
        result_json['seg_list'] = seg_list
        result_json['entity_kv'] = entity_kv
        return result_json

    def get_wordseg(self, json_obj):
        seg_list = list()
        for item in json_obj:
            words = item.get('words', list())
            for word in words:
                term = word.get('term', '')
                seg_list.append(term)
        return seg_list

    def get_wordner(self, json_obj):
        entity_kv = dict()
        entity = ''
        person_set = set()
        org_set = set()
        for item in json_obj:
            words = item.get('words', list())
            for word in words:
                term = word.get('term', '')
                netag = word.get('ne', '')
                if netag == 'S-Nh':
                    if len(term) < 2:
                        continue
                    person_set.add(term)
                elif netag == 'B-Ni':
                    entity = term
                elif netag == 'I-Ni':
                    entity += term
                elif netag == 'E-Ni':
                    entity += term
                    org_set.add(entity)
                    entity = ''
        org_list = list(org_set)
        norm_org_list = list()
        if self.org_dedup:
            for i in range(0, len(org_list)):
                found = False
                for j in range(0, len(org_list)):
                    if i == j:
                        continue
                    elif org_list[i] in org_list[j]:
                        found = True
                if not found:
                    norm_org_list.append(org_list[i])
        else:
            norm_org_list = org_list
        if len(person_set) > 0:
            entity_kv['people'] = list(person_set)
        if len(norm_org_list):
            entity_kv['orgs'] = norm_org_list
        return entity_kv


if __name__ == '__main__':
    client = LtpClient(False)
    for line in sys.stdin:
        try:
            text = line.strip()
            json_obj = client.get_response(text)
            json_obj = json.dumps(json_obj, ensure_ascii=False)
            print json_obj
        except Exception, ex:
            print ex
            continue
