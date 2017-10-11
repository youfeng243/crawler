# coding=utf-8

import sys
import toolsutil

reload(sys)
sys.setdefaultencoding('utf8')
import esm
import jieba
import json
import requests


class ProvinceParser:
    def __init__(self, province_conf, phonenum_conf, region_conf, city_conf):
        self.strips = []
        self.province_index = esm.Index()

        self.region_index = esm.Index()
        self.provinces, self.province_kv = self._load_kv(province_conf)
        self.phone_city_map = self._load_phone_conf(phonenum_conf)

        self.region_city_map = {}
        region_list = open(region_conf).read().split('\n')[:-1]
        self.city_list = open(city_conf).read().split('\n')[:-1]
        self.city_set  = set(self.city_list)
        for item in region_list:
            if item:
                tmp_list = item.split(',')
                if len(tmp_list) == 2:
                    city = tmp_list[0]
                    self.region_index.enter(city)
                    self.region_city_map[city] = tmp_list[1].strip()

        self.region_index.fix()


    def _load_kv(self, file):
        my_kv = dict()
        my_set = set()
        for line in open(file):
            fields = line.strip().split('\t')
            if len(fields) != 2:
                continue
            city = toolsutil.utf8_encode(fields[0].strip())
            province = toolsutil.utf8_encode(fields[1].strip())
            self.province_index.enter(city)
            my_kv[city] = province
            my_set.add(province)

        self.province_index.fix()

        return my_set, my_kv


    def _load_phone_conf(self, phonenum_conf):
        '''加载电话号码对应归属地'''
        phone_city_map = {}
        phonenum_list = open(phonenum_conf).read().split('\n')[:-1]
        for item in phonenum_list:
            tmp_list = item.split('\t')
            if len(tmp_list) == 2:
                phone_city_map[str(tmp_list[0])] = tmp_list[1]

        return phone_city_map


    def get_province(self, content, isaddress=False):
        if isaddress:
            return self.get_province_from_address(content)

        content_str = toolsutil.utf8_encode(content)
        province = self.province_kv.get(content_str, None)
        if province:
            return province
        seg_list = jieba.cut(content, cut_all=True)
        province_freq = dict()
        for seg in seg_list:
            seg = str(seg)
            seg = toolsutil.utf8_encode(seg)
            if seg in self.provinces:
                return seg
            else:
                province = self.province_kv.get(seg, '')
                if province != '':
                    province_freq[province] = province_freq.get(province, 0) + 1
        for k, v in sorted(province_freq.items(), lambda x, y: cmp(x[1], y[1]), reverse=True):
            return k
        return ''

    def get_province_from_address(self, address):
        content_ret = self.province_index.query(address)
        if content_ret:
            for ret in content_ret:
                return self.province_kv.get(ret[1])
        return ''


    def get_region(self, content, isaddress=False):
        '''获取行政区域'''
        if isaddress:
            return self.get_region_from_address(content)

        content_str = toolsutil.utf8_encode(content)
        city = self.region_city_map.get(content_str, None)
        if city:
            return city

        seg_list = jieba.cut(content, cut_all=True)
        city_freq = dict()
        for seg in seg_list:
            seg = str(seg)
            seg = toolsutil.utf8_encode(seg)
            if seg in self.city_set:
                return self.region_city_map.get(seg)
            else:
                province = self.region_city_map.get(seg, '')
                if province != '':
                    city_freq[province] = city_freq.get(province, 0) + 1
        for k, v in sorted(city_freq.items(), lambda x, y: cmp(x[1], y[1]), reverse=True):
            return k
        return ''

    def get_region_from_address(self, address):
        content_ret = self.region_index.query(address)
        if content_ret:
            for ret in content_ret:
                return self.region_city_map.get(ret[1])
        return ''

    def get_region_from_phonenum(self, phonenum):
        '''从手机号码获取行政区域'''

        #1 判断号码是手机号码还是电话号码
        phonenum = str(phonenum)
        phonenum = phonenum.replace('-','')
        region = ''
        ret1 = toolsutil.re_find_one('^1[358]\d{9}$|^147\d{8}', phonenum)
        text = ''
        if ret1:
            #1 手机号码
            url = "https://www.iteblog.com/api/mobile.php?mobile=%s" % phonenum
            try:
                resp = requests.get(url, timeout=30)
                text = resp.text
                data = json.loads(text)

                if isinstance(data, dict):
                    province = data.get("province", "")
                    city = data.get("city", "")
                    region = province + city
                    return region
                else:
                    return ""
            except Exception as e:
                return ""

        #2 电话号码
        ret2 = toolsutil.re_find_one('^0\d{2,3}$', phonenum)
        if ret2:
            region = self.phone_city_map.get(ret2)
        return region




if __name__ == '__main__':
    province_parser = ProvinceParser('../dict/province_city.conf','../dict/phonenum_city.conf', '../dict/region_city.conf','../dict/city.conf')
    text = "桃山林区基层法院"
    province = province_parser.get_province(text,1)
    city = province_parser.get_region(text, 1)

    print " province:",province,"city:",city

    #print province_parser.get_region_from_phonenum('18320790914')
