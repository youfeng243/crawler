# coding=utf-8

import sys
import os

import traceback

sys.path.append('..')
reload(sys)
import re
from datetime import datetime
from province_parser import ProvinceParser


pat_time = re.compile('\d+:\d+:\d+')
pat_chinese = re.compile(u'([\u4e00-\u9fa5a-zA-Z0-9]+)')
pat_NA = re.compile('[N\/A]')

#匹配暂无,暂无数据
pat_non_num = re.compile(u"[\u6682\u000d\u000a][^\x00-\xff]+")


def regex_remove_time(var_str):
    return re.sub(pat_time, ' ', var_str)


def file_to_dict(path):
    file = open(path)
    dict = {}
    for line in file.readlines():
        line = line.split('\t')
        key = unicode(line[0].strip())
        value = unicode(line[1].strip())
        dict[key] = value
    return dict


# 切分省市或者行业


def regex_chinese(var_str):
    return re.findall(pat_chinese, unicode(var_str))


lst_cfg_area = [u'province', u'city', u'district']
current_path = os.getcwd()
basic_path   = current_path[:current_path.rfind('i_entity_extractor')]
province_conf_path = basic_path + 'i_entity_extractor/dict/province_city.conf'
region_city_path = basic_path + 'i_entity_extractor/dict/region_city.conf'
phone_city = basic_path + 'i_entity_extractor/dict/phonenum_city.conf'
city_path = basic_path + 'i_entity_extractor/dict/city.conf'
province_city = file_to_dict(province_conf_path)
province_parser = ProvinceParser(province_conf_path, phone_city, region_city_path, city_path)


def province_city_district(lst_location):
    address = {u'province': '', u'city': '', u'district': ''}
    if len(lst_location) == 3:
        for i in range(3):
            address[lst_cfg_area[i]] = lst_location[i]
        return address
    for item in lst_location:
        address[u'province'] = province_city.get(re.sub(u'市|省', '', item), '')
        if u'市' in item or item in [u'上海', u'北京', u'天津', u'重庆']:
            address[u'city'] = item
        else:
            c = re.sub(u'区|县', '', item)
            if c in province_city.keys() and c != province_city.get(c):
                address[u'city'] = item
        if u'区' in item or u'县' in item:
            address[u'district'] = item
    return address


def map_region(lst_location):
    region = {'province': '', 'city': '', 'district': ''}
    address = ''.join(lst_location)
    province = unicode(province_parser.get_province(address, 1))
    city = unicode(province_parser.get_region(address, 1))
    if province in city and province != city:
        city = city.replace(province, '')
    region['province'] = province
    region['city'] = city
    for lst in lst_location:
        if city not in lst and province not in lst:
            region['district'] = lst
            break
    return region


def etl_sub_str(var_str, start, length):
    if len(var_str) <= start + length:
        return var_str
    return var_str[start: start + length]


def str2datetime(date_string, format):
    if date_string == None or date_string == '':
        return None
    try:
        return datetime.strptime(date_string, format)
    except Exception:
        return None


def regex_remove_na(var_str):
    return re.sub(pat_NA, '', var_str)


def regex_remove_non_num(var_str):
    return re.sub(pat_non_num, '', var_str)



