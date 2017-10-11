# coding=utf-8
# 工商信息实体解析
import sys
import os
import re

sys.path.append('..')
sys.path.append('../..')
import json
import copy

from i_entity_extractor.common_parser_lib import toolsutil
from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
import gsxx_conf
import string
import time


class GsxxExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.config_path = self.basic_path + "i_entity_extractor/extractors/gsxx/mapping.conf"
        self.mapping_conf = self.read_config()
        self.period_regex = re.compile(u"(\d{4}.\d{1,2}.\d{1,2}).*?(\d{4}.\d{1,2}.\d{1,2})")
        self.period_regex2 = re.compile(u"\d{4}.\d{1,2}.\d{1,2}")

        self.punctuation_list = ['+', '！', '。', '，', '？', '&', '＃', '@', '、', '～', '＊', '……', '（', '）', '；']
        for special_str in string.punctuation:
            self.punctuation_list.append(special_str)
        self.extract_re = re.compile(
            u'^(.{0,5}名称|企业基本信息：名称|企业\(机构\)名称|名称序号: 企业名称|变更前内容|变更后内容|【变更前内容|【变更后内容)\s{0,1}(:|：|】|\s) {0,3}([^;\:\.]+)')

    def read_config(self):
        mapping_conf = {}
        file = open(self.config_path)
        for line in file:
            pars = line.strip().split(',')
            if len(pars) >= 2:
                key = pars[0].encode("utf8")
                value = pars[1].encode("utf8")
                mapping_conf[key] = value
        return mapping_conf

    def format_extract_data(self, extract_data, topic_id):
        '''实体解析抽取数据'''

        is_baseinfo_page = False

        entity_data = {}
        if extract_data:
            entity_data = copy.deepcopy(extract_data)

            for key in gsxx_conf.gsxx_key_list:
                if entity_data.has_key(key) and entity_data.get(key) == None:
                    entity_data.pop(key)

            if entity_data.has_key("base_info"):
                is_baseinfo_page = True
                entity_data.pop("base_info")
                for item in extract_data["base_info"]:
                    for key, value in item.items():
                        base_key = key[:3]
                        base_value = "value" + key[3:]
                        if base_key == "key" and item.has_key(base_value):
                            key = item[key].encode("utf8")
                            if item.get(base_value) == None:
                                continue
                            value = item.get(base_value, "").encode("utf8")
                            key_values = [(key, value)]
                            key_pars = key.split("\t")
                            value_pars = value.split("\t")
                            if len(key_pars) > 1 and len(key_pars) == len(value_pars):
                                index = 0
                                while index < len(key_pars):
                                    key_values.append((key_pars[index], value_pars[index]))
                                    index += 1
                            for key, value in key_values:
                                value = value.strip()
                                if key in self.mapping_conf:
                                    if not entity_data.has_key(self.mapping_conf[key]):
                                        entity_data[self.mapping_conf[key]] = value
                                    if self.mapping_conf[key] == 'code':
                                        if value != None and len(value) >= 18:
                                            entity_data["unified_social_credit_code"] = value
                                        else:
                                            entity_data["registered_code"] = value
                                    break
            company = entity_data.get("company", "")
            # Check whether this page is the base info page
            # Fuck this code a thousand times!
            if entity_data.has_key("unified_social_credit_code") or \
                    entity_data.has_key("registered_code") or \
                    entity_data.has_key("code"):
                is_baseinfo_page = True
                if company == "":
                    self.log.error("base info without company " + json.dumps(entity_data))

                if entity_data.has_key("shareholder_information"):
                    shareholder_information = entity_data.get("shareholder_information")
                    new_shareholder_information = []
                    for each in shareholder_information:
                        if each.has_key("subscription_detail") and each.get("subscription_detail") != None:
                            each["subscription_detail"] = self.deal_data(
                                each.get("subscription_detail", []), ["subscription_amount"])
                            each["subscription_detail"] = self.deal_time(each.get("subscription_detail", []), ["subscription_time","subscription_publish_time"])

                        if each.has_key("paied_detail") and each.get("paied_detail") != None:
                            each["paied_detail"] = self.deal_data(each.get("paied_detail", []),["paied_amount"])
                            each["paied_detail"] = self.deal_time(each.get("paied_detail", []),["paied_time","paied_publish_time"])

                        new_shareholder_information.append(each)

                    entity_data["shareholder_information"] = self.deal_data(new_shareholder_information,["subscription_amount","paied_amount"])

                if entity_data.has_key("contributor_information"):
                    shareholder_information = entity_data.get("contributor_information")
                    new_shareholder_information = []
                    for each in shareholder_information:
                        if each.has_key("subscription_detail") and each.get("subscription_detail") != None:
                            each["subscription_detail"] = self.deal_data(
                                each.get("subscription_detail", []), ["subscription_amount"])
                            each["subscription_detail"] = self.deal_time(each.get("subscription_detail", []),["subscription_time","subscription_publish_time"])

                        if each.has_key("paied_detail") and each.get("paied_detail") != None:
                            each["paied_detail"] = self.deal_data(each.get("paied_detail", []),["paied_amount"])
                            each["paied_detail"] = self.deal_time(each.get("paied_detail", []),["paied_time", "paied_publish_time"])

                        new_shareholder_information.append(each)

                    entity_data["contributor_information"] = self.deal_data(new_shareholder_information,["subscription_amount","paied_amount"])

                if entity_data.has_key("code"):
                    value = entity_data["code"]
                    # entity_data.pop("code")
                    if len(value) == 18:
                        entity_data["unified_social_credit_code"] = value
                    else:
                        entity_data["registered_code"] = value

                src_registered_capital = entity_data.get('src_registered_capital')
                if not src_registered_capital:
                    src_registered_capital = entity_data.get('registered_capital')
                if src_registered_capital:
                    entity_data['src_registered_capital'] = src_registered_capital
                    registered_capital, registered_capital_unit = self.parser_tool.money_parser.transfer_money(
                        src_registered_capital)
                    entity_data["registered_capital"] = registered_capital
                    entity_data["registered_capital_unit"] = registered_capital_unit

                if entity_data.has_key("period_from"):
                    start_time = self.parser_tool.date_parser.get_date_list(entity_data["period_from"])
                    entity_data.pop("period_from")
                    if entity_data.has_key("period_to"):
                        if entity_data.get("period_to") == None:
                            entity_data.pop("period_to")
                        else:
                            end_time = self.parser_tool.date_parser.get_date_list(entity_data["period_to"])
                            entity_data["period"] = toolsutil.norm_date(start_time) + u"至" + toolsutil.norm_date(
                                end_time)
                            entity_data.pop("period_to")
                    else:
                        entity_data["period"] = toolsutil.norm_date(start_time) + u"至"

                if entity_data.has_key("period"):
                    period = entity_data.get("period", "")
                    ret = toolsutil.re_findone(self.period_regex, period)
                    if ret and len(ret) == 2:
                        start_time = toolsutil.norm_date(ret[0])
                        end_time = toolsutil.norm_date(ret[1])
                        period = start_time + u"至" + end_time
                    else:
                        ret2 = toolsutil.re_findone(self.period_regex2, period)
                        if ret2:
                            start_time = toolsutil.norm_date(ret2)
                            period = start_time + u"至"
                        else:
                            period = u"－－"
                    entity_data["period"] = period

                if company != "":
                    province, city = self.cal_province_city(entity_data)
                    entity_data["province"] = entity_data.get("province") if entity_data.get("province") else province
                    entity_data["city"] = entity_data.get("city") if entity_data.get("city") else city

                    if not self.filter_company(company):
                        entity_data["delete"] = 1

                    entity_data['company'] = company.replace('(', '（').replace(')', '）')

            if entity_data.has_key("changerecords"):
                changerecords_list = []
                used_name_list = []
                for item in entity_data["changerecords"]:
                    change_item = item.get("change_item", "")
                    change_item = unicode(change_item)
                    if change_item in gsxx_conf.used_name_change_item_list:
                        after_name = unicode(item.get("after_content", ""))
                        befor_name = unicode(item.get("before_content", ""))

                        checked_after_name = self.check_name(company, after_name)
                        checked_befor_name = self.check_name(company, befor_name)

                        if checked_after_name:
                            used_name_list.append(checked_after_name)
                        if checked_befor_name:
                            used_name_list.append(checked_befor_name)

                    change_date = item.get("change_date", "")
                    if isinstance(change_date, basestring):
                        change_date = toolsutil.norm_date_time(
                            self.parser_tool.date_parser.get_date_list(item.get("change_date", "")))
                    else:
                        change_date = str(change_date)
                        ret = toolsutil.re_find_one(u'\d+', change_date)
                        if len(change_date) > 10 and ret == change_date:
                            tmp = int(change_date[:-3])
                            data_value = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tmp))
                            change_date = data_value

                    item["change_date"] = change_date
                    changerecords_list.append(item)
                used_name_list = list(set(used_name_list))
                entity_data["used_name_list"] = used_name_list
                entity_data["changerecords"] = changerecords_list

            if entity_data.has_key("invested_companies"):
                invested_companies_list = []
                for single in entity_data["invested_companies"]:
                    invest_amount, invest_amount_unit = self.parser_tool.money_parser.transfer_money(
                        single.get("invest_amount", ""))
                    single["invest_amount"] = invest_amount
                    single["invest_amount_unit"] = invest_amount_unit
                    invested_companies_list.append(single)
                entity_data["invested_companies"] = invested_companies_list

            if entity_data.has_key("investor_change"):
                entity_data["investor_change"] = self.deal_time(entity_data.get("investor_change",[]),["change_date"])

            if entity_data.has_key("business_status"):
                entity_data["business_status"] = entity_data.get("business_status", "").replace(',', '，')

            if entity_data.has_key("registered_code"):
                value = entity_data.get("registered_code", "")
                if len(value.strip()) >= 18:
                    entity_data["registered_code"] = ""
            else:
                entity_data["registered_code"] = ""


            if not entity_data.get("industry"):
                entity_data["industry"] = self.parser_tool.industry_parser.predict(company)

            for key, value in entity_data.items():
                if value is None or (isinstance(value, basestring) and value.strip() == ''):
                    del entity_data[key]

        return entity_data

    def check_name(self, company_name, name):
        company_name = company_name.strip()
        name = name.strip()

        re_match = self.extract_re.match(name)
        if re_match:
            name = re_match.groups()[-1].strip()

        name = name.strip(u'*;.')
        name = re.split(u';|，|。|．', name)[0]
        name = name.strip(u'*.')

        for pstr in gsxx_conf.prefix_cut_list:
            if name.startswith(pstr):
                name = name[len(pstr):]

        for fstr in gsxx_conf.fobidden_str_list:
            if fstr in name:
                return False
        if len(name) > 30:
            return False
        if len(name) <= 4:
            return False
        if company_name == name:
            return False
        return name

    def filter_company(self, company):
        '''过滤公司名'''
        if not company:
            return False
        company = unicode(company.strip())
        first_str = toolsutil.strQ2B(company[:1])
        last_str = toolsutil.strQ2B(company[-1:])

        if first_str in self.punctuation_list:
            return False

        if last_str in self.punctuation_list:
            return False

        return True

    def cal_province_city(self, item):
        province = city = ""
        if item:
            company = item.get("company", "")
            address = item.get("address", "")
            registered_address = item.get("registered_address", "")
            company_province = self.parser_tool.province_parser.get_province(company)
            address_province = self.parser_tool.province_parser.get_province(address, 1)
            registered_address_province = self.parser_tool.province_parser.get_province(registered_address, 1)
            company_city = self.parser_tool.province_parser.get_region(company)
            address_city = self.parser_tool.province_parser.get_region(address, 1)
            registered_address_city = self.parser_tool.province_parser.get_region(registered_address, 1)
            province = self.select_data(company_province, address_province, registered_address_province)
            city = self.select_data(company_city, address_city, registered_address_city)

        return (province, city)

    def select_data(self, company_data, address_data, registered_address_data):
        data = ""
        data_map = {}
        if company_data:
            if company_data not in data_map.keys():
                data_map[company_data] = 0
            data_map[company_data] += 1
        if address_data:
            if address_data not in data_map.keys():
                data_map[address_data] = 0
            data_map[address_data] += 1
        if registered_address_data:
            if registered_address_data not in data_map.keys():
                data_map[registered_address_data] = 0
            data_map[registered_address_data] += 1
        if data_map.get(company_data, 0) < 2 and data_map.get(address_data, 0) < 2 and data_map.get(
                registered_address_data, 0) < 2:
            if registered_address_data:
                data = registered_address_data
            elif address_data:
                data = address_data
            else:
                data = company_data
            return data
        for k, v in sorted(data_map.items(), lambda x, y: cmp(x[1], y[1]), reverse=True):
            data = k
            return data
        return data

    def deal_data(self, src_data_list, key_list):

        new_data_list = []
        deal_flag = False
        for iter_data in src_data_list:
            for key in key_list:
                try:
                    paied_amount, paied_amount_unit = self.parser_tool.money_parser.transfer_money(
                        iter_data[key])
                    iter_data[key] = paied_amount
                    data_unit = key + "_unit"
                    iter_data[data_unit] = paied_amount_unit
                    deal_flag = True

                except:
                    pass
            new_data_list.append(iter_data)

        return new_data_list

    def deal_time(self,src_data_list,key_list):

        new_data_list = []
        deal_flag     = False
        for iter_data in src_data_list:
            for key in key_list:
                try:
                    time_value = str(iter_data[key])
                    ret = toolsutil.re_find_one(u'\d+',time_value)
                    if len(time_value) > 10 and ret == time_value:
                        tmp = int(time_value[:-3])
                        data_value = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(tmp))
                        iter_data[key] = data_value
                        deal_flag = True
                    else:
                        iter_data[key] = toolsutil.norm_date_time(iter_data[key])
                        deal_flag = True
                except:
                    pass
            new_data_list.append(iter_data)

        return new_data_list

if __name__ == '__main__':
    import pytoml
    import sys
    import time
    from common.log import log

    sys.path.append('../../')

    with open('../../entity.toml', 'rb') as config:
        conf = pytoml.load(config)

    log.init_log(conf, console_out=conf['logger']['console'])
    conf['log'] = log

    topic_id = 49
    from i_entity_extractor.entity_extractor_route import EntityExtractorRoute
    import json

    route = EntityExtractorRoute(conf)
    topic_info = route.all_topics.get(topic_id, None)
    begin_time = time.time()
    obj = GsxxExtractor(topic_info, log)

    extract_data = {
        "base_info": [
            {
                "key": "公司名称",
                "value": "中国投资有限责任公司"
            },
            {
                "key": "统一社会信用代码/注册号",
                "value": " 916100007521179571 "
            },
            {
                "key": "企业法人",
                "value": "丁学东"
            },
            {
                "key": "经营状态",
                "value": "开业"
            },
            {
                "key": "注册资本",
                "value": None,
            },
            {
                "key": "成立时间",
                "value": "2007-09-28"
            },
            {
                "key": "运营时间",
                "value": "2007-09-28至"
            },
            {
                "key": "企业类型",
                "value": "有限责任公司"
            },
            {
                "key": "注册地址",
                "value": "北京市东城区朝阳门北大街1号新保利大厦16-19层"
            },
            {
                "key": "经营范围",
                "value": "许可经营项目：(无)一般经营项目：境内外币债券等外币类金融产品投资；境外债券、股票、基金、衍生金融工具等金融产品投资；境内外股权投资；对外委托投资；委托金融机构进行贷款；外汇资产受托管理；发起设立股权投资基金及基金管理公司；国家有关部门批准的其他业务。"
            },
            {
                "key": "联系方式",
                "value": "010-84096277"
            },
            {
                "key": "E-mail",
                "value": "recruit@china-inv.cn"
            }
        ],

        "company": "乐山市五通桥力源发电(有)限责任公司",
        "business_scope": "水力发电及销售；输配电及控制设备修理（依法须经批准的项目，经相关部门批准后方可开展经营活动）。※",
        "invested_companies": None,
        "address": "乐山市五通桥竹根镇建设街",
    }

    entity_data = obj.format_extract_data(extract_data, topic_id)
    entity_data = obj.after_process(entity_data)
    print json.dumps(entity_data, ensure_ascii=False, encoding='utf8')
    print "-----------------------------"
    for key, value in entity_data.items():
        if isinstance(value, list):
            for i in value:
                print key, ":", i
        elif isinstance(value, dict):
            for key2, value2 in value.items():
                print key2, ":", value2
        else:
            print key, ":", value

    print "time_cost:", time.time() - begin_time

