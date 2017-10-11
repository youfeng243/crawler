# coding=utf-8
# 企业年报实体解析

import copy
import json
import time
from i_entity_extractor.common_parser_lib import toolsutil
from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor


class AnnualReportsExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.config_path  = self.basic_path + "i_entity_extractor/extractors/annual_reports/mapping.conf"
        self.mapping_conf = self.read_config()

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
        entity_data = {}
        if extract_data:
            entity_data = copy.deepcopy(extract_data)
            if entity_data.has_key("base_info"):
                entity_data.pop("base_info")
                for item in extract_data["base_info"]:
                    for key, value in item.items():
                        base_key = key[:3]
                        base_value = "value" + key[3:]
                        if base_key == "key" and item.has_key(base_value):
                            key = item[key].encode("utf8")
                            value = item[base_value].encode("utf8")
                            key_values = [(key, value)]
                            key_pars = key.split("\t")
                            value_pars = value.split("\t")
                            if len(key_pars) > 1 and len(key_pars) == len(value_pars):
                                index = 0
                                while index < len(key_pars):
                                    key_values.append((key_pars[index], value_pars[index]))
                                    index += 1
                            for key, value in key_values:
                                if key in self.mapping_conf:
                                    entity_data[self.mapping_conf[key]] = value

                                    if self.mapping_conf[key] == 'code':
                                        entity_data.pop("code")
                                        if len(value) == 18:
                                            entity_data["unified_social_credit_code"] = value
                                        else:
                                            entity_data["registered_code"] = value
                                    break

        if not entity_data.get("company_name") and entity_data.get("company"):
            entity_data["company_name"] = entity_data.get("company")


        if entity_data.has_key("edit_change_infos") and entity_data.get("edit_change_infos") != None:
            entity_data["edit_change_infos"] = self.deal_time(entity_data.get("edit_change_infos",[]),["change_date"])

        if entity_data.has_key("shareholder_information") and entity_data.get("shareholder_information") != None:
            entity_data["shareholder_information"] = self.deal_time(entity_data.get("shareholder_information",[]),["subscription_time","paied_time"])


        if entity_data.has_key("administrative_licensing_info") and entity_data.get("administrative_licensing_info") != None:
            entity_data["administrative_licensing_info"] = self.deal_time(entity_data.get("administrative_licensing_info",[]),["license_period_date"])

        if entity_data.has_key("edit_shareholding_change_infos") and entity_data.get("edit_shareholding_change_infos") != None:
            entity_data["edit_shareholding_change_infos"] = self.deal_time(entity_data.get("edit_shareholding_change_infos",[]),["change_date"])



        return entity_data


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

# if __name__ == '__main__':
#     import sys
#     sys.path.append('../../')
#     topic_id = 136
#     import pytoml
#     from conf import get_config
#     from bdp.i_crawler.i_extractor.ttypes import BaseInfo, CrawlInfo, ExtractInfo, PageParseInfo
#     with open('../../entity.toml', 'rb') as config:
#         config = pytoml.load(config)
#     conf = get_config(config)
#     import common
#     from entity_extractor_route import EntityExtractorRoute
#     route = EntityExtractorRoute()
#     topic_info = route.all_topics.get(topic_id, None)
#     obj = AnnualReportsExtractor(topic_info, common.log)
#     extract_data = {
#     "base_info": [
#         {
#             "key": "统一社会信用代码/注册号",
#             "value": "911400001123101349"
#         },
#         {
#             "key": "企业名称",
#             "value": "华晋焦煤有限责任公司"
#         },
#         {
#             "key": "企业通信地址",
#             "value": "山西省吕梁市离市区久安路57号"
#         },
#         {
#             "key": "邮政编码",
#             "value": "033000"
#         },
#         {
#             "key": "企业联系电话",
#             "value": "0358-8296368"
#         },
#         {
#             "key": "企业电子邮箱",
#             "value": "hjjmyxzrgs@163.com"
#         },
#         {
#             "key": "从业人数",
#             "value": "企业选择不公示"
#         },
#         {
#             "key": "企业经营状态",
#             "value": "开业"
#         },
#         {
#             "key": "是否有网站或网店",
#             "value": "是"
#         },
#         {
#             "key": "有限责任公司本年度是否发生股东股权转让",
#             "value": "否"
#         },
#         {
#             "key": "是否有投资信息或购买其他公司股权",
#             "value": "有"
#         },
#         {
#             "key": "对外提供保证担保信息",
#             "value": "否"
#         }
#     ],
#     "enterprise_asset_status_information": [
#         {
#             "key": "资产总额",
#             "value": "企业选择不公示"
#         },
#         {
#             "key": "所得者权益合计",
#             "value": "企业选择不公示"
#         },
#         {
#             "key": "营业总收入",
#             "value": "企业选择不公示"
#         },
#         {
#             "key": "利润总额",
#             "value": "企业选择不公示"
#         },
#         {
#             "key": "营业总收入中主营业务收入",
#             "value": "企业选择不公示"
#         },
#         {
#             "key": "净利润",
#             "value": "企业选择不公示"
#         },
#         {
#             "key": "纳税总额",
#             "value": "企业选择不公示"
#         },
#         {
#             "key": "负债总额",
#             "value": "企业选择不公示"
#         }
#     ],
#     "invested_companies": [
#         {
#             "company_name": "华晋煤层气综合利用有限责任公司",
#             "registered_code": "140000110106177"
#         },
#         {
#             "company_name": "山西华晋吉宁煤业有限责任公司",
#             "registered_code": "140000105974138"
#         },
#         {
#             "company_name": "山西华晋明珠煤业有限责任公司",
#             "registered_code": "140000206970138"
#         },
#         {
#             "company_name": "山西焦煤华晋寨圪塔能源有限责任公司",
#             "registered_code": "141000000074580"
#         },
#         {
#             "company_name": "石太铁路客运专线有限责任公司",
#             "registered_code": "140100103043161"
#         },
#         {
#             "company_name": "山西汾河焦煤股份有限公司",
#             "registered_code": "140000100099469"
#         },
#         {
#             "company_name": "山西焦煤集团汾河物业管理有限公司",
#             "registered_code": "140100103047124"
#         },
#         {
#             "company_name": "山西焦煤集团房地产开发有限公司",
#             "registered_code": "140100103020695"
#         },
#         {
#             "company_name": "山西焦煤交通能源投资有限公司",
#             "registered_code": "140000110111179"
#         }
#     ],
#     "province": "山西",
#     "shareholder_information": [
#         {
#             "paied_amount": "42354.798018",
#             "paied_time": "1204646400000",
#             "paied_type": "货币",
#             "shareholder_name": "山西焦煤集团有限责任公司",
#             "subscription_amount": "42354.798018",
#             "subscription_time": "1204646400000",
#             "subscription_type": "货币"
#         },
#         {
#             "paied_amount": "40693.825547",
#             "paied_time": "2017年6月4日",
#             "paied_type": "货币",
#             "shareholder_name": "中国中煤能源股份有限公司",
#             "subscription_amount": "40693.825547",
#             "subscription_time": "1204646400000",
#             "subscription_type": "货币"
#         }
#     ],
#     "websites": [
#         {
#             "name": "华晋焦煤有限责任公司",
#             "site": "http://www.sx.xinhuanet.com/qyzx/hjjm/",
#             "type": "网站"
#         }
#     ],
#     "year": "2015年度"
# }
#     src_url = "www.baidu.com"
#     data = json.dumps(extract_data)
#     extract_info = ExtractInfo(ex_status=2, extract_data=data)
#     base_info = BaseInfo(url=src_url)
#     parser_info = PageParseInfo(base_info=base_info, extract_info=extract_info)
#     entity_data = obj.entity_extract(parser_info, extract_data)
#     #data = obj.after_extract(src_url, entity_data, extract_data)
#     print data
#     for key, value in entity_data.items():
#         if isinstance(value, list):
#             for i in value:
#                 print key, ":", i
#         elif isinstance(value, dict):
#             for key2, value2 in value.items():
#                 print key2, ":", value2
#         else:
#             print key, ":", value