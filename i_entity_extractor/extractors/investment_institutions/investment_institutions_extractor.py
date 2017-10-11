# coding=utf8
import sys
import re
import time
import traceback
sys.path.append('../../')
from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib import etl_tool
import copy


class InvestmentInstitutionsExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.lst_cfg_area = [u'province', u'city', u'county']

    def format_extract_data(self, extract_data, topic_id):
        """实体解析抽取数据"""
        item = copy.deepcopy(extract_data)

        lst_schema = ['fax', 'contacts', 'address', 'managing_partner', 'capital', 'phone', 'email', 'city']
        for schema in lst_schema:  # N/A用空代替(传真,N/A 暂无数据;联系人,N/A 暂无数据 暂未收录,此处假设没有人姓"暂")
            schema_name = item.get(schema, '')
            item[schema] = '' if schema_name == u"N/A" or schema_name == u'暂无数据' or schema_name == u'暂未收录' else schema_name

        # 处理省市县的拆分#拆分it橘子
        lst_location = etl_tool.regex_chinese(item.get('region', ''))
        map_region = etl_tool.province_city_district(lst_location)
        for region_key in map_region.keys():
            item[region_key] = map_region[region_key]

        source_site = item.get('_src', [{}])[0].get('site', u'')
        if source_site == u'www.itjuzi.com':
            item['county'] = item.get(u'county', u'')

        # 拆分私募通总部,私募通总部字段合并到地区字段中
        # lst_headquarters = etl_tool.regex_chinese(item.get('enterprise_headquarters', ''))
        # item['region'] = lst_headquarters  # 合并
        # for idx in xrange(len(lst_headquarters)):
        #     if idx > 2:
        #         continue
        #     item[self.lst_cfg_area[idx]] = lst_headquarters[idx]

        # 处理成立时间
        setup_dater = item.get(u'setup_date', u'')
        try:
            setup_date = etl_tool.str2datetime(setup_dater, '%Y-%m-%d %H:%M:%S')
            if not setup_date:
                setup_date = etl_tool.str2datetime(setup_dater, '%Y-%m-%d')
            item["setup_date"] = '' if setup_date == None else setup_date.strftime("%Y-%m-%d")
        except Exception:
            print setup_dater
            print traceback.format_exc()

        # 管理资本
        capital = item.get('capital', '')
        res_amount = self.parser_tool.money_parser.new_trans_money(capital, u"万", False)
        item['amounts'] = res_amount[0]
        item['units'] = res_amount[1]
        item['currency'] = res_amount[2]
        return item



if __name__ == '__main__':
    import pytoml
    import sys

    sys.path.append('../../')
    from conf import get_config

    with open('../../entity.toml', 'rb') as config:
        config = pytoml.load(config)
    conf = get_config(config)
    import common

    topic_id = 32
    from entity_extractor_route import EntityExtractorRoute

    route = EntityExtractorRoute()
    topic_info = route.all_topics.get(topic_id, None)

    obj = InvestmentInstitutionsExtractor(topic_info, common.log)
    extract_data = {
    "funds_count" : "",
    "source_site" : "因果树",
    "_record_id" : "d53dd6da1d723918f714f81ed90c26e1",
    "enterprise_type" : "VC",
    "city" : "",
    "managing_partner" : "白文涛",
    "contacts" : "白文涛",
    "_in_time" : "2016-10-31 22:09:33",
    "capital" : "62900万人民币",
    "setup_date" : "2007-08-27 00:00:00",
    "email" : "maggie@sharecapital.cn",
    "profile" : "深圳市分享投资合伙企业（简称“分享投资”）成立于2007年8月27日，是一家由国内杰出企业家和投资人共同创立的专业股权投资有限合伙企业。分享投资重点关注高科技、新能源、新型服务业领域，致力于发掘和扶持未来的行业领袖，为其提供资金和创业增值服务。分享投资专注于处在发展期的细分领域前三名的企业。截止2012年，分享投资管理人民币和美元两类基金，总规模约为12亿元人民币。",
    "fax" : "0755-86331909",
    "enterprise_short_name" : "分享投资",
    "enterprise_full_name" : "深圳市分享投资合伙企业(有限合伙)",
    "address" : "",
    "capital_type" : "中资",
    "investment_team" : [
        {
            "partner_name" : "白文涛",
            "partner_job" : "分享投资 管理合伙人兼执行合伙人"
        },
        {
            "partner_name" : "Erik Lassila",
            "partner_job" : "投资委员会委员/管理合伙人"
        },
        {
            "partner_name" : "黄反之",
            "partner_job" : "投资委员会委员/管理合伙人"
        },
        {
            "partner_name" : "崔欣欣",
            "partner_job" : "投资总监"
        },
        {
            "partner_name" : "管涛",
            "partner_job" : "投资经理"
        },
        {
            "partner_name" : "赵文彬",
            "partner_job" : "投资经理"
        },
        {
            "partner_name" : "李洁",
            "partner_job" : "投资经理"
        },
        {
            "partner_name" : "谢开",
            "partner_job" : "投资经理"
        },
        {
            "partner_name" : "陈静",
            "partner_job" : "财务经理"
        },
        {
            "partner_name" : "顾宁",
            "partner_job" : "风险控制总监"
        }
    ],
    "phone" : "0755-86331929",
    "region" : "上海",
    "enterprise_headquarters" : "",
    "_utime" : "2016-11-01 19:31:13"
    }
    entity_data = obj.format_extract_data(extract_data)

    print "-----------------------------"
    for key, value in entity_data.items():
        if isinstance(value, list):
            for v in value:
                print key, ":", v
        elif isinstance(value, dict):
            for key2, value2 in value.items():
                print key2, ":", value2
        else:
            print key, ":", value

    keys = ['province', 'funds_count', 'source_site', 'currency', 'enterprise_type', 'city', 'capital_type', 'contacts', 'amounts', 'capital', 'units', 'setup_date', 'email', 'profile', 'fax', 'enterprise_short_name', 'enterprise_full_name', 'address', 'managing_partner', 'investment_team', 'phone', 'url', 'region', 'enterprise_headquarters', '_in_time', '_src', '_record_id', '_id']
    transfer_data(keys, 'investment_institutions')
    print keys
