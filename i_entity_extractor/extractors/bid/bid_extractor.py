# coding=utf-8
# 招标中标实体解析
import sys

sys.path.append("..")
sys.path.append("../../")

import json

import bid_conf
from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
import copy


class BidExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)


    def format_extract_data(self, extract_data, topic_id):
        '''实体解析抽取数据'''
        entity_data = copy.deepcopy(extract_data)
        title = extract_data.get("title")
        bid_type = extract_data.get("bid_type")

        province = extract_data.get("province")
        city = extract_data.get("city")

        if bid_type == None and title != None:
            bid_type = self.bid_type_parser(title)


        if province == None:
            if city != None:
                province = self.parser_tool.province_parser.get_province(city)
            else:
                province = self.parser_tool.province_parser.get_province(title)

        result_data = {}
        if extract_data.has_key("content"):
            content      = extract_data.get("content")
            city         = self.parser_tool.bid_region_parser.do_parser(content)
            closing_time = self.parser_tool.bid_close_time_parser.do_parser(content)
            (money_list, budget_money_list) = self.parser_tool.bid_money_parser.do_parser(content)
            money_unit   = money_list[0][1] if money_list else ''

            if not money_unit:
                money_unit = budget_money_list[0][1] if budget_money_list else ''

            money_list = [x[0] for x in money_list]
            budget_money_list = [x[0] for x in budget_money_list]

            result_data = self.parser_tool.bid_company_parser.do_parser(content, title)

            if city == "" and result_data.get("zhaobiao"):
                city = self.parser_tool.province_parser.get_region(result_data.get("zhaobiao"))
            result_data["city"] = city
            result_data["closing_time"] = closing_time
            result_data["bid_money_list"] = money_list
            result_data["bid_budget_list"] = budget_money_list
            result_data["bid_money"] = ','.join(money_list)
            result_data["bid_budget"] = ','.join(budget_money_list)
            result_data['money_unit'] = money_unit
        else:
            content = None

        entity_data["province"]           = province
        entity_data["public_bid_company"] = result_data.get("zhaobiao")
        entity_data["win_bid_company"] = result_data.get("zhongbiao")
        entity_data["candicate_win_bid_company"] = result_data.get("houxuan_zhongbiao")
        entity_data["bid_budget"]      = result_data.get("bid_budget")
        entity_data["bid_money"]       = result_data.get("bid_money")
        entity_data["bid_money_list"]  = result_data.get("bid_money_list")
        entity_data["bid_budget_list"] = result_data.get("bid_budget_list")
        entity_data["bid_type"]        = bid_type
        entity_data["city"]            = result_data.get("city")
        entity_data["agent"]           = result_data.get("agent")
        entity_data["bid_content"]     = content
        entity_data["closing_time"]    = result_data.get("closing_time")
        entity_data["money_unit"]      = result_data.get("money_unit")


        return entity_data


    def bid_type_parser(self, title):
        '''标类型解析'''
        bid_type = ''

        for keyword in bid_conf.bid_type_keyword_list:
            if keyword[0] in title:
                bid_type = keyword[1]
                break
        return bid_type


if __name__ == "__main__":
    import pytoml
    import sys

    sys.path.append('../../')
    from conf import get_config
    from bdp.i_crawler.i_extractor.ttypes import BaseInfo, ExtractInfo, PageParseInfo

    with open('../../entity.toml', 'rb') as config:
        config = pytoml.load(config)
    conf = get_config(config)
    import common

    topic_id = 41
    from entity_extractor_route import EntityExtractorRoute

    route = EntityExtractorRoute()
    topic_info = route.all_topics.get(topic_id, None)
    src_url = 'http://www.hnzbtb.com/ZhongBiaoInfo433881.htm'
    extract_data = {
    "bid_id": "",
    "bid_type": "",
    "city": "",
    "content": ' 亳州市体育公园项目施工中标公示 招标人   建安投资控股集团有限公司   工程名称  亳州市体育公园项目施工（BZGC2016202）   招标方式  公开招标   开标时间  2016年10月19日9:30   中标单位名称  第一中标候选人：江苏南通三建集团股份有限公司 第二中标候选人：中国建筑第八工程局有限公司 第三中标候选人：中铁电气化局集团有限公司   中标价  第一中标候选人报价：壹亿零柒佰玖拾伍万捌仟贰佰玖拾壹元柒角贰分（小写：107958291.72元） 第二中标候选人报价：壹亿零伍佰玖拾万零肆仟陆佰肆拾元伍角陆分（小写：105904640.56元） 第三中标候选人报价：壹亿零壹佰捌拾玖万捌仟捌佰玖拾元陆角贰分（小写：101898890.62元）   项目负责人  第一中标候选人项目负责人：黄敏逵 第二中标候选人项目负责人：王先文 第三中标候选人项目负责人：朱鼎亚   第一中标候选人 业绩奖项  1、企业工程奖项： ①闸北区文化馆和大宁社区文化活动中心工程，荣获2012—2013年度中国建设鲁班奖（国家优质工程），颁发时间：2013年12月； ②镇江皇冠假日酒店工程，荣获2013—2014年度国家优质工程奖，颁发时间：2014年11月； 2、项目经理奖项： ①高速·滨湖时代广场C-01地块C3#—C7#楼及商业土建总施工，竣工日期：2014年11月； 3、施工企业奖项： ①荣获2015年度全国优秀施工企业，由中国施工业管理协会颁发，颁发时间：2016年3月； ②全国建筑业先进企业称号，由中国建筑业协会颁发，颁发时间：2011年12月。   第二中标候选人 业绩奖项  1、企业工程奖项： ①利通广场工程，荣获2012—2013年度中国建筑工程鲁班奖（国家优质工程奖），颁发时间：2013年12月。 ②上海浦东发展银行合肥综合中心工程，荣获2014—2015年度国家优质工程奖，颁发时间：2015年11月。 2、项目经理奖项： ①双山保儿片区旧村改造项目A-9-5地块（凯德MALL·新都心项目），竣工日期：2016年6月。 3、施工企业奖项： ①荣获2015年度全国优秀施工企业，由中国施工业管理协会颁发，颁发时间：2016年3月； ②全国建筑业先进企业称号，由中国建筑业协会颁发，颁发时间：2014年11月。   第三中标候选人 业绩奖项  1、项目经理奖项： ①新建铁路天津至秦皇岛客运专线唐山站房、滨海北站房、滦河站房工程标段施工，竣工日期：2013年6月。 2、施工企业奖项： ①全国建筑业先进企业称号，由中国建筑业协会颁发，颁发时间：2016年10月。   公示时间  至2016年10月25日   质疑（异议）联系电话  招标人：0558-5582632 招标机构：0558-5991108   投诉电话  市招管局：0558-5991109   备注  领取中标通知书时，请提供下列材料： 1. 企业住所地或亳州检察机关出具的该企业无行贿犯罪记录证明。 2.电子版综合治税信息表格，下载地址：http://www.bzztb.gov.cn/BZWZ/综合治税信息填报表格式_工程类.xls',
    "province": "",
    "publish_time": "2016年12月12日 15:24",
    "title": "河南省太康县成人中等专业学校附属医院装修项目招标公告"
}
    obj = BidExtractor(topic_info, common.log)
    data = json.dumps(extract_data)
    extract_info = ExtractInfo(ex_status=2, extract_data=data)
    base_info = BaseInfo(url=src_url)
    parser_info = PageParseInfo(base_info=base_info, extract_info=extract_info)
    entity_data = obj.entity_extract(parser_info, extract_data)
    entity_data = obj.after_extract(base_info.url, entity_data, extract_data)

    for key, value in entity_data.items():
        if isinstance(value, list):
            for i in value:
                print key, ":", i, type(value)
        elif isinstance(value, dict):
            for key2, value2 in value.items():
                print key2, ":", value2
        else:
            print key, ":", value
