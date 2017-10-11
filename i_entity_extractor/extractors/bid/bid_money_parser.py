# coding=utf8
import sys

sys.path.append("..")
from i_entity_extractor.common_parser_lib import toolsutil
from i_entity_extractor.common_parser_lib.money_parser import MoneyParser
import bid_conf
import traceback
import esm
import re


class Bid_money_parser:
    def __init__(self):
        self.min_money       = 3000
        self.max_money       = 10000000000
        self.content_length  = 60
        self.deal_num        = 10
        self.money_regex     = re.compile('\d+.\d+')
        self.money_wan_regex = re.compile('\d+.\d+万')
        self.budget_index    = esm.Index()
        for keyword in bid_conf.bid_budget_keyword_list:
            self.budget_index.enter(keyword)
        self.budget_index.fix()

        self.money_index = esm.Index()
        for keyword in bid_conf.bid_money_keyword_list:
            self.money_index.enter(keyword)
        self.money_index.fix()

    def do_parser(self, content):
        '''获取正文中金额'''
        money_list = []
        budget_money_list = []
        if not content:
            return money_list, budget_money_list
        content = content.encode('utf8')
        content = content.replace(',', '').replace('，', '').replace(' ', '')

        # 1 获取中标金额
        money_list = self.get_money(content, self.money_index, self.content_length)
        for num in range(self.deal_num):
            if not money_list:
                money_list = self.get_money(content, self.money_index, self.content_length + (num + 1) * 15)
            else:
                break

        if money_list:
            money_list = self.norm_money_list(money_list)

        # 2 获取招标预算金额
        budget_money_list = self.get_money(content, self.budget_index, self.content_length)
        for num in range(self.deal_num):
            if not budget_money_list:
                budget_money_list = self.get_money(content, self.budget_index, self.content_length + num * 15)
            else:
                break
        if budget_money_list:
            budget_money_list = self.norm_money_list(budget_money_list)
        return money_list, budget_money_list

    def get_money(self, content, esm_index, content_length):
        money_list = []
        money_ret = esm_index.query(content)
        if money_ret:
            find_flag = False
            for ret in money_ret:
                pos = ret[0][1]
                relate_content = content[pos:pos + content_length]

                money = toolsutil.re_find_one('\d+.\d+万', relate_content)
                if money:
                    money, unit = MoneyParser().transfer_money(money)
                    money = unicode(money)
                    money_list.append((money, unit))
                    find_flag = True
            if not find_flag:
                for ret in money_ret:
                    find_flag2 = False
                    pos = ret[0][1]
                    relate_content = content[pos:pos + content_length]
                    if '万元' in relate_content:
                        find_flag2 = True
                    money_yuan = toolsutil.re_find_one('\d+.\d+', relate_content)
                    if money_yuan:
                        if find_flag2:
                            money_yuan = money_yuan + '万'
                        money_yuan, unit = MoneyParser().transfer_money(money_yuan)
                        money_yuan = unicode(money_yuan)
                        money_list.append((money_yuan, unit))
        return money_list

    def norm_money_list(self, money_list):
        '''根据特性格式化金额，过滤干扰数字'''
        result_list = []
        for item in money_list:
            if len(item) != 2:
                continue
            digit = item[0]
            if digit and float(digit) > self.min_money and float(digit) < self.max_money:
                result_list.append(item)

        result_list = list(set(result_list))
        return result_list


if __name__ == "__main__":


    import time

    obj = Bid_money_parser()

    begin_time = time.time()

    content = ' 亳州市体育公园项目施工中标公示 招标人   建安投资控股集团有限公司   工程名称  亳州市体育公园项目施工（BZGC2016202）   招标方式  公开招标   开标时间  2016年10月19日9:30   中标单位名称  第一中标候选人：江苏南通三建集团股份有限公司 第二中标候选人：中国建筑第八工程局有限公司 第三中标候选人：中铁电气化局集团有限公司   中标价  第一中标候选人报价：壹亿零柒佰玖拾伍万捌仟贰佰玖拾壹元柒角贰分（小写：107958291.72元） 第二中标候选人报价：壹亿零伍佰玖拾万零肆仟陆佰肆拾元伍角陆分（小写：105904640.56元） 第三中标候选人报价：壹亿零壹佰捌拾玖万捌仟捌佰玖拾元陆角贰分（小写：101898890.62元）   项目负责人  第一中标候选人项目负责人：黄敏逵 第二中标候选人项目负责人：王先文 第三中标候选人项目负责人：朱鼎亚   第一中标候选人 业绩奖项  1、企业工程奖项： ①闸北区文化馆和大宁社区文化活动中心工程，荣获2012—2013年度中国建设鲁班奖（国家优质工程），颁发时间：2013年12月； ②镇江皇冠假日酒店工程，荣获2013—2014年度国家优质工程奖，颁发时间：2014年11月； 2、项目经理奖项： ①高速·滨湖时代广场C-01地块C3#—C7#楼及商业土建总施工，竣工日期：2014年11月； 3、施工企业奖项： ①荣获2015年度全国优秀施工企业，由中国施工业管理协会颁发，颁发时间：2016年3月； ②全国建筑业先进企业称号，由中国建筑业协会颁发，颁发时间：2011年12月。   第二中标候选人 业绩奖项  1、企业工程奖项： ①利通广场工程，荣获2012—2013年度中国建筑工程鲁班奖（国家优质工程奖），颁发时间：2013年12月。 ②上海浦东发展银行合肥综合中心工程，荣获2014—2015年度国家优质工程奖，颁发时间：2015年11月。 2、项目经理奖项： ①双山保儿片区旧村改造项目A-9-5地块（凯德MALL·新都心项目），竣工日期：2016年6月。 3、施工企业奖项： ①荣获2015年度全国优秀施工企业，由中国施工业管理协会颁发，颁发时间：2016年3月； ②全国建筑业先进企业称号，由中国建筑业协会颁发，颁发时间：2014年11月。   第三中标候选人 业绩奖项  1、项目经理奖项： ①新建铁路天津至秦皇岛客运专线唐山站房、滨海北站房、滦河站房工程标段施工，竣工日期：2013年6月。 2、施工企业奖项： ①全国建筑业先进企业称号，由中国建筑业协会颁发，颁发时间：2016年10月。   公示时间  至2016年10月25日   质疑（异议）联系电话  招标人：0558-5582632 招标机构：0558-5991108   投诉电话  市招管局：0558-5991109   备注  领取中标通知书时，请提供下列材料： 1. 企业住所地或亳州检察机关出具的该企业无行贿犯罪记录证明。 2.电子版综合治税信息表格，下载地址：http://www.bzztb.gov.cn/BZWZ/综合治税信息填报表格式_工程类.xls'
    money_list, budget_money_list = obj.do_parser(content)
    print money_list

    for money in money_list:
        print "money:", money,type(money)

    for money in budget_money_list:
        print "budget:", money

