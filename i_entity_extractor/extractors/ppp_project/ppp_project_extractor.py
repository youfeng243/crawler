# coding=utf8
import sys
import re
import traceback
sys.path.append('../../')
from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib import etl_tool
import copy


class PppProjectExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)

    def format_extract_data(self, extract_data, topic_id):
        """实体解析抽取数据"""
        item = copy.deepcopy(extract_data)
        reward_mechanism = self.format_split(item, 'reward_mechanism')        #回报机制reward_mechanism
        item['reward_mechanism'] = reward_mechanism

        project_operation_mode = self.format_split(item, 'project_operation_mode')
        item['project_operation_mode'] = project_operation_mode

        term_of_cooperation = self.format_term_of_cooperation(item, 'term_of_cooperation')
        item['term_of_cooperation'] = term_of_cooperation

        industry = item.get('industry', '')        #处理行业
        lst_industry = etl_tool.regex_chinese(industry)
        item['industry'] = '/'.join(lst_industry)

        lst_location = etl_tool.regex_chinese(item.get('location', ''))     #处理省市县的拆分, 从location字段进行抽取拆分
        map_region = etl_tool.province_city_district(lst_location)
        for region_key in map_region.keys():
            item[region_key] = map_region[region_key]
        item['location'] = ''.join(lst_location)

        item['ppp_implementation_phase'] = self.format_ppp_implementation_phase(item)     #过滤PPP项目库

        project_level = item.get('demonstration_project_level', u'')     #示范/推介项目级别
        item['demonstration_project_level'] = u'其它' if project_level.strip() == u'其他'else project_level

        contact_phone = item.get('contact_phone', u'')     #联系人电话
        item['contact_phone'] = re.sub('^[^\d]+|[^\d]+$', '', contact_phone)        #过滤掉电话号码前后非数字的字符

        investment_amount = item.get('investment_amount', '')      #金额格式化
        res_investment_amount = self.parser_tool.money_parser.new_trans_money(investment_amount, out_money_unit=u"万")
        item['amounts'] = res_investment_amount[0]
        item['units']    = res_investment_amount[1]
        item['currency'] = res_investment_amount[2]
        source_site = item.get('_src', [{}])[0].get('site', u'')

        project_type = u""        #网站来源和content的关键字来确定project_type
        if source_site == u'www.chinappp.cn':
            content = item.get(u'content', u'')
            project_type = project_type if content.find(u'财政部项目库')>0 else u'发改委项目'
        if source_site == u'www.cpppc.org':
            project_type = u"财政部项目"

        item['project_type'] = project_type
        launch_time = item.get('launch_time', '')
        item['launch_time'] = etl_tool.regex_remove_time(launch_time).strip()
        return item

    def format_split(self, item, key):
        """格式化，将字段包含多项内容的统一用逗号分隔符分隔"""
        project_operation_mode = item.get(key, u'')
        if project_operation_mode.find(u'其他') or project_operation_mode.find(u'其她'):
            project_operation_mode = project_operation_mode.replace(u'其他', u'其它')
            project_operation_mode = project_operation_mode.replace(u'其她', u'其它')
        res = re.split(u"([ \\\，,及和或；、﹑/+]*)", project_operation_mode)
        real_res = set()
        for i in xrange(len(res)):
            res_item = res[i].strip()
            if i % 2 == 1 or len(res_item) <= 1:
                continue
            if res[i].find(u'等') > -1:
                res_item = res[i][res[i].find(u'等') + 1: ]
            real_res.add(res_item)
        return ",".join(sorted(real_res))

    def format_ppp_implementation_phase(self, item):
        """处理ppp_implementation_phase字段，将百分比形式转化为文字描述类型进度"""
        ppp_implementation_phase = item.get('ppp_implementation_phase', u'')
        ppp_implementation_phase = ppp_implementation_phase.replace(u'PPP', u'')
        p_name = ppp_implementation_phase.strip()
        if p_name == u"20%":
            p_name = u"识别阶段"
        elif p_name == u"40%":
            p_name = u"准备阶段"
        elif p_name == u"60%":
            p_name = u"采购阶段"
        elif p_name == u"80%":
            p_name = u"执行阶段"
        elif p_name == u"100%":
            p_name = u"移交阶段"
        return p_name

    def format_term_of_cooperation(self, item, key):
        """如果不能存在数字的进行格式化"""
        value = item.get(key, u'')
        res = re.findall(u'(\d+)', value)
        if len(res) > 0:
            return value
        return u''




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

    topic_id = 33
    from i_entity_extractor.entity_extractor_route import EntityExtractorRoute
    from i_entity_extractor.common_parser_lib.mongo import MongDb
    import json

    route = EntityExtractorRoute(conf)
    topic_info = route.all_topics.get(topic_id, None)
    begin_time = time.time()
    obj = PppProjectExtractor(topic_info, log)
    extract_data = {
    "demonstration_project_batch" : "",
    "money_unit" : "元",
    "_record_id" : "c81fd13bd09d7248857d2a5f6d8b968d",
    "reward_mechanism" : "可行性缺口补助",
    "city" : "邢台市",
    "district" : "清河县",
    "contacts" : "唐洪刚",
    "demonstration_project_level" : "其它",
    "_in_time" : "2016-10-14 17:42:45",
    "content" : "邢台市清河县生活垃圾焚烧发电项目 \r\n\r\n\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t所在地区\r\n\t\t\t\t\t\t\t河北省\r\n\t\t\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t\t\t--> 邢台市\t\r\n\t\t\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t\t\t--> 清河县\t\r\n\t\t\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t所属行业\r\n\t\t\t\t\t\t\t能源->垃圾发电\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t投资金额\r\n\t\t\t\t\t\t\t        30,000万元\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\tPPP实施阶段\r\n\t\t\t\t\t\t\t识别阶段\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t拟合作期限\r\n\t\t\t\t\t\t\t30年\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t项目运作方式\r\n\t\t\t\t\t\t\tBOT\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t回报机制\r\n\t\t\t\t\t\t\t可行性缺口补助\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t发起时间\r\n\t\t\t\t\t\t\t2015-01-01\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t发起类型\r\n\t\t\t\t\t\t\t政府发起\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t项目概况\r\n\t\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t\t　清河县生活垃圾焚烧发电项目，生产规模为日处理生活垃圾600吨：其中一期规模是日处理生活垃圾400吨/日，垃圾保底量是300吨/日。在不违反国家法律、法规、政策前提下，社会资本自筹资金完成本项目的建设。社会资本选择方式为竞争性谈判，计划特许经营期为三十年（不含建设期），自生活垃圾焚烧发电设施投入商业运营之日起开始计算。项目在建设、运营阶段的权属归社会资本方所有，运营期结束阶段的权属归政府所有。\r\n\t\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t联系人\r\n\t\t\t\t\t\t\t唐洪刚\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t联系人电话\r\n\t\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t示范/推介项目级别\r\n\t\t\t\t\t\t\t其它\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t示范项目批次",
    "location" : "河北省\r\n\t\t\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t\t\t--> 邢台市\t\r\n\t\t\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t\t\r\n\t\t\t\t\t\t\t\t\t--> 清河县",
    "project_id" : "",
    "contact_phone" : "",
    "_utime" : "2017-02-14 20:30:23",
    "project_overview" : "清河县生活垃圾焚烧发电项目，生产规模为日处理生活垃圾600吨：其中一期规模是日处理生活垃圾400吨/日，垃圾保底量是300吨/日。在不违反国家法律、法规、政策前提下，社会资本自筹资金完成本项目的建设。社会资本选择方式为竞争性谈判，计划特许经营期为三十年（不含建设期），自生活垃圾焚烧发电设施投入商业运营之日起开始计算。项目在建设、运营阶段的权属归社会资本方所有，运营期结束阶段的权属归政府所有。",
    "province" : "河北省",
    "project_operation_mode" : "BOT",
    "ppp_implementation_phase" : "识别阶段",
    "term_of_cooperation" : "30年",
    "investment_amount" : "30,000万元",
    "industry" : "能源->垃圾发电",
    "launch_time" : "2015-01-01 00:00:00",
    "project" : "邢台市清河县生活垃圾焚烧发电项目",
    "launch_type" : "政府发起"
}
    entity_data = obj.format_extract_data(extract_data,topic_id)

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
    keys = ['demonstration_project_batch', 'reward_mechanism', 'city', 'district', 'contacts', 'amounts', 'demonstration_project_level', 'content', 'location', 'term_of_cooperation', 'units', 'project_id', 'contact_phone', 'project_overview', 'province', 'project_operation_mode', 'ppp_implementation_phase', 'investment_amount', 'industry', 'launch_time', 'project', 'launch_type', '_in_time', '_src', '_record_id', '_id']

    transfer_data(keys, 'ppp_project')
    print keys

