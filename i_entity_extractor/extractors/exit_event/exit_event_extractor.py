# coding=utf8
import sys
import re
import time
sys.path.append('../../')
from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib import etl_tool
import copy
import json


class ExitEventExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)


    def format_extract_data(self, extract_data, topic_id):
        """实体解析抽取数据"""
        item = copy.deepcopy(extract_data)

        # 退出时间 divestment_date
        divestment_date = item.get('divestment_date', '')
        item['divestment_date'] = etl_tool.regex_remove_time(divestment_date).strip()

        # 首次投资时间 first_investment_date
        first_investment_date = item.get('first_investment_date', '')
        item['first_investment_date'] = etl_tool.regex_remove_time(first_investment_date).strip()

        # 累计投资金额
        cumulative_investment_amount = item.get('cumulative_investment_amount', '')
        res_cumulative_investment_amount = self.parser_tool.money_parser.new_trans_money(cumulative_investment_amount, u"万")
        item['amounts'] = res_cumulative_investment_amount[0]
        item['units'] = res_cumulative_investment_amount[1]
        item['currency'] = res_cumulative_investment_amount[2]

        # 企业全名
        content = {'content': item.get('describe', '') + item.get('title', '')}
        company_entity = json.dumps(content, ensure_ascii=False, encoding='utf8')
        company_entity = json.loads(company_entity)
        enterprise_name_list = company_entity.get('entitylink', {}).keys()
        sort_name = item.get('enterprise_short_name', '')
        full_name = ''
        for name in enterprise_name_list:
            if set(list(unicode(sort_name))).issubset(set(list(unicode(name)))):
                full_name = name
                break
        if not item.get('enterprise_full_name', ''):
            item['enterprise_full_name'] = full_name

        return item



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
    obj = ExitEventExtractor(topic_info, log)
    extract_data = {
    "source_site" : "私募通",
    "describe" : "2014年1月21日,于常州光洋轴承股份有限公司上市之时，苏州德晟亨风创业投资合伙企业（有限合伙）（管理机构：苏州元禾控股有限公司）获得退出，账面退出回报为1.99倍。",
    "currency" : "人民币",
    "first_investment_date" : "2007-11-01",
    "_record_id" : "4caf0c95546b5968e63df4cabcc5d8a6",
    "current_rewards_multiple" : "47.97",
    "cumulative_investment_amount" : "",
    "title" : "Tenaya Capital V, L.P.退出去哪儿网",
    "amounts" : "0.0",
    "_in_time" : "2016-11-11 17:33:38",
    "units" : "万",
    "internal_rewards_rate" : "90.45%",
    "_utime" : "2017-03-28 15:58:55",
    "enterprise_short_name" : "光洋股份",
    "enterprise_full_name" : "亚洲投资有限公司",
    "divestment_date" : "2013-11-01",
    "divestment_amount" : "",
    "url" : "http://exit.pedata.cn/exit/2468171.html",
    "divestment_short_name" : "特纳亚（原雷曼兄弟）",
    "divestment_mode" : "IPO"
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

    keys = ['divestment_date', 'describe', 'source_site', 'current_rewards_multiple', 'cumulative_investment_amount', 'title', 'url', 'divestment_short_name', 'amounts', 'units', 'currency', 'enterprise_short_name', 'enterprise_full_name', 'first_investment_date', 'divestment_mode', 'internal_rewards_rate', 'divestment_amount', '_in_time', '_src', '_record_id', '_id']
    # transfer_data(keys, 'exit_event')
    print keys

