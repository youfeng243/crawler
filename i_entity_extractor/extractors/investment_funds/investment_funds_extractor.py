# coding=utf8
import sys
import re
import time
sys.path.append('../../')
from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib import etl_tool
import copy


class InvestmentFundsExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)

    def format_extract_data(self, extract_data, topic_id):
        """实体解析抽取数据"""
        item = copy.deepcopy(extract_data)

        # 电话,姓名,邮箱,地址
        lst_format = [u'phone', u'email', u'contacts', u'address']
        for item_format in lst_format:
            item[item_format] = item.get(item_format, u'').replace(u'- -', u'')

        # 目标规模
        target_scale = item.get(u'target_scale', u'')
        target_scale = self.parser_tool.money_parser.new_trans_money(target_scale, u"万", False)
        item['amounts'] = target_scale[0]
        item['units'] = target_scale[1]
        item['currency'] = target_scale[2]

        # 成立时间
        virtual_investment_region = item.get(u'virtual_investment_region', u'').replace(u'- -', u'')
        res_virtual_investment_region = etl_tool.str2datetime(virtual_investment_region, '%Y-%m-%d %H:%M:%S')
        item['virtual_investment_region'] = res_virtual_investment_region if res_virtual_investment_region == None else res_virtual_investment_region.strftime("%Y-%m-%d")
        return item



if __name__ == '__main__':
    import pytoml
    import sys
    import json

    sys.path.append('../../')
    from conf import get_config
    from bdp.i_crawler.i_extractor.ttypes import BaseInfo, CrawlInfo, ExtractInfo, PageParseInfo

    with open('../../entity.toml', 'rb') as config:
        config = pytoml.load(config)
    conf = get_config(config)
    import common

    topic_id = 32
    from entity_extractor_route import EntityExtractorRoute
    from i_entity_extractor.common_parser_lib.mongo import MongDb
    import traceback

    route = EntityExtractorRoute()
    topic_info = route.all_topics.get(topic_id, None)
    begin_time = time.time()
    print "start"
    obj = InvestmentFundsExtractor(topic_info, common.log)
    db_reader = MongDb('172.16.215.2', 40042, 'final_data', 'readme', 'readme')
    cursor = db_reader.db['investment_funds'].find({})
    num = 0
    for item in cursor:
        num += 1
        try:
            src_url = item.get("_src")[0]['url']
            extract_data = {
                "target_scale": item.get("reward_mechanism", ""),
                "virtual_investment_region": item.get("project_operation_mode", ""),
                "demonstration_project_level": item.get("demonstration_project_level", "")
            }
            data = json.dumps(extract_data)
            extract_info = ExtractInfo(ex_status=2, extract_data=data)
            base_info = BaseInfo(url=src_url)
            parser_info = PageParseInfo(base_info=base_info, extract_info=extract_info)
            entity_data = obj.entity_extract(parser_info, item)
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
        except Exception:
            print traceback.format_exc()
            pass
        break
    keys = ['source_site', 'funds_collects', 'currency', 'virtual_investment_industry', 'limited_partners', 'city', 'contacts', 'amounts', 'fund_status', 'units', 'setup_date', 'email', 'virtual_investment_step', 'profile', 'fax', 'fund_name', 'phone', 'address', 'enterprise_name', 'fund_type', 'region', 'target_scale', 'virtual_investment_region', '_in_time', '_src', '_record_id', '_id']
    transfer_data(keys, 'investment_funds')
    print keys