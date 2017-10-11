# coding=utf-8
# 网贷黑名单实体解析 net_loan_blacklist

import json
import sys
import re

sys.path.append("..")
sys.path.append("../../")

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
import copy
from i_entity_extractor.common_parser_lib import toolsutil


class NetloanBlacklistExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)


    def format_extract_data(self, extract_data, topic_id):
        '''格式化数据'''
        entity_data = copy.deepcopy(extract_data)

        # 处理借贷款时间
        borrow_date = entity_data.get(u'borrow_date', u'')
        borrow_date = toolsutil.str2datetime(borrow_date, '%Y-%m-%d %H:%M:%S')
        entity_data["borrow_date"] = borrow_date if borrow_date == None else borrow_date.strftime("%Y-%m-%d")
        # 处理更新时间
        update_date = entity_data.get(u'update_date', u'')
        update_date = toolsutil.str2datetime(update_date, '%Y-%m-%d %H:%M:%S')
        entity_data["update_date"] = update_date if update_date == None else update_date.strftime("%Y-%m-%d")
        # 处理承诺还款时间
        committed_repayment_time = entity_data.get(u'committed_repayment_time', u'')
        committed_repayment_time = toolsutil.str2datetime(committed_repayment_time, '%Y-%m-%d %H:%M:%S')
        entity_data[
            "committed_repayment_time"] = committed_repayment_time if committed_repayment_time == None else committed_repayment_time.strftime(
            "%Y-%m-%d")

        return entity_data




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

    topic_id = 124
    from i_entity_extractor.entity_extractor_route import EntityExtractorRoute
    from i_entity_extractor.common_parser_lib.mongo import MongDb
    import json

    route = EntityExtractorRoute(conf)
    topic_info = route.all_topics.get(topic_id, None)
    begin_time = time.time()
    obj = NetloanBlacklistExtractor(topic_info, log)
    extract_data = {
        "_site_record_id": "http://www.chalaolai.com/laolai-content.asp?newid=3507",
        "_src": [
            {
                "site": "www.chalaolai.com",
                "site_id": 72177616690229180,
                "url": "http://www.chalaolai.com/laolai-content.asp?newid=3507"
            }
        ],
        "alipay": "",
        "amount": "",
        "bank": "",
        "bank_card_id": "",
        "borrow_date": "2012/4/25",
        "card_id": "3506001971****0016",
        "committed_repayment_time": "2016-9-2",
        "content": "",
        "delay_count": "",
        "delay_date": "273",
        "delay_principal": "",
        "document_location": "",
        "email_address": "",
        "loan_term": "",
        "name": "苏彤雯",
        "other_contact": "",
        "phone": "",
        "platform": "",
        "repayment_count": "",
        "residence_location": "福建省漳州市芗城区南太武7号",
        "source_site": "弘正道",
        "status": "",
        "update_date": "2016/9/5]",
        "url": "【曝光】“老赖”苏彤雯",
        "website": ""
    }



    entity_data = obj.format_extract_data(extract_data, topic_id)
    entity_data = obj.after_process(entity_data)
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


