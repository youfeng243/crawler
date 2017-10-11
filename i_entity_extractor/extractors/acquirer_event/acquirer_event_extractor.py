# coding=utf8
import sys
import re
import time
sys.path.append('../../')
sys.path.append('../../../')
sys.path.append('../../../..')
from crawler.i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from crawler.i_entity_extractor.common_parser_lib import etl_tool
from crawler.i_entity_extractor.common_parser_lib import toolsutil as tool

import copy


class AcquirerEventExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)



    def format_extract_data(self, extract_data, topic_id):
        """实体解析抽取数据"""
        item = copy.deepcopy(extract_data)

        # 金额
        amount = item.get(u'amount', u'')
        res_amount = self.parser_tool.money_parser.new_trans_money(amount, u"万", False)
        item['amount'] = res_amount[0]
        item['units'] = res_amount[1]
        item['currency'] = res_amount[2]

        # 开始时间
        begin_date = item.get(u'begin_date', u'')
        begin_date = tool.norm_date(begin_date)
        item["begin_date"] = begin_date.split(' ')[0] if begin_date != None else ''

        # 结束时间
        end_date = item.get(u'end_date', u'')
        end_date = tool.norm_date(end_date)
        item["end_date"] = end_date.split(' ')[0] if end_date != None else ''

        industry = item.get('industry', '')
        lst_industry = etl_tool.regex_chinese(industry)
        item['industry'] = '/'.join(lst_industry)
        # for idx in xrange(len(lst_industry)):
        #     item['industry_' + str(idx + 1)] = lst_industry[idx]

        # IT桔子并购状态
        source_site = item.get('_src', [{}])[0].get('site', u'')
        if source_site == u'www.itjuzi.com':
            item['status'] = u'已完成'
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
    obj = AcquirerEventExtractor(topic_info, log)
    extract_data = {
  "_site_record_id": "http://zdb.pedaily.cn/ma/show44301/",
  "_src": [
    {
      "site": "zdb.pedaily.cn",
      "site_id": 2858455580731584500,
      "url": "http://zdb.pedaily.cn/ma/show44301/"
    }
  ],
  "acquirer_full_name": "深圳能源集团股份有限公司",
  "acquirered_full_name": "Newton Industrial Limited",
  "begin_date": "2017-03-09",
  "describe": "深圳能源收购Newton",
  "end_date": "",
  "industry": "能源及矿产\t电力、燃气及水的生产和供应业",
  "involved_stakes": "0.00 %",
  "is_national": "",
  "mode": "",
  "source_site": "投资界",
  "status": "进行中",
  "url": "http://zdb.pedaily.cn/ma/show44301/"
}
    entity_data = obj.format_extract_data(extract_data,topic_id)
    entity_data['acquirer'] = {'acquirer_message_lists': entity_data.get('acquirer_message_lists', []), 'acquirer_contact_list': entity_data.get('acquirer_contact_lists', [])}
    entity_data['acquirered'] = {'acquirer_message_lists': entity_data.get('acquirered_message_lists', []), 'acquirer_contact_list': entity_data.get('acquirered_contact_lists', [])}

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

    keys = ['status', 'acquirer_short_name', 'begin_date', 'end_date', 'is_national', 'acquirered_full_name', 'url', 'industry', 'describe', 'acquirered_short_name', 'acquirer_full_name', 'currency', 'amount', 'mode', 'units', 'source_site', 'involved_stakes', 'acquirered', 'acquirer', '_in_time', '_src', '_record_id', '_id']
    # transfer_data(keys, 'acquirer_event')
    print keys