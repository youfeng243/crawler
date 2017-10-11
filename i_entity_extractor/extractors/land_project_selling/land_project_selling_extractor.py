# coding=utf-8
# 土地项目转让实体解析 land_project_selling

import json
import sys
import re

sys.path.append("..")
sys.path.append("../../")
sys.path.append("../../../")
sys.path.append("../../../..")

from crawler.i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from pyquery import PyQuery as py
from crawler.i_entity_extractor.common_parser_lib import toolsutil
import copy


class LandProjectSellingExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)

    def format_extract_data(self,extract_data, topic_id):
        '''格式化数据'''
        entity_data = copy.deepcopy(extract_data)

        # 处理金额
        transfer_price = entity_data.get(u'transfer_price', u'')
        transfer_price = self.parser_tool.money_parser.new_trans_money(transfer_price, u"万", True)
        entity_data[u'transfer_price'] = transfer_price[0]
        entity_data[u'transfer_price_unit'] = transfer_price[1]
        entity_data[u'transfer_price_ccy'] = transfer_price[2]

        update_date = entity_data.get(u'update_date', u'')
        update_date = toolsutil.norm_date(update_date)
        entity_data[u"update_date"] = update_date

        # 面积单位
        acreage = extract_data.get('acreage', '')
        acreage_unit = re.sub(u'\d|\.', '', acreage)
        entity_data['acreage_unites'] = acreage_unit

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

    topic_id = 106
    from i_entity_extractor.entity_extractor_route import EntityExtractorRoute
    from i_entity_extractor.common_parser_lib.mongo import MongDb
    import json

    route = EntityExtractorRoute(conf)
    topic_info = route.all_topics.get(topic_id, None)
    begin_time = time.time()
    obj = LandProjectSellingExtractor(topic_info, log)

    extract_data = {
  "_site_record_id": "http://land.fang.com/financing/100320.html",
  "_src": [
    {
      "site": "land.fang.com",
      "site_id": -3177588227352288000,
      "url": "http://land.fang.com/financing/100320.html"
    }
  ],
  "acreage": "113339㎡",
  "belongs": "",
  "city": "邢台",
  "contacts": "马莹(实名)",
  "mode": "项目融资",
  "planning_purposes": "商业办公",
  "province": "河北",
  "region": "河北\t邢台",
  "service_life": "40年",
  "source_site": "房天下",
  "transfer_price": "面议",
  "update_date": "2014/4/6 19:08:21",
  "url": "http://land.fang.com/financing/100320.html"
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
