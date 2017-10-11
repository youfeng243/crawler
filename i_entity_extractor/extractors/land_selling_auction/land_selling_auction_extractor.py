# coding=utf-8
# 土地出让公告实体解析 land_selling_auction

import json
import sys
import re

sys.path.append("..")
sys.path.append("../../")
sys.path.append("../../../")

from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from pyquery import PyQuery as py
import copy


class LandSellingAuctionExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.info_dic = {
            u"宗地编号": u"code",
            u"宗地总面积": u"acreage",
            u"宗地面积": u"acreage",
            u"宗地坐落": u"address",
            u"出让年限": u"land_use_year",
            u"保证金": u"margin",
            u"起始价": u"starting_price",
            u"容积率": u"volume_ratio"
        }

    def format_extract_data(self, extract_data, topic_id):
        '''格式化数据'''
        entity_data = copy.deepcopy(extract_data)

        content = entity_data.get('content', u'')

        if content == u'': return {}
        tables = py(content, parser='html').find('#tdContent').find('table').find('table').items()

        lst_sub_model = []
        for table in tables:
            model = {}
            for tr in table.find('tr').items():
                tds = tr.find('td')
                if len(tds) == 0 or len(tds) % 2 == 1: continue
                model.update(self._get_info(tds))
            starting_price = model.get('starting_price', u'')
            starting_price_amount = self.parser_tool.money_parser.new_trans_money(starting_price, u"万")
            model['starting_price'] = starting_price_amount[0]
            model['starting_price_unit'] = starting_price_amount[1]
            model['starting_price_ccy'] = starting_price_amount[2]

            margin = model.get('margin', u'')
            margin_amount = self.parser_tool.money_parser.new_trans_money(margin, u"万")
            model['margin'] = margin_amount[0]
            model['margin_unit'] = margin_amount[1]
            model['margin_ccy'] = margin_amount[2]

            if len(model.keys()) < 2: continue
            lst_sub_model.append(model)

        entity_data["land_basic_info"] = lst_sub_model
        for land in lst_sub_model:
            for key,value in land.items():
                entity_data[key] = value

        return entity_data

    def _get_info (self,tds):
        lst_value = tds.filter (lambda i: i % 2 == 1).map (lambda i , e: py (e).text ())
        lst_title = tds.filter (lambda i: i % 2 == 0).map (lambda i , e: py (e).text ())
        map_title_value = zip (lst_title , lst_value)
        model = {}
        for k_title , v_value in map_title_value:
            k_title = k_title.replace (u'：' , u'')
            if k_title == u'':
                continue
            key = self.info_dic.get (k_title , None)
            if key is None: continue
            model [ key ] = v_value
        return model



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

    topic_id = 161
    from i_entity_extractor.entity_extractor_route import EntityExtractorRoute

    import json

    route = EntityExtractorRoute(conf)
    topic_info = route.all_topics.get(topic_id, None)
    begin_time = time.time()
    obj = LandSellingAuctionExtractor(topic_info, log)

    extract_data =  {
  "_site_record_id": "recorderguid=9e24af5b-8f23-4301-9157-5dac71a3858a",
  "_src": [
    {
      "site": "www.landchina.com",
      "site_id": -820196621217744300,
      "url": "http://www.landchina.com/DesktopModule/BizframeExtendMdl/workList/bulWorkView.aspx?wmguid=f11f72ad-7d51-4c51-870f-d6c147116184&recorderguid=9e24af5b-8f23-4301-9157-5dac71a3858a&sitePath=5fc0dd8a41417747b0ac75334ed25db1"
    }
  ],

  "content": "<table width=\"612\" cellspacing=\"0\" cellpadding=\"0\" border=\"0\" class=\"tablebox02 mt10\">\r\n                      <tbody>\r\n                      <tr>\r\n                          <td>\n<span class=\"gray2\">地区：</span><a href=\"/market/330000__________1.html\" class=\"blue01\" target=\"_blank\">浙江</a>\n</td>\r\n                          <td>\n<span class=\"gray2\">所在地：</span><a href=\"/market/330200__________1.html\" class=\"blue01\" target=\"_blank\">宁波</a>\n</td>\r\n                      </tr>\r\n                      <tr>\r\n                        <td>\n<span class=\"gray2\">总面积：</span><em class=\"red01\">131838</em> 平方米</td>\r\n                        <td>\n<span class=\"gray2\">建设用地面积：</span><em class=\"red01\">131838</em>平方米</td>\r\n                      </tr>\r\n                      <tr>\r\n                        <td>\n<span class=\"gray2\">规划建筑面积：</span><em class=\"red01\">197757</em> 平方米</td>\r\n                        <td>\n<span class=\"gray2\">代征面积：</span><em class=\"red01\">暂无</em>\n</td>\r\n                      </tr>\r\n                      <tr>\r\n                        <td>\n<span class=\"gray2\">容积率：</span>1.0-1.5</td>\r\n                        <td>\n<span class=\"gray2\">绿化率：</span>暂无</td>\r\n                      </tr>\r\n                      <tr>\r\n                        <td>\n<span class=\"gray2\">商业比例：</span>暂无</td>\r\n                        <td>\n<span class=\"gray2\">建筑密度：</span>50</td>\r\n                      </tr>\r\n                      <tr>\r\n                        <td>\n<span class=\"gray2\">限制高度：</span>24</td>\r\n                        <td>\n<span class=\"gray2\">出让形式：</span>挂牌</td>\r\n                      </tr>\r\n                      <tr>\r\n                        <td>\n<span class=\"gray2\">出让年限：</span>50</td>\r\n                        <td>\n<span class=\"gray2\">位置：</span>宁波江北高新技术产业园区</td>\r\n                      </tr>\r\n                      <tr>\r\n                        <td title=\"东至杨枝桥路、南至庆丰路、西至浦丰路、北至畅阳路\">\n<span class=\"gray2\">四至：</span>东至杨枝桥路、南至庆丰路、西至浦丰路...</td>\r\n                        <td>\n<span class=\"gray2\">规划用途：</span><a href=\"/market/__________1.html\" class=\"blue01\" target=\"_blank\">工业</a>\n</td>\r\n                      </tr>\r\n                    </tbody>\n</table>\r\n             \n",
  }
    entity_data = obj.format_extract_data(extract_data,topic_id)
    print json.dumps(entity_data,ensure_ascii=False,encoding='UTF-8')
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

