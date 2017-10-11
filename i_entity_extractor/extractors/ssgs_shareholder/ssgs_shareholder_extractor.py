# coding=utf-8
# 上市股东信息实体解析


from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib import toolsutil
import pandas
import copy

class SsgsShareholderExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.shareholders_map_conf = {
            'nan': "year_quarter",
            u"股东人数(户)": "shareholder_number",
            u"较上期变化(%)": "shareholder_number_compared_previous_change",
            u"人均流通股(股)": "per_capita_float",
            u"人均流通股较上期变化": "per_capita_float_compared_previous_change",
            u"筹码集中度": "chip_concentration",
            u"股价(元)": "share_price",
            u"人均持股金额(元)": "capita_holdings_amount_pre",
            u"前十大股东持股合计(%)": "top10_shareholders_aggregate",
            u"前十大流通股东持股合计(%)": "top10_holdings_total_circulation",
        }

    def format_extract_data(self, extract_data, topic_id):
        '''实体解析抽取数据'''
        entity_data = copy.deepcopy(extract_data)

        top10_shareholders_info     = extract_data.get("top10_shareholders_info")
        top10_shareholders_year     = extract_data.get("top10_shareholders_year")
        ten_outstanding_shares_info = extract_data.get("ten_outstanding_shares_info")
        ten_outstanding_shares_year = extract_data.get("ten_outstanding_shares_year")

        fund_table_list = []
        if entity_data.has_key("fund_table_info") and entity_data.has_key("fund_table_year"):
            fund_table_info = extract_data.get("fund_table_info")
            fund_table_year = extract_data.get("fund_table_year")
            if len(fund_table_info) == len(fund_table_year):
                num = len(fund_table_info)
                for index in range(num):
                    for key in fund_table_year[index].keys():
                        fund_info = fund_table_info[index].get('fund_info')
                        if isinstance(fund_info, list):
                            for index_num in range(len(fund_info)):
                                if key not in fund_info[index_num].keys():
                                    fund_info[index_num][key] = top10_shareholders_year[index][key]
                    for item in fund_info:
                        fund_table_list.append(item)
            else:
                self.log.error(
                    "fund_table_info_len not euqal fund_table_year_len, fund_table_info_len:%s\tfund_table_year_len:%s" % (
                    len(fund_table_info), len(fund_table_year)))

        top10_shareholders_list = []
        if entity_data.has_key("top10_shareholders_info") and entity_data.has_key("top10_shareholders_year"):
            if len(top10_shareholders_info) == len(top10_shareholders_year):
                num = len(top10_shareholders_info)
                for index in range(num):
                    for key in top10_shareholders_year[index].keys():
                        shareholders = top10_shareholders_info[index].get('shareholders')
                        if isinstance(shareholders, list):
                            for index_num in range(len(shareholders)):
                                if key not in shareholders[index_num].keys():
                                    shareholders[index_num][key] = top10_shareholders_year[index][key]
                    for item in shareholders:
                        top10_shareholders_list.append(item)
            else:
                self.log.error(
                    "top10_shareholders_info_len not euqal top10_shareholders_year_len, top10_shareholders_info_len:%s\ttop10_shareholders_year_len:%s" % (
                    len(top10_shareholders_info), len(top10_shareholders_year)))

        ten_outstanding_shares_list = []
        if entity_data.has_key("top10_shareholders_info") and entity_data.has_key("ten_outstanding_shares_year"):
            if len(ten_outstanding_shares_info) == len(ten_outstanding_shares_year):
                num = len(ten_outstanding_shares_info)
                for index in range(num):
                    for key in ten_outstanding_shares_year[index].keys():
                        shareholders = ten_outstanding_shares_info[index].get('shareholders')
                        if isinstance(shareholders, list):
                            for index_num in range(len(shareholders)):
                                if key not in shareholders[index_num].keys():
                                    shareholders[index_num][key] = top10_shareholders_year[index][key]
                    for item in shareholders:
                        ten_outstanding_shares_list.append(item)
            else:
                self.log.error(
                    "ten_outstanding_shares_info_len not euqal ten_outstanding_shares_year_len, ten_outstanding_shares_info_len:%s\tten_outstanding_shares_year_len:%s" % (
                    len(ten_outstanding_shares_info), len(ten_outstanding_shares_year)))

        code = extract_data.get('code')
        stock_code = None
        if code:
            stock_code = toolsutil.re_find_one('\d+', extract_data.get('code'))

        shareholder_number_table = extract_data.get('shareholder_number_table')
        shareholder_number_list = []

        if isinstance(shareholder_number_table, basestring):
            net_table_data = toolsutil.html2excel(shareholder_number_table)
            if isinstance(net_table_data, pandas.core.frame.DataFrame):
                net_table_data = net_table_data.T.values
                head_keys = net_table_data[0]
                for index in range(len(net_table_data)):
                    if index == 0:
                        continue
                    tmp_map = {}
                    for index_num in range(len(net_table_data[index])):
                        key = head_keys[index_num]
                        tmp_map[key] = net_table_data[index][index_num]
                    shareholder_number_list.append(tmp_map)

        shareholder_number_list = self.format_shareholder_number_info(shareholder_number_list)

        entity_data["ten_outstanding_shares"] = ten_outstanding_shares_list
        entity_data["top10_shareholders"] = top10_shareholders_list
        entity_data["fund_table"] = fund_table_list
        entity_data["code"] = code
        entity_data["shareholder_number_table"] = shareholder_number_list
        entity_data["stock_code"] = stock_code
        return entity_data

    def format_shareholder_number_info(self, shareholder_number_list):
        '''格式化shareholder_number_list'''
        result_list = []
        for item in shareholder_number_list:
            tmp_map = {}
            for key, value in item.items():
                if isinstance(key, float):
                    key = str(key)
                tmp_map[self.shareholders_map_conf.get(key, '')] = value
            result_list.append(tmp_map)
        return result_list




if __name__ == "__main__":
    import pytoml
    import sys
    import time
    from common.log import log

    sys.path.append('../../')

    with open('../../entity.toml', 'rb') as config:
        conf = pytoml.load(config)

    log.init_log(conf, console_out=conf['logger']['console'])
    conf['log'] = log

    topic_id = 123
    from i_entity_extractor.entity_extractor_route import EntityExtractorRoute
    from i_entity_extractor.common_parser_lib.mongo import MongDb
    import json

    route = EntityExtractorRoute(conf)
    topic_info = route.all_topics.get(topic_id, None)
    begin_time = time.time()
    obj = SsgsShareholderExtractor(topic_info, log)

    extract_data = {
  "_site_record_id": "sz002432",
  "_src": [
    {
      "site": "f10.eastmoney.com",
      "site_id": -2648107288641591300,
      "url": "http://f10.eastmoney.com/f10_v2/ShareholderResearch.aspx?code=sz002432"
    }
  ],
  "code": "sz002432",
  "company_name": "九安医疗",
  "fund_table_info": [
    {
      "fund_info": [
        {
          "circulation_ratio": "0.22%",
          "fund_code": "160219",
          "link": "购买",
          "name_fund": "国泰国证医药卫生行业指数分级",
          "net_worth_ratio": "0.20%",
          "number_shares": "829,348",
          "position_value": "11,693,806",
          "ranking": "1",
          "total_equity_ratio": "0.19%"
        }
      ]
    },
    {
      "fund_info": [
        {
          "circulation_ratio": "1.77%",
          "fund_code": "501015",
          "link": "购买",
          "name_fund": "财通多策略升级混合",
          "net_worth_ratio": "2.12%",
          "number_shares": "6,578,950",
          "position_value": "102,171,093",
          "ranking": "1",
          "total_equity_ratio": "1.52%"
        },
        {
          "circulation_ratio": "0.28%",
          "fund_code": "320003",
          "link": "购买",
          "name_fund": "诺安先锋混合",
          "net_worth_ratio": "0.40%",
          "number_shares": "1,058,588",
          "position_value": "16,439,871",
          "ranking": "2",
          "total_equity_ratio": "0.24%"
        },
        {
          "circulation_ratio": "0.14%",
          "fund_code": "320001",
          "link": "购买",
          "name_fund": "诺安平衡混合",
          "net_worth_ratio": "0.52%",
          "number_shares": "529,294",
          "position_value": "8,219,935",
          "ranking": "3",
          "total_equity_ratio": "0.12%"
        },
        {
          "circulation_ratio": "0.13%",
          "fund_code": "000219",
          "link": "购买",
          "name_fund": "博时裕益混合",
          "net_worth_ratio": "5.33%",
          "number_shares": "487,701",
          "position_value": "7,573,996",
          "ranking": "4",
          "total_equity_ratio": "0.11%"
        },
        {
          "circulation_ratio": "0.12%",
          "fund_code": "162412",
          "link": "购买",
          "name_fund": "华宝兴业中证医疗指数分级",
          "net_worth_ratio": "1.31%",
          "number_shares": "443,488",
          "position_value": "6,887,368",
          "ranking": "5",
          "total_equity_ratio": "0.10%"
        },
        {
          "circulation_ratio": "0.04%",
          "fund_code": "163109",
          "link": "购买",
          "name_fund": "申万菱信深证成指分级",
          "net_worth_ratio": "0.06%",
          "number_shares": "137,717",
          "position_value": "2,138,745",
          "ranking": "6",
          "total_equity_ratio": "0.03%"
        },
        {
          "circulation_ratio": "0.03%",
          "fund_code": "159938",
          "link": "购买",
          "name_fund": "广发中证全指医药卫生ETF",
          "net_worth_ratio": "0.24%",
          "number_shares": "102,813",
          "position_value": "1,596,685",
          "ranking": "7",
          "total_equity_ratio": "0.02%"
        },
        {
          "circulation_ratio": "0.02%",
          "fund_code": "502056",
          "link": "购买",
          "name_fund": "广发医疗指数分级",
          "net_worth_ratio": "1.32%",
          "number_shares": "84,463",
          "position_value": "1,311,710",
          "ranking": "8",
          "total_equity_ratio": "0.02%"
        },
        {
          "circulation_ratio": "0.01%",
          "fund_code": "159907",
          "link": "购买",
          "name_fund": "广发中小板300ETF",
          "net_worth_ratio": "0.14%",
          "number_shares": "23,900",
          "position_value": "371,167",
          "ranking": "9",
          "total_equity_ratio": "0.01%"
        },
        {
          "circulation_ratio": "0.01%",
          "fund_code": "159903",
          "link": "购买",
          "name_fund": "南方深证成份ETF",
          "net_worth_ratio": "0.06%",
          "number_shares": "19,900",
          "position_value": "309,047",
          "ranking": "10",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "159943",
          "link": "购买",
          "name_fund": "大成深证成份ETF",
          "net_worth_ratio": "0.06%",
          "number_shares": "17,900",
          "position_value": "277,987",
          "ranking": "11",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "159918",
          "link": "购买",
          "name_fund": "嘉实中创400ETF",
          "net_worth_ratio": "0.14%",
          "number_shares": "14,088",
          "position_value": "218,786",
          "ranking": "12",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "512100",
          "link": "购买",
          "name_fund": "南方中证1000ETF",
          "net_worth_ratio": "0.07%",
          "number_shares": "7,600",
          "position_value": "118,028",
          "ranking": "13",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "161612",
          "link": "购买",
          "name_fund": "融通深证成份指数",
          "net_worth_ratio": "0.06%",
          "number_shares": "6,302",
          "position_value": "97,870",
          "ranking": "14",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "162413",
          "link": "购买",
          "name_fund": "华宝兴业中证1000指数分级",
          "net_worth_ratio": "0.07%",
          "number_shares": "4,600",
          "position_value": "71,438",
          "ranking": "15",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "002269",
          "link": "购买",
          "name_fund": "银华大数据灵活配置定开混合",
          "net_worth_ratio": "0.05%",
          "number_shares": "3,400",
          "position_value": "52,802",
          "ranking": "16",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "162510",
          "link": "购买",
          "name_fund": "国联安双力中小板综指(LOF)",
          "net_worth_ratio": "0.17%",
          "number_shares": "3,391",
          "position_value": "52,662",
          "ranking": "17",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "162010",
          "link": "购买",
          "name_fund": "长城久兆中小板300指数分级",
          "net_worth_ratio": "0.14%",
          "number_shares": "2,528",
          "position_value": "39,259",
          "ranking": "18",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "001180",
          "link": "购买",
          "name_fund": "广发医药卫生联接A",
          "net_worth_ratio": "0.00%",
          "number_shares": "2,000",
          "position_value": "31,060",
          "ranking": "19",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "270026",
          "link": "购买",
          "name_fund": "广发中小板300联接",
          "net_worth_ratio": "0.00%",
          "number_shares": "200",
          "position_value": "3,106",
          "ranking": "20",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "070030",
          "link": "购买",
          "name_fund": "嘉实中创400ETF联接",
          "net_worth_ratio": "0.00%",
          "number_shares": "100",
          "position_value": "1,553",
          "ranking": "21",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "202017",
          "link": "购买",
          "name_fund": "南方深证成份ETF联接A",
          "net_worth_ratio": "0.00%",
          "number_shares": "100",
          "position_value": "1,553",
          "ranking": "22",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "001319",
          "link": "购买",
          "name_fund": "农银信息传媒股票",
          "net_worth_ratio": "0.00%",
          "number_shares": "87",
          "position_value": "1,351",
          "ranking": "23",
          "total_equity_ratio": "0.00%"
        }
      ]
    },
    {
      "fund_info": [
        {
          "circulation_ratio": "0.41%",
          "fund_code": "001072",
          "link": "购买",
          "name_fund": "华安智能装备主题股票",
          "net_worth_ratio": "1.61%",
          "number_shares": "1,514,838",
          "position_value": "26,024,916",
          "ranking": "1",
          "total_equity_ratio": "0.35%"
        },
        {
          "circulation_ratio": "0.04%",
          "fund_code": "050016",
          "link": "购买",
          "name_fund": "博时宏观回报债券A/B",
          "net_worth_ratio": "0.49%",
          "number_shares": "155,900",
          "position_value": "2,678,362",
          "ranking": "2",
          "total_equity_ratio": "0.04%"
        }
      ]
    },
    {
      "fund_info": [
        {
          "circulation_ratio": "1.77%",
          "fund_code": "501015",
          "link": "购买",
          "name_fund": "财通多策略升级混合",
          "net_worth_ratio": "2.52%",
          "number_shares": "6,578,950",
          "position_value": "119,342,153",
          "ranking": "1",
          "total_equity_ratio": "1.52%"
        },
        {
          "circulation_ratio": "0.47%",
          "fund_code": "160219",
          "link": "购买",
          "name_fund": "国泰国证医药卫生行业指数分级",
          "net_worth_ratio": "0.48%",
          "number_shares": "1,753,966",
          "position_value": "31,816,943",
          "ranking": "2",
          "total_equity_ratio": "0.41%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "320003",
          "link": "购买",
          "name_fund": "诺安先锋混合",
          "net_worth_ratio": "0.00%",
          "number_shares": "1,058,588",
          "position_value": "0",
          "ranking": "3",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.17%",
          "fund_code": "162412",
          "link": "购买",
          "name_fund": "华宝兴业中证医疗指数分级",
          "net_worth_ratio": "1.46%",
          "number_shares": "616,788",
          "position_value": "11,188,534",
          "ranking": "4",
          "total_equity_ratio": "0.14%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "320001",
          "link": "购买",
          "name_fund": "诺安平衡混合",
          "net_worth_ratio": "0.00%",
          "number_shares": "529,294",
          "position_value": "0",
          "ranking": "5",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.12%",
          "fund_code": "001892",
          "link": "购买",
          "name_fund": "长盛新兴成长混合",
          "net_worth_ratio": "0.68%",
          "number_shares": "452,374",
          "position_value": "8,206,064",
          "ranking": "6",
          "total_equity_ratio": "0.10%"
        },
        {
          "circulation_ratio": "0.04%",
          "fund_code": "002085",
          "link": "购买",
          "name_fund": "长盛互联网+混合",
          "net_worth_ratio": "0.83%",
          "number_shares": "146,300",
          "position_value": "2,653,882",
          "ranking": "7",
          "total_equity_ratio": "0.03%"
        },
        {
          "circulation_ratio": "0.04%",
          "fund_code": "163109",
          "link": "购买",
          "name_fund": "申万菱信深证成指分级",
          "net_worth_ratio": "0.08%",
          "number_shares": "132,611",
          "position_value": "2,405,563",
          "ranking": "8",
          "total_equity_ratio": "0.03%"
        },
        {
          "circulation_ratio": "0.03%",
          "fund_code": "159938",
          "link": "购买",
          "name_fund": "广发中证全指医药卫生ETF",
          "net_worth_ratio": "0.35%",
          "number_shares": "127,313",
          "position_value": "2,309,457",
          "ranking": "9",
          "total_equity_ratio": "0.03%"
        },
        {
          "circulation_ratio": "0.03%",
          "fund_code": "502056",
          "link": "购买",
          "name_fund": "广发医疗指数分级",
          "net_worth_ratio": "1.77%",
          "number_shares": "95,464",
          "position_value": "1,731,716",
          "ranking": "10",
          "total_equity_ratio": "0.02%"
        },
        {
          "circulation_ratio": "0.02%",
          "fund_code": "399011",
          "link": "购买",
          "name_fund": "中海医疗保健主题股票",
          "net_worth_ratio": "1.80%",
          "number_shares": "65,800",
          "position_value": "1,193,612",
          "ranking": "11",
          "total_equity_ratio": "0.02%"
        },
        {
          "circulation_ratio": "0.01%",
          "fund_code": "159907",
          "link": "购买",
          "name_fund": "广发中小板300ETF",
          "net_worth_ratio": "0.16%",
          "number_shares": "27,500",
          "position_value": "498,850",
          "ranking": "12",
          "total_equity_ratio": "0.01%"
        },
        {
          "circulation_ratio": "0.01%",
          "fund_code": "159943",
          "link": "购买",
          "name_fund": "大成深证成份ETF",
          "net_worth_ratio": "0.07%",
          "number_shares": "23,200",
          "position_value": "420,848",
          "ranking": "13",
          "total_equity_ratio": "0.01%"
        },
        {
          "circulation_ratio": "0.01%",
          "fund_code": "159903",
          "link": "购买",
          "name_fund": "南方深证成份ETF",
          "net_worth_ratio": "0.07%",
          "number_shares": "22,300",
          "position_value": "404,522",
          "ranking": "14",
          "total_equity_ratio": "0.01%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "159918",
          "link": "购买",
          "name_fund": "嘉实中创400ETF",
          "net_worth_ratio": "0.17%",
          "number_shares": "16,288",
          "position_value": "295,464",
          "ranking": "15",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "161612",
          "link": "购买",
          "name_fund": "融通深证成份指数",
          "net_worth_ratio": "0.07%",
          "number_shares": "6,302",
          "position_value": "114,318",
          "ranking": "16",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "162413",
          "link": "购买",
          "name_fund": "华宝兴业中证1000指数分级",
          "net_worth_ratio": "0.08%",
          "number_shares": "5,500",
          "position_value": "99,770",
          "ranking": "17",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "001180",
          "link": "购买",
          "name_fund": "广发医药卫生联接A",
          "net_worth_ratio": "0.01%",
          "number_shares": "3,100",
          "position_value": "56,234",
          "ranking": "18",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "162010",
          "link": "购买",
          "name_fund": "长城久兆中小板300指数分级",
          "net_worth_ratio": "0.14%",
          "number_shares": "2,528",
          "position_value": "45,857",
          "ranking": "19",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "162510",
          "link": "购买",
          "name_fund": "国联安双力中小板综指(LOF)",
          "net_worth_ratio": "0.00%",
          "number_shares": "2,191",
          "position_value": "39,744",
          "ranking": "20",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "070030",
          "link": "购买",
          "name_fund": "嘉实中创400ETF联接",
          "net_worth_ratio": "0.01%",
          "number_shares": "500",
          "position_value": "9,070",
          "ranking": "21",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "202017",
          "link": "购买",
          "name_fund": "南方深证成份ETF联接A",
          "net_worth_ratio": "0.00%",
          "number_shares": "200",
          "position_value": "3,628",
          "ranking": "22",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "000826",
          "link": "购买",
          "name_fund": "广发百发100指数A",
          "net_worth_ratio": "0.00%",
          "number_shares": "76",
          "position_value": "1,378",
          "ranking": "23",
          "total_equity_ratio": "0.00%"
        },
        {
          "circulation_ratio": "0.00%",
          "fund_code": "001734",
          "link": "购买",
          "name_fund": "广发百发大数据成长混合A",
          "net_worth_ratio": "0.00%",
          "number_shares": "75",
          "position_value": "1,360",
          "ranking": "24",
          "total_equity_ratio": "0.00%"
        }
      ]
    },
    {
      "fund_info": [
        {
          "circulation_ratio": "0.48%",
          "fund_code": "160219",
          "link": "购买",
          "name_fund": "国泰国证医药卫生行业指数分级",
          "net_worth_ratio": "0.49%",
          "number_shares": "1,773,366",
          "position_value": "32,966,873",
          "ranking": "1",
          "total_equity_ratio": "0.48%"
        },
        {
          "circulation_ratio": "0.17%",
          "fund_code": "162412",
          "link": "购买",
          "name_fund": "华宝兴业中证医疗指数分级",
          "net_worth_ratio": "1.56%",
          "number_shares": "642,488",
          "position_value": "11,943,851",
          "ranking": "2",
          "total_equity_ratio": "0.17%"
        },
        {
          "circulation_ratio": "0.16%",
          "fund_code": "000826",
          "link": "购买",
          "name_fund": "广发百发100指数A",
          "net_worth_ratio": "0.95%",
          "number_shares": "609,176",
          "position_value": "11,324,581",
          "ranking": "3",
          "total_equity_ratio": "0.16%"
        }
      ]
    }
  ],
  "fund_table_year": [
    {
      "year_quarter": "2017-03-31"
    },
    {
      "year_quarter": "2016-12-31"
    },
    {
      "year_quarter": "2016-09-30"
    },
    {
      "year_quarter": "2016-06-30"
    },
    {
      "year_quarter": "2016-03-31"
    }
  ],
  "restricted_ban": [
    {
      "ban_number": "",
      "equity_ratio_shares_circulation": "",
      "lifted_shares_total_ratio": "",
      "release_time": "",
      "stock_type": ""
    },
    {
      "ban_number": "6081万",
      "equity_ratio_shares_circulation": "16.35%",
      "lifted_shares_total_ratio": "14.05%",
      "release_time": "2017-06-27",
      "stock_type": "定向增发机构配售股份"
    }
  ],
  "shareholder_number_table": "<table id=\"Table0\">\n<tr>\n<th class=\"tips-colnameL\"></th>\n<th class=\"tips-dataL\">17-03-31</th>\n<th class=\"tips-dataL\">16-12-31</th>\n<th class=\"tips-dataL\">16-09-30</th>\n<th class=\"tips-dataL\">16-06-30</th>\n<th class=\"tips-dataL\">16-03-31</th>\n<th class=\"tips-dataL\">15-12-31</th>\n<th class=\"tips-dataL\">15-11-30</th>\n<th class=\"tips-dataL\">15-10-31</th>\n<th class=\"tips-dataL\">15-09-30</th>\n<th class=\"tips-dataL\">15-08-31</th>\n</tr>\n<tr>\n<th class=\"tips-fieldnameL\">股东人数(户)</th>\n<td class=\"tips-dataL\">6.44万</td>\n<td class=\"tips-dataL\">6.57万</td>\n<td class=\"tips-dataL\">6.73万</td>\n<td class=\"tips-dataL\">7.12万</td>\n<td class=\"tips-dataL\">7.16万</td>\n<td class=\"tips-dataL\">7.79万</td>\n<td class=\"tips-dataL\">8.20万</td>\n<td class=\"tips-dataL\">7.50万</td>\n<td class=\"tips-dataL\">5.18万</td>\n<td class=\"tips-dataL\">5.10万</td>\n</tr>\n<tr>\n<th class=\"tips-fieldnameL\">较上期变化(%)</th>\n<td class=\"tips-dataL\">-2.02</td>\n<td class=\"tips-dataL\">-2.38</td>\n<td class=\"tips-dataL\">-5.47</td>\n<td class=\"tips-dataL\">-0.49</td>\n<td class=\"tips-dataL\">-8.05</td>\n<td class=\"tips-dataL\">-5.04</td>\n<td class=\"tips-dataL\">9.31</td>\n<td class=\"tips-dataL\">44.87</td>\n<td class=\"tips-dataL\">1.51</td>\n<td class=\"tips-dataL\">7.14</td>\n</tr>\n<tr>\n<th class=\"tips-fieldnameL\">人均流通股(股)</th>\n<td class=\"tips-dataL\">5775</td>\n<td class=\"tips-dataL\">5658</td>\n<td class=\"tips-dataL\">5524</td>\n<td class=\"tips-dataL\">5222</td>\n<td class=\"tips-dataL\">5186</td>\n<td class=\"tips-dataL\">4769</td>\n<td class=\"tips-dataL\">4538</td>\n<td class=\"tips-dataL\">4960</td>\n<td class=\"tips-dataL\">7186</td>\n<td class=\"tips-dataL\">7294</td>\n</tr>\n<tr>\n<th class=\"tips-fieldnameL\">较上期变化(%)</th>\n<td class=\"tips-dataL\">2.06</td>\n<td class=\"tips-dataL\">2.44</td>\n<td class=\"tips-dataL\">5.79</td>\n<td class=\"tips-dataL\">0.69</td>\n<td class=\"tips-dataL\">8.75</td>\n<td class=\"tips-dataL\">5.09</td>\n<td class=\"tips-dataL\">-8.52</td>\n<td class=\"tips-dataL\">-30.97</td>\n<td class=\"tips-dataL\">-1.49</td>\n<td class=\"tips-dataL\">-6.67</td>\n</tr>\n<tr>\n<th class=\"tips-fieldnameL\">筹码集中度</th>\n<td class=\"tips-dataL\">非常集中</td>\n<td class=\"tips-dataL\">非常集中</td>\n<td class=\"tips-dataL\">非常集中</td>\n<td class=\"tips-dataL\">非常集中</td>\n<td class=\"tips-dataL\">非常集中</td>\n<td class=\"tips-dataL\">较集中</td>\n<td class=\"tips-dataL\">较集中</td>\n<td class=\"tips-dataL\">较集中</td>\n<td class=\"tips-dataL\">非常集中</td>\n<td class=\"tips-dataL\">非常集中</td>\n</tr>\n<tr>\n<th class=\"tips-fieldnameL\">股价(元)</th>\n<td class=\"tips-dataL\">14.10</td>\n<td class=\"tips-dataL\">15.53</td>\n<td class=\"tips-dataL\">17.18</td>\n<td class=\"tips-dataL\">18.14</td>\n<td class=\"tips-dataL\">18.59</td>\n<td class=\"tips-dataL\">24.70</td>\n<td class=\"tips-dataL\">29.05</td>\n<td class=\"tips-dataL\">29.01</td>\n<td class=\"tips-dataL\">13.17</td>\n<td class=\"tips-dataL\">15.40</td>\n</tr>\n<tr>\n<th class=\"tips-fieldnameL\">人均持股金额(元)</th>\n<td class=\"tips-dataL\">8.14万</td>\n<td class=\"tips-dataL\">8.79万</td>\n<td class=\"tips-dataL\">9.49万</td>\n<td class=\"tips-dataL\">9.47万</td>\n<td class=\"tips-dataL\">9.64万</td>\n<td class=\"tips-dataL\">11.8万</td>\n<td class=\"tips-dataL\">13.2万</td>\n<td class=\"tips-dataL\">14.4万</td>\n<td class=\"tips-dataL\">9.46万</td>\n<td class=\"tips-dataL\">11.2万</td>\n</tr>\n<tr>\n<th class=\"tips-fieldnameL\">前十大股东持股合计(%)</th>\n<td class=\"tips-dataL\">49.24</td>\n<td class=\"tips-dataL\">49.85</td>\n<td class=\"tips-dataL\">49.78</td>\n<td class=\"tips-dataL\">50.96</td>\n<td class=\"tips-dataL\">50.20</td>\n<td class=\"tips-dataL\">50.92</td>\n<td class=\"tips-dataL\">--</td>\n<td class=\"tips-dataL\">--</td>\n<td class=\"tips-dataL\">52.54</td>\n<td class=\"tips-dataL\">--</td>\n</tr>\n<tr>\n<th class=\"tips-fieldnameL\">前十大流通股东持股合计(%)</th>\n<td class=\"tips-dataL\">41.87</td>\n<td class=\"tips-dataL\">42.37</td>\n<td class=\"tips-dataL\">42.73</td>\n<td class=\"tips-dataL\">43.19</td>\n<td class=\"tips-dataL\">50.20</td>\n<td class=\"tips-dataL\">50.72</td>\n<td class=\"tips-dataL\">--</td>\n<td class=\"tips-dataL\">--</td>\n<td class=\"tips-dataL\">52.54</td>\n<td class=\"tips-dataL\">--</td>\n</tr>\n</table>\r\n            \n",
  "ten_outstanding_shares_info": [
    {
      "shareholders": [
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "nature_shareholders": "投资公司",
          "number_shares": "161,338,702",
          "ranking": "1",
          "shareholder_name": "石河子三和股权投资合伙企业(有限合伙)",
          "shares_type": "A股",
          "total_stake_distribution": "43.37%"
        },
        {
          "changes_proportion": "-23.10%",
          "increase_decrease_shares": "-3,064,434",
          "nature_shareholders": "其它",
          "number_shares": "10,200,000",
          "ranking": "2",
          "shareholder_name": "HEDDINGTON LTD.",
          "shares_type": "A股",
          "total_stake_distribution": "2.74%"
        },
        {
          "changes_proportion": "25.68%",
          "increase_decrease_shares": "439,800",
          "nature_shareholders": "个人",
          "number_shares": "2,152,200",
          "ranking": "3",
          "shareholder_name": "陈荷跃",
          "shares_type": "A股",
          "total_stake_distribution": "0.58%"
        },
        {
          "changes_proportion": "-1.06%",
          "increase_decrease_shares": "-19,300",
          "nature_shareholders": "个人",
          "number_shares": "1,806,000",
          "ranking": "4",
          "shareholder_name": "叶新林",
          "shares_type": "A股",
          "total_stake_distribution": "0.49%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "nature_shareholders": "个人",
          "number_shares": "1,075,000",
          "ranking": "5",
          "shareholder_name": "徐留胜",
          "shares_type": "A股",
          "total_stake_distribution": "0.29%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "nature_shareholders": "个人",
          "number_shares": "1,012,545",
          "ranking": "6",
          "shareholder_name": "蔡少芸",
          "shares_type": "A股",
          "total_stake_distribution": "0.27%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "nature_shareholders": "个人",
          "number_shares": "1,000,000",
          "ranking": "7",
          "shareholder_name": "蔡少红",
          "shares_type": "A股",
          "total_stake_distribution": "0.27%"
        },
        {
          "changes_proportion": "51.13%",
          "increase_decrease_shares": "327,900",
          "nature_shareholders": "个人",
          "number_shares": "969,163",
          "ranking": "8",
          "shareholder_name": "王雄",
          "shares_type": "A股",
          "total_stake_distribution": "0.26%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "nature_shareholders": "其它",
          "number_shares": "842,800",
          "ranking": "9",
          "shareholder_name": "中央汇金资产管理有限责任公司",
          "shares_type": "A股",
          "total_stake_distribution": "0.23%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "证券投资基金",
          "number_shares": "829,348",
          "ranking": "10",
          "shareholder_name": "中国农业银行股份有限公司-国泰国证医药卫生行业指数分级证券投资基金",
          "shares_type": "A股",
          "total_stake_distribution": "0.22%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "--",
          "nature_shareholders": "--",
          "number_shares": "181,225,758",
          "ranking": "",
          "shareholder_name": "合计",
          "shares_type": "--",
          "total_stake_distribution": "48.72%"
        }
      ]
    },
    {
      "shareholders": [
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "nature_shareholders": "投资公司",
          "number_shares": "161,338,702",
          "ranking": "1",
          "shareholder_name": "石河子三和股权投资合伙企业(有限合伙)",
          "shares_type": "A股",
          "total_stake_distribution": "43.37%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "nature_shareholders": "其它",
          "number_shares": "13,264,434",
          "ranking": "2",
          "shareholder_name": "HEDDINGTON LTD.",
          "shares_type": "A股",
          "total_stake_distribution": "3.57%"
        },
        {
          "changes_proportion": "10.74%",
          "increase_decrease_shares": "177,000",
          "nature_shareholders": "个人",
          "number_shares": "1,825,300",
          "ranking": "3",
          "shareholder_name": "叶新林",
          "shares_type": "A股",
          "total_stake_distribution": "0.49%"
        },
        {
          "changes_proportion": "58.00%",
          "increase_decrease_shares": "628,600",
          "nature_shareholders": "个人",
          "number_shares": "1,712,400",
          "ranking": "4",
          "shareholder_name": "陈荷跃",
          "shares_type": "A股",
          "total_stake_distribution": "0.46%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "个人",
          "number_shares": "1,075,000",
          "ranking": "5",
          "shareholder_name": "徐留胜",
          "shares_type": "A股",
          "total_stake_distribution": "0.29%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "个人",
          "number_shares": "1,012,545",
          "ranking": "6",
          "shareholder_name": "蔡少芸",
          "shares_type": "A股",
          "total_stake_distribution": "0.27%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "个人",
          "number_shares": "1,000,000",
          "ranking": "7",
          "shareholder_name": "蔡少红",
          "shares_type": "A股",
          "total_stake_distribution": "0.27%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "nature_shareholders": "其它",
          "number_shares": "842,800",
          "ranking": "8",
          "shareholder_name": "中央汇金资产管理有限责任公司",
          "shares_type": "A股",
          "total_stake_distribution": "0.23%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "个人",
          "number_shares": "663,600",
          "ranking": "9",
          "shareholder_name": "王君福",
          "shares_type": "A股",
          "total_stake_distribution": "0.18%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "个人",
          "number_shares": "641,263",
          "ranking": "10",
          "shareholder_name": "王雄",
          "shares_type": "A股",
          "total_stake_distribution": "0.17%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "--",
          "nature_shareholders": "--",
          "number_shares": "183,376,044",
          "ranking": "",
          "shareholder_name": "合计",
          "shares_type": "--",
          "total_stake_distribution": "49.29%"
        }
      ]
    },
    {
      "shareholders": [
        {
          "changes_proportion": "-3.00%",
          "increase_decrease_shares": "-4,994,600",
          "nature_shareholders": "投资公司",
          "number_shares": "161,338,702",
          "ranking": "1",
          "shareholder_name": "石河子三和股权投资合伙企业(有限合伙)",
          "shares_type": "A股",
          "total_stake_distribution": "43.37%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "nature_shareholders": "其它",
          "number_shares": "13,264,434",
          "ranking": "2",
          "shareholder_name": "HEDDINGTON LTD.",
          "shares_type": "A股",
          "total_stake_distribution": "3.57%"
        },
        {
          "changes_proportion": "6.48%",
          "increase_decrease_shares": "100,350",
          "nature_shareholders": "个人",
          "number_shares": "1,648,300",
          "ranking": "3",
          "shareholder_name": "叶新林",
          "shares_type": "A股",
          "total_stake_distribution": "0.44%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "个人",
          "number_shares": "1,538,900",
          "ranking": "4",
          "shareholder_name": "申小玲",
          "shares_type": "A股",
          "total_stake_distribution": "0.41%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "基金资产管理计划",
          "number_shares": "1,533,601",
          "ranking": "5",
          "shareholder_name": "中铁宝盈资产-广发银行-中铁宝盈-夺宝1号资产管理计划",
          "shares_type": "A股",
          "total_stake_distribution": "0.41%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "证券投资基金",
          "number_shares": "1,514,838",
          "ranking": "6",
          "shareholder_name": "中国农业银行股份有限公司-华安智能装备主题股票型证券投资基金",
          "shares_type": "A股",
          "total_stake_distribution": "0.41%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "信托计划",
          "number_shares": "1,206,530",
          "ranking": "7",
          "shareholder_name": "平安信托有限责任公司-平安财富*聚金8号证券投资集合资金信托计划",
          "shares_type": "A股",
          "total_stake_distribution": "0.32%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "个人",
          "number_shares": "1,083,800",
          "ranking": "8",
          "shareholder_name": "陈荷跃",
          "shares_type": "A股",
          "total_stake_distribution": "0.29%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "信托计划",
          "number_shares": "971,500",
          "ranking": "9",
          "shareholder_name": "云南国际信托有限公司-源盛恒瑞15号集合资金信托计划",
          "shares_type": "A股",
          "total_stake_distribution": "0.26%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "nature_shareholders": "其它",
          "number_shares": "842,800",
          "ranking": "10",
          "shareholder_name": "中央汇金资产管理有限责任公司",
          "shares_type": "A股",
          "total_stake_distribution": "0.23%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "--",
          "nature_shareholders": "--",
          "number_shares": "184,943,405",
          "ranking": "",
          "shareholder_name": "合计",
          "shares_type": "--",
          "total_stake_distribution": "49.72%"
        }
      ]
    },
    {
      "shareholders": [
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "nature_shareholders": "投资公司",
          "number_shares": "166,333,302",
          "ranking": "1",
          "shareholder_name": "石河子三和股权投资合伙企业(有限合伙)",
          "shares_type": "A股",
          "total_stake_distribution": "44.71%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "nature_shareholders": "其它",
          "number_shares": "13,264,434",
          "ranking": "2",
          "shareholder_name": "HEDDINGTON LTD.",
          "shares_type": "A股",
          "total_stake_distribution": "3.57%"
        },
        {
          "changes_proportion": "-1.09%",
          "increase_decrease_shares": "-19,400",
          "nature_shareholders": "证券投资基金",
          "number_shares": "1,753,966",
          "ranking": "3",
          "shareholder_name": "中国农业银行股份有限公司-国泰国证医药卫生行业指数分级证券投资基金",
          "shares_type": "A股",
          "total_stake_distribution": "0.47%"
        },
        {
          "changes_proportion": "30.24%",
          "increase_decrease_shares": "359,421",
          "nature_shareholders": "个人",
          "number_shares": "1,547,950",
          "ranking": "4",
          "shareholder_name": "叶新林",
          "shares_type": "A股",
          "total_stake_distribution": "0.42%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "其它",
          "number_shares": "950,000",
          "ranking": "5",
          "shareholder_name": "太原狮头集团有限公司",
          "shares_type": "A股",
          "total_stake_distribution": "0.26%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "nature_shareholders": "其它",
          "number_shares": "842,800",
          "ranking": "6",
          "shareholder_name": "中央汇金资产管理有限责任公司",
          "shares_type": "A股",
          "total_stake_distribution": "0.23%"
        },
        {
          "changes_proportion": "-4.00%",
          "increase_decrease_shares": "-25,700",
          "nature_shareholders": "证券投资基金",
          "number_shares": "616,788",
          "ranking": "7",
          "shareholder_name": "中国银行股份有限公司-华宝兴业中证医疗指数分级证券投资基金",
          "shares_type": "A股",
          "total_stake_distribution": "0.17%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "个人",
          "number_shares": "571,073",
          "ranking": "8",
          "shareholder_name": "周勇明",
          "shares_type": "A股",
          "total_stake_distribution": "0.15%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "QFII",
          "number_shares": "527,100",
          "ranking": "9",
          "shareholder_name": "新韩法国巴黎资产运用株式会社-新韩法巴中国内地中小市值RQFII股票集成投资信托基金(股票)(交易所)",
          "shares_type": "A股",
          "total_stake_distribution": "0.14%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "个人",
          "number_shares": "520,600",
          "ranking": "10",
          "shareholder_name": "丁前进",
          "shares_type": "A股",
          "total_stake_distribution": "0.14%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "--",
          "nature_shareholders": "--",
          "number_shares": "186,928,013",
          "ranking": "",
          "shareholder_name": "合计",
          "shares_type": "--",
          "total_stake_distribution": "50.25%"
        }
      ]
    },
    {
      "shareholders": [
        {
          "changes_proportion": "0.45%",
          "increase_decrease_shares": "744,600",
          "nature_shareholders": "投资公司",
          "number_shares": "166,333,302",
          "ranking": "1",
          "shareholder_name": "石河子三和股权投资合伙企业(有限合伙)",
          "shares_type": "A股",
          "total_stake_distribution": "44.80%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "nature_shareholders": "其它",
          "number_shares": "13,264,434",
          "ranking": "2",
          "shareholder_name": "HEDDINGTON LTD.",
          "shares_type": "A股",
          "total_stake_distribution": "3.57%"
        },
        {
          "changes_proportion": "3.63%",
          "increase_decrease_shares": "62,081",
          "nature_shareholders": "证券投资基金",
          "number_shares": "1,773,366",
          "ranking": "3",
          "shareholder_name": "中国农业银行股份有限公司-国泰国证医药卫生行业指数分级证券投资基金",
          "shares_type": "A股",
          "total_stake_distribution": "0.48%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "个人",
          "number_shares": "1,188,529",
          "ranking": "4",
          "shareholder_name": "叶新林",
          "shares_type": "A股",
          "total_stake_distribution": "0.32%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "信托计划",
          "number_shares": "850,000",
          "ranking": "5",
          "shareholder_name": "陕西省国际信托股份有限公司-陕国投·海中湾(齐鲁)3号证券投资集合资金信托计划",
          "shares_type": "A股",
          "total_stake_distribution": "0.23%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "nature_shareholders": "其它",
          "number_shares": "842,800",
          "ranking": "6",
          "shareholder_name": "中央汇金资产管理有限责任公司",
          "shares_type": "A股",
          "total_stake_distribution": "0.23%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "个人",
          "number_shares": "729,600",
          "ranking": "7",
          "shareholder_name": "邵志坚",
          "shares_type": "A股",
          "total_stake_distribution": "0.20%"
        },
        {
          "changes_proportion": "-38.55%",
          "increase_decrease_shares": "-403,000",
          "nature_shareholders": "证券投资基金",
          "number_shares": "642,488",
          "ranking": "8",
          "shareholder_name": "中国银行股份有限公司-华宝兴业中证医疗指数分级证券投资基金",
          "shares_type": "A股",
          "total_stake_distribution": "0.17%"
        },
        {
          "changes_proportion": "4.33%",
          "increase_decrease_shares": "25,300",
          "nature_shareholders": "证券投资基金",
          "number_shares": "609,176",
          "ranking": "9",
          "shareholder_name": "兴业银行股份有限公司-广发中证百度百发策略100指数型证券投资基金",
          "shares_type": "A股",
          "total_stake_distribution": "0.16%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "nature_shareholders": "个人",
          "number_shares": "500,059",
          "ranking": "10",
          "shareholder_name": "车卫真",
          "shares_type": "A股",
          "total_stake_distribution": "0.13%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "--",
          "nature_shareholders": "--",
          "number_shares": "186,733,754",
          "ranking": "",
          "shareholder_name": "合计",
          "shares_type": "--",
          "total_stake_distribution": "50.30%"
        }
      ]
    }
  ],
  "ten_outstanding_shares_year": [
    {
      "year_quarter": "2017-03-31"
    },
    {
      "year_quarter": "2016-12-31"
    },
    {
      "year_quarter": "2016-09-30"
    },
    {
      "year_quarter": "2016-06-30"
    },
    {
      "year_quarter": "2016-03-31"
    }
  ],
  "top10_shareholders_change": [],
  "top10_shareholders_info": [
    {
      "shareholders": [
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "161,338,702",
          "ranking": "1",
          "shareholder_name": "石河子三和股权投资合伙企业(有限合伙)",
          "shares_type": "流通A股",
          "total_equity_stake": "37.28%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "18,256,578",
          "ranking": "2",
          "shareholder_name": "招商财富-招商银行-晟融1号专项资产管理计划",
          "shares_type": "限售流通A股",
          "total_equity_stake": "4.22%"
        },
        {
          "changes_proportion": "-23.10%",
          "increase_decrease_shares": "-3,064,434",
          "number_shares": "10,200,000",
          "ranking": "3",
          "shareholder_name": "HEDDINGTON LTD.",
          "shares_type": "流通A股",
          "total_equity_stake": "2.36%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "6,578,950",
          "ranking": "4",
          "shareholder_name": "中国工商银行股份有限公司-财通多策略升级混合型证券投资基金",
          "shares_type": "限售流通A股",
          "total_equity_stake": "1.52%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "6,578,947",
          "ranking": "5",
          "shareholder_name": "第一创业证券-国信证券-共盈大岩量化定增集合资产管理计划",
          "shares_type": "限售流通A股",
          "total_equity_stake": "1.52%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "2,631,579",
          "ranking": "6",
          "shareholder_name": "财通基金-工商银行-国海证券股份有限公司",
          "shares_type": "限售流通A股",
          "total_equity_stake": "0.61%"
        },
        {
          "changes_proportion": "25.68%",
          "increase_decrease_shares": "439,800",
          "number_shares": "2,152,200",
          "ranking": "7",
          "shareholder_name": "陈荷跃",
          "shares_type": "流通A股",
          "total_equity_stake": "0.50%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "1,973,684",
          "ranking": "8",
          "shareholder_name": "财通基金-平安银行-中信建投证券股份有限公司",
          "shares_type": "限售流通A股",
          "total_equity_stake": "0.46%"
        },
        {
          "changes_proportion": "-1.06%",
          "increase_decrease_shares": "-19,300",
          "number_shares": "1,806,000",
          "ranking": "9",
          "shareholder_name": "叶新林",
          "shares_type": "流通A股",
          "total_equity_stake": "0.42%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "1,587,882",
          "ranking": "10",
          "shareholder_name": "诺安基金-工商银行-诺安定增稳利6号资产管理计划",
          "shares_type": "限售流通A股",
          "total_equity_stake": "0.37%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "--",
          "number_shares": "213,104,522",
          "ranking": "",
          "shareholder_name": "合计",
          "shares_type": "--",
          "total_equity_stake": "49.26%"
        }
      ]
    },
    {
      "shareholders": [
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "161,338,702",
          "ranking": "1",
          "shareholder_name": "石河子三和股权投资合伙企业(有限合伙)",
          "shares_type": "流通A股",
          "total_equity_stake": "37.28%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "18,256,578",
          "ranking": "2",
          "shareholder_name": "招商财富-招商银行-晟融1号专项资产管理计划",
          "shares_type": "限售流通A股",
          "total_equity_stake": "4.22%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "13,264,434",
          "ranking": "3",
          "shareholder_name": "HEDDINGTON LTD.",
          "shares_type": "流通A股",
          "total_equity_stake": "3.06%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "6,578,950",
          "ranking": "4",
          "shareholder_name": "中国工商银行股份有限公司-财通多策略升级混合型证券投资基金",
          "shares_type": "限售流通A股",
          "total_equity_stake": "1.52%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "6,578,947",
          "ranking": "5",
          "shareholder_name": "第一创业证券-国信证券-共盈大岩量化定增集合资产管理计划",
          "shares_type": "限售流通A股",
          "total_equity_stake": "1.52%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "2,631,579",
          "ranking": "6",
          "shareholder_name": "财通基金-工商银行-国海证券股份有限公司",
          "shares_type": "限售流通A股",
          "total_equity_stake": "0.61%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "1,973,684",
          "ranking": "7",
          "shareholder_name": "财通基金-平安银行-中信建投证券股份有限公司",
          "shares_type": "限售流通A股",
          "total_equity_stake": "0.46%"
        },
        {
          "changes_proportion": "10.74%",
          "increase_decrease_shares": "177,000",
          "number_shares": "1,825,300",
          "ranking": "8",
          "shareholder_name": "叶新林",
          "shares_type": "流通A股",
          "total_equity_stake": "0.42%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "number_shares": "1,712,400",
          "ranking": "9",
          "shareholder_name": "陈荷跃",
          "shares_type": "流通A股",
          "total_equity_stake": "0.40%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "1,587,882",
          "ranking": "10",
          "shareholder_name": "诺安基金-工商银行-诺安定增稳利6号资产管理计划",
          "shares_type": "限售流通A股",
          "total_equity_stake": "0.37%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "--",
          "number_shares": "215,748,456",
          "ranking": "",
          "shareholder_name": "合计",
          "shares_type": "--",
          "total_equity_stake": "49.86%"
        }
      ]
    },
    {
      "shareholders": [
        {
          "changes_proportion": "-3.00%",
          "increase_decrease_shares": "-4,994,600",
          "number_shares": "161,338,702",
          "ranking": "1",
          "shareholder_name": "石河子三和股权投资合伙企业(有限合伙)",
          "shares_type": "流通A股",
          "total_equity_stake": "37.28%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "18,256,578",
          "ranking": "2",
          "shareholder_name": "招商财富-招商银行-晟融1号专项资产管理计划",
          "shares_type": "限售流通A股",
          "total_equity_stake": "4.22%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "13,264,434",
          "ranking": "3",
          "shareholder_name": "HEDDINGTON LTD.",
          "shares_type": "流通A股",
          "total_equity_stake": "3.06%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "6,578,950",
          "ranking": "4",
          "shareholder_name": "中国工商银行股份有限公司-财通多策略升级混合型证券投资基金",
          "shares_type": "限售流通A股",
          "total_equity_stake": "1.52%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "6,578,947",
          "ranking": "5",
          "shareholder_name": "第一创业证券-国信证券-共盈大岩量化定增集合资产管理计划",
          "shares_type": "限售流通A股",
          "total_equity_stake": "1.52%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "2,631,579",
          "ranking": "6",
          "shareholder_name": "财通基金-工商银行-国海证券股份有限公司",
          "shares_type": "限售流通A股",
          "total_equity_stake": "0.61%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "1,973,684",
          "ranking": "7",
          "shareholder_name": "财通基金-平安银行-中信建投证券股份有限公司",
          "shares_type": "限售流通A股",
          "total_equity_stake": "0.46%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "number_shares": "1,648,300",
          "ranking": "8",
          "shareholder_name": "叶新林",
          "shares_type": "流通A股",
          "total_equity_stake": "0.38%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "1,587,882",
          "ranking": "9",
          "shareholder_name": "诺安基金-工商银行-诺安定增稳利6号资产管理计划",
          "shares_type": "限售流通A股",
          "total_equity_stake": "0.37%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "1,587,882",
          "ranking": "9",
          "shareholder_name": "诺安基金-建设银行-利安人寿-诺安定享7号资产管理计划",
          "shares_type": "限售流通A股",
          "total_equity_stake": "0.37%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "--",
          "number_shares": "215,446,938",
          "ranking": "",
          "shareholder_name": "合计",
          "shares_type": "--",
          "total_equity_stake": "49.79%"
        }
      ]
    },
    {
      "shareholders": [
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "166,333,302",
          "ranking": "1",
          "shareholder_name": "石河子三和股权投资合伙企业(有限合伙)",
          "shares_type": "流通A股",
          "total_equity_stake": "38.43%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "number_shares": "18,256,578",
          "ranking": "2",
          "shareholder_name": "招商财富-招商银行-晟融1号专项资产管理计划",
          "shares_type": "限售流通A股",
          "total_equity_stake": "4.22%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "13,264,434",
          "ranking": "3",
          "shareholder_name": "HEDDINGTON LTD.",
          "shares_type": "流通A股",
          "total_equity_stake": "3.06%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "number_shares": "6,578,950",
          "ranking": "4",
          "shareholder_name": "中国工商银行股份有限公司-财通多策略升级混合型证券投资基金",
          "shares_type": "限售流通A股",
          "total_equity_stake": "1.52%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "number_shares": "6,578,947",
          "ranking": "5",
          "shareholder_name": "第一创业证券-国信证券-共盈大岩量化定增集合资产管理计划",
          "shares_type": "限售流通A股",
          "total_equity_stake": "1.52%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "number_shares": "2,631,579",
          "ranking": "6",
          "shareholder_name": "财通基金-工商银行-国海证券股份有限公司",
          "shares_type": "限售流通A股",
          "total_equity_stake": "0.61%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "number_shares": "1,973,684",
          "ranking": "7",
          "shareholder_name": "财通基金-平安银行-中信建投证券股份有限公司",
          "shares_type": "限售流通A股",
          "total_equity_stake": "0.46%"
        },
        {
          "changes_proportion": "-1.09%",
          "increase_decrease_shares": "-19,400",
          "number_shares": "1,753,966",
          "ranking": "8",
          "shareholder_name": "中国农业银行股份有限公司-国泰国证医药卫生行业指数分级证券投资基金",
          "shares_type": "流通A股",
          "total_equity_stake": "0.41%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "number_shares": "1,587,882",
          "ranking": "9",
          "shareholder_name": "诺安基金-建设银行-利安人寿-诺安定享7号资产管理计划",
          "shares_type": "限售流通A股",
          "total_equity_stake": "0.37%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "number_shares": "1,587,882",
          "ranking": "9",
          "shareholder_name": "诺安基金-工商银行-诺安定增稳利6号资产管理计划",
          "shares_type": "限售流通A股",
          "total_equity_stake": "0.37%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "--",
          "number_shares": "220,547,204",
          "ranking": "",
          "shareholder_name": "合计",
          "shares_type": "--",
          "total_equity_stake": "50.97%"
        }
      ]
    },
    {
      "shareholders": [
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "166,333,302",
          "ranking": "1",
          "shareholder_name": "石河子三和股权投资合伙企业(有限合伙)",
          "shares_type": "流通A股",
          "total_equity_stake": "44.71%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "13,264,434",
          "ranking": "2",
          "shareholder_name": "HEDDINGTON LTD.",
          "shares_type": "流通A股",
          "total_equity_stake": "3.57%"
        },
        {
          "changes_proportion": "3.63%",
          "increase_decrease_shares": "62,081",
          "number_shares": "1,773,366",
          "ranking": "3",
          "shareholder_name": "中国农业银行股份有限公司-国泰国证医药卫生行业指数分级证券投资基金",
          "shares_type": "流通A股",
          "total_equity_stake": "0.48%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "number_shares": "1,188,529",
          "ranking": "4",
          "shareholder_name": "叶新林",
          "shares_type": "流通A股",
          "total_equity_stake": "0.32%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "number_shares": "850,000",
          "ranking": "5",
          "shareholder_name": "陕西省国际信托股份有限公司-陕国投·海中湾(齐鲁)3号证券投资集合资金信托计划",
          "shares_type": "流通A股",
          "total_equity_stake": "0.23%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "不变",
          "number_shares": "842,800",
          "ranking": "6",
          "shareholder_name": "中央汇金资产管理有限责任公司",
          "shares_type": "流通A股",
          "total_equity_stake": "0.23%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "number_shares": "729,600",
          "ranking": "7",
          "shareholder_name": "邵志坚",
          "shares_type": "流通A股",
          "total_equity_stake": "0.20%"
        },
        {
          "changes_proportion": "-38.55%",
          "increase_decrease_shares": "-403,000",
          "number_shares": "642,488",
          "ranking": "8",
          "shareholder_name": "中国银行股份有限公司-华宝兴业中证医疗指数分级证券投资基金",
          "shares_type": "流通A股",
          "total_equity_stake": "0.17%"
        },
        {
          "changes_proportion": "4.33%",
          "increase_decrease_shares": "25,300",
          "number_shares": "609,176",
          "ranking": "9",
          "shareholder_name": "兴业银行股份有限公司-广发中证百度百发策略100指数型证券投资基金",
          "shares_type": "流通A股",
          "total_equity_stake": "0.16%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "新进",
          "number_shares": "500,059",
          "ranking": "10",
          "shareholder_name": "车卫真",
          "shares_type": "流通A股",
          "total_equity_stake": "0.13%"
        },
        {
          "changes_proportion": "--",
          "increase_decrease_shares": "--",
          "number_shares": "186,733,754",
          "ranking": "",
          "shareholder_name": "合计",
          "shares_type": "--",
          "total_equity_stake": "50.20%"
        }
      ]
    }
  ],
  "top10_shareholders_year": [
    {
      "year_quarter": "2017-03-31"
    },
    {
      "year_quarter": "2016-12-31"
    },
    {
      "year_quarter": "2016-09-30"
    },
    {
      "year_quarter": "2016-06-30"
    },
    {
      "year_quarter": "2016-03-31"
    }
  ]
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
