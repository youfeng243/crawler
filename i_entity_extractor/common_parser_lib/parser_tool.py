# coding=utf8
import sys
import os

sys.path.append('../')
from province_parser import ProvinceParser
from casecause_parser import CaseCauseParser
from caseid_parser import CaseIdParser
from date_parser import DateParser
from excel_parser import ExcelParser
from money_parser import MoneyParser
from litigant_util import LitigantUtil
#from industry_parser import IndustryParser
from industry_parser_old import IndustryParser
from court_parser import CourtParser
from court_place_parser import CourtPlaceParser

from i_entity_extractor.extractors.judge_wenshu.wenshu_parser import Wenshu_parser
from i_entity_extractor.extractors.bid.bid_money_parser import Bid_money_parser
from i_entity_extractor.extractors.bid.bid_company_parser import Bid_company_parser
from i_entity_extractor.extractors.bid.bid_region_parser import BidRegionParser
from i_entity_extractor.extractors.bid.bid_close_time_parser import BidCloseTimeParser
from company_parser import CompanyParser
from i_entity_extractor.extractors.fygg.fygg_parser import Fygg_parser


class ParserTool:
    def __init__(self):

        current_path = os.getcwd()
        basic_path = current_path[:current_path.rfind('i_entity_extractor')]

        self.casecause_conf_path   = basic_path + 'i_entity_extractor/dict/casecause.conf'
        self.company_pre_path      = basic_path + 'i_entity_extractor/dict/company_pre.conf'
        self.company_end_path      = basic_path + 'i_entity_extractor/dict/company_end.conf'
        self.province_conf_path    = basic_path + 'i_entity_extractor/dict/province_city.conf'
        self.phonenum_conf_path    = basic_path + 'i_entity_extractor/dict/phonenum_city.conf'
        self.region_conf_path      = basic_path + 'i_entity_extractor/dict/region_city.conf'
        self.litigant_conf_path    = basic_path + 'i_entity_extractor/dict/litigant.conf'
        self.plaintiff_conf_path   = basic_path + 'i_entity_extractor/dict/plaintiff.conf'
        self.defendant_conf_path   = basic_path + 'i_entity_extractor/dict/defendant_fygg.conf'
        self.city_conf_path        = basic_path + 'i_entity_extractor/dict/city.conf'
        self.court_conf_path       = basic_path + 'i_entity_extractor/dict/court.conf'
        self.court_place_conf_path = basic_path + 'i_entity_extractor/dict/court_place.conf'

        #计算企业行业配置文件
        # self.business_model_path   = basic_path + 'i_entity_extractor/dict/fasttext_industry_business.model.bin'
        # self.company_name_model_path = basic_path + 'i_entity_extractor/dict/fasttext_industry_company_name.model.bin'
        # self.industry_conf_path    = basic_path + 'i_entity_extractor/dict/industry.conf'
        # self.postfix_deterministic_path  = basic_path + 'i_entity_extractor/dict/postfix_deterministic.conf'
        # self.postfix_deterministic2_path  = basic_path + 'i_entity_extractor/dict/postfix_deterministic2.conf'
        # self.company_keyword_path = basic_path + 'i_entity_extractor/dict/company_keyword.conf'
        # self.company_name_postfix_path = basic_path + 'i_entity_extractor/dict/company_name_postfix.conf'
        self.industry_parser_model_path = basic_path + 'i_entity_extractor/dict/industry_predict.model.bin'

        self.parser_init()

    def parser_init(self):
        self.province_parser = ProvinceParser(self.province_conf_path, self.phonenum_conf_path, self.region_conf_path, self.city_conf_path)
        self.company_parser = CompanyParser(self.company_pre_path)
        self.case_cause_parser = CaseCauseParser(self.casecause_conf_path)
        self.date_parser  = DateParser()
        self.excel_parser = ExcelParser()
        self.money_parser = MoneyParser()
        self.caseid_parser = CaseIdParser()
        self.litigant_util_parser = LitigantUtil(self.litigant_conf_path)

        self.bid_money_parser      = Bid_money_parser()
        self.bid_company_parser    = Bid_company_parser(self.company_parser)
        self.bid_region_parser     = BidRegionParser(self.province_parser)
        self.bid_close_time_parser = BidCloseTimeParser(self.date_parser)

        self.fygg_parser   = Fygg_parser(self.plaintiff_conf_path,self.defendant_conf_path)
        self.wenshu_parser = Wenshu_parser()

        self.court_parser       = CourtParser(self.court_conf_path)
        self.court_place_parser = CourtPlaceParser(self.court_place_conf_path)

        self.industry_parser    = IndustryParser(self.industry_parser_model_path)


parser_tool = ParserTool()


if __name__ == "__main__":
    print parser_tool.industry_parser.predict("中国招商银行股份有限公司")
    print parser_tool.province_parser.get_province("江西招商银行股份有限公司", 0)

    print parser_tool.province_parser.get_region("江西泰和县招商银行股份有限公司", 0)