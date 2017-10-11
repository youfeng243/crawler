# coding=utf8

import sys
import jieba
from industry_fasttext import IndustyParserFast
from companyname_rules_classfier import CompanyNameIndustryParser
from business_rules_classfier import BusinessRuleIndustryParser

reload(sys)
sys.setdefaultencoding('utf8')

class IndustryParser:
    def __init__(self, company_model_path, business_model_path, industry_conf_path, postfix_deterministic_path, postfix_deterministic2_path,company_keyword_path,company_name_postfix_path):
        self.insdustry_model_parser   = IndustyParserFast(company_model_path,business_model_path)
        self.company_name_rule_parser = CompanyNameIndustryParser(industry_conf_path, postfix_deterministic_path, postfix_deterministic2_path)
        self.business_rule_parser     = BusinessRuleIndustryParser(industry_conf_path)
        self.company_postfix          = self.load(company_name_postfix_path)
        self.company_keyword          = self.load(company_keyword_path)


    def load(self,filename):
        data_dict = {}
        with open(filename) as f :
            for line in f :
                fields = line.strip().replace("\n",'').split()
                if len(fields) != 2 :
                    continue
                data_dict[fields[0].strip()] = fields[1].strip()
        return  data_dict

    def company_postfix_result(self, company_name ) :
        for postfix,industry in self.company_postfix.items() :
            if company_name.endswith(postfix) :
                return industry
        return  ''

    def company_keyword_result(self, company_name ) :
        for postfix, industry in self.company_postfix.items():
            if postfix in company_name :
                return  industry
        return ''

    def fast_text_result(self, company_info ):

        return  self.insdustry_model_parser.predict_name_and_business(company_info)

    def predict(self, company_name, business_scope) :
        try:

            result = self.company_postfix_result( company_name)
            if result != '' :
                return result

            result = self.fast_text_result( company_name + "\t" + business_scope)
            if result != '':
                return result

            result          = self.company_name_rule_parser.predict( company_name )
            result_1, score = self.insdustry_model_parser.name_predict( company_name )
            result_2        = self.business_rule_parser.bus_predict(company_name, business_scope)
            if result != '' and len(result_2) > 0 and  result in result_2 :
                return result
            elif result_1 != '' and len(result_2) > 0 and result_1 in result_2:
                return result_1
            elif result != '' :
                return result
            elif result_1 != '' and score >= 0.9 :
                return result_1

            result = self.company_keyword_result(company_name)
            result_3 = self.insdustry_model_parser.business_predict(business_scope)
            if result != '' and len(result_2) > 0 and result in result_2:
                return result
            elif result != '' and len(result_3) > 0 and result in result_3:
                return result
            elif result != '':
                return result
            if len(result_2) > 0 and result_2[0].strip() != '#' :
                return result_2[0]
            elif len(result_3) > 0 :
                return result_3[0]
            return '未知'
        except Exception as e:
            # print e
            return 'format_error'



if __name__ == "__main__":
    import sys

    sys.path.append('../')
    obj = IndustryParser('../dict/fasttext_industry_company_name.model.bin','../dict/fasttext_industry_business.model.bin',
                         '../dict/industry.conf', '../dict/postfix_deterministic.conf', '../dict/postfix_deterministic2.conf',
                         '../dict/company_keyword.conf','../dict/company_name_postfix.conf')
    print obj.predict("乐山市主题装饰工程有限公司","	建筑装饰设计及施工；销售建材。")
    print obj.predict("乐山市中心城区高翔农资产品经营部","	农药种子、肥料、农膜及小型农具零售")
    print obj.predict("乐山市中心城区郭英理发店	理发服务","")
