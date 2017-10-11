# coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )
import jieba
import fasttext
import toolsutil


class IndustyParserFast :
    def __init__(self, company_model_path, business_model_path, ):
        self.company_name_model = fasttext.load_model(company_model_path, label_prefix='__label__')
        self.business_model     = fasttext.load_model(business_model_path, label_prefix='__label__')

    def deal_predict_result(self, result_business) :
        result_list = []
        score_list = []
        if len(result_business) <= 0:
            return result_list
        if len(result_business[0]) <= 0:
            return result_list
        for e in result_business[0] :
            if len(e) <= 0:
                return result_list
            pre = e[0].strip()
            score = e[1]
            result_list.append(pre)
            score_list.append(score)
            # result_list.append(str(score))
        return result_list, score_list

    def name_predict(self, company_name):

        if len(company_name) < 6 :
            return  ''
        seg_name = jieba.cut(company_name.replace(u"\t", u"").replace(u"\n", ""))
        seg_name = " ".join(seg_name)

        result_name = self.company_name_model.predict_proba([seg_name], 1)
        if len(result_name) <= 0 or len(result_name[0]) <= 0 or len(result_name[0][0]) <= 0:
            pre = ''
            score = 0
        else:
            pre = result_name[0][0][0]
            score = result_name[0][0][1]
        return (pre,score)

    def business_predict(self,business_scope):
        business_scope = business_scope.strip()
        if len(business_scope) < 6 :
            return []
        if business_scope == 'null' or business_scope == 'NULL' :
            return []
        seg_business = jieba.cut(business_scope.replace(u"\t", u"").replace(u"\n", ""))
        seg_business = " ".join(seg_business)
        result_business = self.business_model.predict_proba([seg_business], 3)
        business_result, score_list = self.deal_predict_result(result_business)

        return business_result

    def predict_name_and_business(self, company_info) :
        company_info = toolsutil.utf8_decode(company_info)
        fields = company_info.replace(u'\n','').split(u'\t')
        if len(fields) != 2 :
            return
        company_name = fields[0]
        business=fields[1]
        seg_name = jieba.cut(company_name.replace(u"\t", u"").replace(u"\n", ""))
        seg_business = jieba.cut(business.replace(u"\t", u"").replace(u"\n", ""))
        seg_name = " ".join(seg_name)
        seg_business = " ".join(seg_business)
        result_name = self.company_name_model.predict_proba([seg_name],1)
        result_business = self.business_model.predict_proba([seg_business], 3)

        if len(result_name) <= 0 or len(result_name[0]) <= 0  or  len(result_name[0][0]) <= 0 :
            pre = '&'
            score = 0
        else:
            pre = result_name[0][0][0]
            score = result_name[0][0][1]

        business_result, score_list = self.deal_predict_result(result_business)

        for k in business_result :
            if k.strip() == pre.strip() and score >= 0.9:
                return pre
        if len(score_list) > 0 and score_list[0] >= 0.9 :
            return business_result[0]
        elif len(score_list) <= 0 and score >= 0.9 :
            return pre

        return ''


if __name__ == '__main__':
    company_info = "乐山市中心城区高翔农资产品经营部	农药种子、肥料、农膜及小型农具零售"
    fast_text_example = IndustyParserFast("../dict/sample.news_fasttext_industry_company_name.model.bin","../dict/sample.news_fasttext_industry_business.model.bin")

    print company_info.strip()
    print fast_text_example.predict_name_and_business(company_info)

