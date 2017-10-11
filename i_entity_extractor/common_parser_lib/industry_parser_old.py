# coding=utf8

import sys
import jieba
import fasttext

reload(sys)
sys.setdefaultencoding('utf8')
import toolsutil


class IndustryParser:
    def __init__(self, model_path):
        self.classifier = fasttext.load_model(model_path, label_prefix='__label__')

    def predict(self, content):
        '''预测行业'''
        industry = ''
        texts = []
        content = toolsutil.utf8_encode(content.strip())
        if len(content) <= 0:
            return industry

        company_seg = ' '.join(jieba.cut(content))
        texts.append(company_seg)
        if len(texts) >= 100000:
            labels = self.classifier.predict_proba(texts, 1)
            if len(labels) != len(texts):
                texts = []
                return industry
            for i in range(len(labels)):
                if len(labels[i]) <= 0:
                    continue
                label, prob = labels[i][0]
                industry = label
            texts = list()

        if len(texts) > 0:
            labels = self.classifier.predict_proba(texts, 1)
            for i in range(len(labels)):
                if len(labels[i]) <= 0:
                    continue
                label, prob = labels[i][0]
                industry = label

        return industry


if __name__ == "__main__":
    import sys

    sys.path.append('../')
    obj = IndustryParser("../dict/industry_predict.model.bin")
    print obj.predict("深圳市腾讯计算机系统有限公司")
    print obj.predict("海致网络技术（北京）有限公司")
    print obj.predict("阿里巴巴网络技术有限公司")