# coding=utf-8

import sys

sys.path.append('../')
import re
import i_entity_extractor.common_parser_lib.toolsutil as toolsutil
import fygg_conf
import esm

class Fygg_parser:
    def __init__(self, plaintiff_conf_path, defendant_conf_path):

        self.plaintiff_pattern_list = open(plaintiff_conf_path, 'r').read().split('\n')
        self.plaintiff_regex_list   = []
        for plaintiff_pattern in self.plaintiff_pattern_list:
            if not plaintiff_pattern:
                continue
            self.plaintiff_regex_list.append(re.compile(unicode(plaintiff_pattern)))

        self.defendant_pattern_list = open(defendant_conf_path, 'r').read().split('\n')
        self.defendant_regex_list   = []
        for defendant_pattern in self.defendant_pattern_list:
            if not defendant_pattern:
                continue
            self.defendant_regex_list.append(re.compile(unicode(defendant_pattern)))

        self.bulletin_type_index = esm.Index()
        for bulletin_type in fygg_conf.bulletin_type_list:
            self.bulletin_type_index.enter(bulletin_type)
        self.bulletin_type_index.fix()

        self.bulletin_type_list   = [u'其他', u'破产文书', u'公示催告',u'宣告失踪、死亡',u'公益诉讼',u'更正']
        self.norm_content_keyword = u'刊登版面'
        self.litiants_seps        = [',', ':', '，', '：', '。', '、', ";", "；", '\t',u'与']
        self.min_litigant_len     = 2
        self.max_litigant_len     = 40
        self.case_id_regex        = re.compile(u'（\d+）\S+号|(\d+)\S+号')

    def do_parser(self, content):
        '''解析主入口'''
        content                    = unicode(content)
        (norm_content,bulletin_type) = self.before_parser(content)
        info                       = self.get_parser_data(content,bulletin_type)
        info["norm_content"]       = norm_content

        return info

    def before_parser(self, content):
        '''获取格式化后的公告内容'''

        #1 获取格式化内容和公告类型
        content = content.replace(" ","")
        content_list      = content.split()
        norm_content_list = []
        bulletin_type     = ""

        bulletin_type_ret = self.bulletin_type_index.query(content)
        if bulletin_type_ret:
            bulletin_type = bulletin_type_ret[0][1]


        if isinstance(content_list, list) and len(content_list) >= 2:
            if bulletin_type in self.bulletin_type_list:
                for index in range(len(content_list)):
                    if self.norm_content_keyword in content_list[index]:
                        break
                    if index > 0:
                        norm_content_list.append(content_list[index].strip())
            else:
                for index in range(len(content_list)):
                    if self.norm_content_keyword in content_list[index]:
                        break
                    if index > 1:
                        norm_content_list.append(content_list[index].strip())

        norm_content = ','.join(norm_content_list)

        return norm_content,bulletin_type

    def get_parser_data(self, content,bulletin_type):
        '''获取实体信息,当事人,原告,被告,公告类型'''

        plaintiff_list = []
        defendant_list = []
        norm_content   = unicode(content).replace(" ","")
        content_list   = toolsutil.my_split(norm_content, ['，',',', '。','\r\n','\t'])
        find_flag      = False

        #1 获取原告
        for rowcontent in content_list:
            for plaintiff_regex in self.plaintiff_regex_list:
                ret = toolsutil.re_findone(plaintiff_regex, unicode(rowcontent))
                if ret:
                    plaintiff_list = toolsutil.my_split(ret, self.litiants_seps)
                    #print "原告：",plaintiff_regex.pattern,','.join(plaintiff_list)
                    find_flag = True
                    break
            if find_flag:
                break

        #2 获取被告
        if unicode(bulletin_type) in self.bulletin_type_list:
            find_flag = False
            for rowcontent in content_list:
                for defendant_regex in self.defendant_regex_list:
                    ret = toolsutil.re_findone(defendant_regex, unicode(rowcontent))
                    if ret:
                        if u'你' in unicode(ret):
                            defendant_list = toolsutil.my_split(content_list[0], self.litiants_seps)
                        else:
                            defendant_list = toolsutil.my_split(ret, self.litiants_seps)
                        if plaintiff_list == []:
                            plaintiff_list = defendant_list
                            defendant_list = []

                        # print "被告：", defendant_regex.pattern, ','.join(defendant_list)
                        # print "原告：", ','.join(plaintiff_list)
                        find_flag = True
                        break

                for defendant_pattern in fygg_conf.defendant_pattern_list:
                    ret = toolsutil.re_find_one(defendant_pattern,unicode(rowcontent))
                    if ret:
                        defendant_list = toolsutil.my_split(ret, self.litiants_seps)
                        find_flag = True
                        break
                if find_flag:
                    break
            plaintiff_list, defendant_list = self.format_litigant(plaintiff_list, defendant_list,fygg_conf.litigant_replace_str_list)

        else:
            content_list = toolsutil.my_split(norm_content, ['。', '\r\n', '\t','，'])
            for rowcontent in content_list:
                tmp_list = re.split(':|：|;',rowcontent)
                if len(tmp_list) == 2:

                    defendant_list   = toolsutil.my_split(tmp_list[0],self.litiants_seps)
                    replace_str_list = fygg_conf.defendant_keyword_list + fygg_conf.plaintiff_keyword_list
                    plaintiff_list, defendant_list = self.format_litigant(plaintiff_list, defendant_list,replace_str_list)
                    break



        info = {
            "plaintiff_list": plaintiff_list,
            "defendant_list": defendant_list,
            "bulletin_type": bulletin_type,
        }

        return info

    def format_litigant(self,input_plaintiff_list,input_defendant_list, replace_str_list):
        '''格式化当事人'''
        plaintiff_list = []
        defendant_list = []
        for defendant in input_defendant_list:
            defendant = re.sub(u'（\S+）','',unicode(defendant))
            for replace_str in replace_str_list:
                defendant = defendant.replace(replace_str,"")
            if len(unicode(defendant)) >= self.min_litigant_len and len(unicode(defendant)) <= self.max_litigant_len:
                defendant_list.append(defendant)
        for plaintiff in input_plaintiff_list:
            plaintiff = re.sub(u'（\S+）', '', unicode(plaintiff))
            for replace_str in replace_str_list:
                plaintiff = plaintiff.replace(replace_str,"")
            if len(unicode(plaintiff)) >= self.min_litigant_len and len(unicode(plaintiff)) <= self.max_litigant_len:
                plaintiff_list.append(plaintiff)

        plaintiff_list = [x for x in plaintiff_list if x]
        defendant_list = [x for x in defendant_list if x not in plaintiff_list and x]

        new_defendant_list = []
        for defendant in defendant_list:
            if toolsutil.re_findone(self.case_id_regex,defendant) or defendant in plaintiff_list or not defendant:
                continue

            if u'法院' in defendant:
                continue
            defendant = defendant.replace('(','（').replace(')','）')
            defendant = re.sub(u'（\S+）','',defendant)
            new_defendant_list.append(defendant)



        return plaintiff_list,new_defendant_list


def main(obj):
    host = '101.201.100.58'
    port = 27017
    database = 'crawl_data'
    coll = 'court_fygg'
    client = MongoClient(host, port)
    db = client[database][coll]
    cursor = db.find().skip(5)
    num = 0

    for item in cursor:
        num += 1
        src_url = item.get("src_url", '')
        content = item.get("bulletin_content", "")
        content = '''
           "公 告\r\n\r\n\r\n\r\n(2017)渝民终41号\r\n\r\n\r\n\r\n邓昆,四川省正雄投资有限公司,蓝元贵,陈建兴, ,李宗理：\r\n\r\n\r\n本院受理中国农业银行股份有限公司重庆彭水支行,彭水县海天水电开发有限公司诉你们借款合同纠纷一案，因你们下落不明，依照《中华人民共和国民事诉讼法》第九十二条的规定，向你公告送达(2017)渝民终41号举证通知书、应诉通知书、合议庭人员通知书、传票等诉讼文书。自本公告发出之日起，经过六十日，即视为送达。提出答辩状的期限为公告期满后的15日内。举证期限为答辩期满后的10日内。本案将于2017年5月24日上午9：30（遇节假日顺延）在本院第六号法庭公开开庭审理，逾期将依法判决。\r\n\r\n\r\n　特此公告。  \r\n\r\n\r\n\r\n\r\n\r\n     重庆市高级人民法院\r\n\r\n\r\n\r\n刊登日期：2017年02月22日\r\n\r\n\r\n\r\n上传日期：2017年02月22日"
           '''

        data = obj.do_parser(content)
        print src_url
        print "---------------"
        for key, value in data.items():
            if isinstance(value, list):
                for i in value:
                    print key, ":", i
            elif isinstance(value, dict):
                for key2, value2 in value.items():
                    print key2, ":", value2
            else:
                print key, ":", value

        if num > 10:
            break


if __name__ == '__main__':
    import pytoml
    import sys

    sys.path.append('../../')

    from conf import get_config

    topic_id = 34
    with open('../../entity.toml', 'rb') as config:
        config = pytoml.load(config)
    conf = get_config(config)
    import common

    obj = Fygg_parser(conf, '../../dict/plaintiff.conf')
    main(obj)
