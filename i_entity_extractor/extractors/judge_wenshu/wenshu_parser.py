# coding=utf8
import re
import sys
import traceback

sys.path.append("..")
sys.path.append("../../")
from i_entity_extractor.common_parser_lib import toolsutil
import copy
import esm
import wenshu_conf
from i_entity_extractor.common_parser_lib.money_parser import MoneyParser


class Wenshu_parser:
    def __init__(self):
        self.wenshu_conf     = wenshu_conf
        self.money_parser    = MoneyParser()
        self.plaintiff_index = esm.Index()
        for keyword in self.wenshu_conf.plaintiff_keyword_list:
            self.plaintiff_index.enter(keyword)
        self.plaintiff_index.fix()

        self.defendant_index = esm.Index()
        for keyword in self.wenshu_conf.defendant_keyword_list:
            self.defendant_index.enter(keyword)
        self.defendant_index.fix()

        self.litigants_index = esm.Index()
        for keyword in self.wenshu_conf.defendant_keyword_list + self.wenshu_conf.plaintiff_keyword_list:
            self.litigants_index.enter(keyword)
        self.litigants_index.fix()

        self.judiciary_index = esm.Index()
        for keyword in self.wenshu_conf.judiciary_keyword_list:
            self.judiciary_index.enter(keyword)
        self.judiciary_index.fix()

        self.judiciary_keywords = sorted(wenshu_conf.judiciary_keyword_list, cmp=wenshu_conf.comp, reverse=True)

        self.company_type_map = wenshu_conf.company_type_map

        self.procedure_type_map = wenshu_conf.procedure_type_map

        self.cause_last_keys = wenshu_conf.cause_last_keys
        self.cause_first_keys = wenshu_conf.cause_first_keys
        self.cause_last_index = esm.Index()
        for w in self.cause_last_keys:
            self.cause_last_index.enter(w)
        self.cause_last_index.fix()

        self.seps = [',', ':', '\t', ' ']
        self.company_length_limit = 25
        self.max_court_length = 20
        self.court_regex = re.compile(u"\S+法院")

        self.court_pattern2nd = re.compile(u'[\d\s](\w{1,6}[省市县]D+法院)')
        self.court_pattern3rd = re.compile(u'[\d\s](\D+法院)')
        self.notCourtWord = [u'\s', u'本院', u'最高', u'诉至', u'原告', u'代表']
        self.case_date_pattern = re.compile(u'二[Ｏ〇OΟ0○]\S+年\S+月\S+日')

        self.case_id_unser_str = re.compile('|'.join(wenshu_conf.case_id_unser_str))

        self.case_ref_pattern = [re.compile(_) for _ in wenshu_conf.case_ref_pattern]
        self.case_id_split    = re.compile(wenshu_conf.case_id_split)
        self.case_id_word     = re.compile(wenshu_conf.province_bref)
        self.idfilterword     = [re.compile(p) for p in wenshu_conf.idfilterword]

        self.judge_pattern    = wenshu_conf.judge_pattern

        self.money_regex      = re.compile(u'\d+\.\d+万元|\d+万元|\d+\.\d+元|\d+元')
        self.money_regex_chs  = re.compile(u'[一二三四五六七八九十壹贰叁肆伍陆柒捌玖拾]万.*?元|[一二三四五六七八九十壹贰叁肆伍陆柒捌玖拾]千.*?元|[一二三四五六七八九十壹贰叁肆伍陆柒捌玖拾]百.*?元')
        self.money_regex_last = re.compile(u'\d+\.\d+万|\d+万|¥\d+\.\d+|给付申请人\d+\.\d+|给付申请人\d+|给付原告\d+\.\d+|给付原告\d+|案件受理费\d+\.\d+|案件受理费\d+|给付\d+\.\d+|给付\d+|申请执行\d+\.\d+|申请执行\d+')

    def get_case_date(self, content):
        '''获取案件日期'''
        content = unicode(content)
        ret = self.case_date_pattern.findall(content)
        case_date = ""
        if ret:
            case_date_value = ret[0]
            case_date       = toolsutil.norm_date(case_date_value)

        return case_date

    def get_court(self, content):
        """获取法院"""
        court = ''
        content = unicode(content)
        content_list = toolsutil.my_split(content, self.seps)
        for row_content in content_list:
            if not row_content:
                continue
            row_content = unicode(row_content)
            ret = toolsutil.re_find_one(self.court_regex, row_content)
            if ret and len(ret) <= self.max_court_length:
                court = ret
                break
        if court == '':
            court = self.get_court2nd(content)
        return court

    def get_court2nd(self, content):
        """获取法院"""
        court = ''
        content = unicode(content)
        res = toolsutil.re_findone(self.court_pattern2nd,content)
        if not res:
            res = toolsutil.re_findone(self.court_pattern2nd, content)
        if res:
            court = res
        return court

    def get_litigant_type(self, litigants):
        '''获取当事人类型'''
        litigant_info_list = []
        if litigants:
            litigant_list = litigants.split(',')

            for litigant in litigant_list:
                found_flag = False
                for key, value in self.company_type_map.items():
                    if key in litigant:
                        tmp_map = {
                            "name": litigant,
                            "type": value,
                        }
                        litigant_info_list.append(tmp_map)
                        found_flag = True
                        break
                if not found_flag:
                    tmp_map = {
                        "name": litigant,
                        "type": u'个人',
                    }
                    litigant_info_list.append(tmp_map)
        return litigant_info_list

    def get_plaintiff_defendant_content(self, content):
        '''获取包含原告和被告最短内容'''
        result_list = []
        content = content.decode('utf8', 'ignore').encode('utf8', 'ignore')
        content_ret = self.plaintiff_index.query(content)
        if content_ret:
            pos = content_ret[0][0][0]
            content = content[pos:]

        for first_replace_str in self.wenshu_conf.first_replace_str_list:
            content = content.replace(first_replace_str, '')

        for replace_str in self.wenshu_conf.replace_space_str:
            content = content.replace(replace_str, ' ')
        content_list = content.split()

        num = 0
        for content in content_list:
            find_flag = False
            num += 1
            if not content or len(content) < 3:
                continue

            for keyword in self.wenshu_conf.sep_keyword_list:
                if keyword in content:
                    find_flag = True
                    break

            # 3 根据关键字分割出包含原告和被告内容
            if num <= 3:
                if find_flag:
                    continue
                else:
                    # 2 过滤不包含原告被告关键字的内容
                    content = unicode(content)
                    defendant_ret = self.defendant_index.query(content)
                    plaintiff_ret = self.plaintiff_index.query(content)
                    if not defendant_ret and not plaintiff_ret:
                        continue
                    result_list.append(content)
            else:
                if find_flag:
                    break
                else:
                    # 2 过滤不包含原告被告关键字的内容
                    content = unicode(content)
                    defendant_ret = self.defendant_index.query(content)
                    plaintiff_ret = self.plaintiff_index.query(content)
                    if not defendant_ret and not plaintiff_ret:
                        continue
                    result_list.append(content)

        return result_list

    def get_plaintiff_defendant(self, content):
        '''获取原告被告'''
        content_list = self.get_plaintiff_defendant_content(content)

        defendant_ret = self.defendant_index.query(content)
        defendant_ret = self.remove_duplicate_tuple(defendant_ret)
        plaintiff_ret = self.plaintiff_index.query(content)
        plaintiff_ret = self.remove_duplicate_tuple(plaintiff_ret)

        defendant_list = self.get_litigant(defendant_ret, content_list)
        plaintiff_list = self.get_litigant(plaintiff_ret, content_list)

        plaintiff_list = [x for x in plaintiff_list if x not in defendant_list]

        if u'检察院' in ','.join(plaintiff_list) and len(defendant_list) >= 1:
            defendant_list = [defendant.replace(u"人", "") for defendant in defendant_list]

        return (plaintiff_list, defendant_list)

    def remove_duplicate_tuple(self, tuple_data_list, index=1):
        '''元组去重'''
        tmp_map = {}
        result_tuple_list = []
        for item in tuple_data_list:
            if item[index] not in tmp_map.keys():
                tmp_map[item[index]] = 1
                result_tuple_list.append(item)

        return result_tuple_list

    def get_litigant(self, keyword_list, content_list):
        pattern_list = []
        result_list = []
        for ret in keyword_list:
            for end in self.wenshu_conf.company_end_list:
                pattern = ret[1] + '(\S+' + end + ')'
                pattern_list.append(pattern)
            pattern = ret[1] + '(\S+)'
            pattern_list.append(pattern)

        pattern_list = list(set(pattern_list))
        for content in content_list:
            for pattern in pattern_list:
                ret = toolsutil.re_find_one(unicode(pattern), content)
                if ret:
                    if len(unicode(ret)) < self.company_length_limit:
                        result_list.append(ret)

        result_list = self.norm_entity_list(result_list)

        return result_list

    def norm_entity_list(self, entity_list):
        '''去除公司列表中所有子集,如 佛山市顺德区龙江镇教育局  顺德区龙江镇教育局'''
        result_list = []
        for entity1 in entity_list:
            found = False
            for entity2 in entity_list:
                if entity1 == entity2:
                    continue

                if entity2 in entity1:
                    found = True
                    break
            if not found and len(unicode(entity1)) < self.company_length_limit:
                result_list.append(entity1)
        result_list = list(set(result_list))
        return result_list

    def get_money_list(self, judge_content):
        '''获取涉案最大金额'''
        tmp_max_money_list = toolsutil.my_split(judge_content, [',', '，', '。'])
        ret_list = toolsutil.re_findall(self.money_regex, unicode(judge_content))

        money_list = []
        for row_content in tmp_max_money_list:
            ret_chs = toolsutil.re_findone(self.money_regex_chs, unicode(row_content))
            if ret_chs:
                chs_money = self.money_parser.trans_chs_money(ret_chs)

                money_list.append(float(chs_money[0]))

        if ret_list:
            for ret in ret_list:
                digit_money = self.money_parser.transfer_money(ret)
                money_list.append(float(digit_money[0]))

        if money_list == []:
            ret_list2 = toolsutil.re_findall(self.money_regex_last, unicode(judge_content))
            if ret_list2:
                for ret in ret_list2:
                    digit_money = self.money_parser.transfer_money(ret)
                    money_list.append(float(digit_money[0]))
        if money_list != []:
            max_money = max(money_list)
            sum_money = sum(money_list)
        else:
            max_money = 0
            sum_money = 0

        return max_money,sum_money

    def case_id_filter(self, case_id):
        if not re.search(self.case_id_word, case_id):
            case = re.sub(u'（|）|\d|号', '', case_id)
            if len(case) < 5:
                return True
        return False

    def getCaseRefId(self, content):
        """获取案件ID"""
        content = content.replace(u' ', '')
        case_id_list = []
        flag = False
        content_list = [self.strQ2B(str) for str in re.split(self.case_id_split, content) if len(str) > 10 and re.search(u'\d', str)]

        # 处理待顿号片段，将带顿号的片段切分
        contentList = []
        for c in content_list:
            if u'、' in c and not re.search(u'、[、\d-]+号', c):
                lis = re.split(u'、', c)
                lis = [l for l in lis if len(l) > 8 and u'仲裁' not in l]
                contentList += lis
            else:
                contentList.append(c)
        for string in contentList:
            string = string.replace('\t', '').replace(' ', '').replace(' ', '')
            for patern in self.case_ref_pattern:

                if u'决定书' in patern.pattern and case_id_list:   # 所有含'决定书'的patern共配一次
                    continue
                case = re.search(patern, string)
                if case:
                    find_id = case.group(1)

                    # 处理案号中包含有'年'的案件
                    if u'年' in find_id:
                        year = re.search(u'(\d{4})年', find_id)
                        if year:
                            find_id = re.sub(u'\d{4}年', year.group(1), find_id)
                    tmp = self.reduceBracket(find_id)
                    flag1 = False
                    for w in self.idfilterword:
                        if re.search(w, tmp) or not re.search('\d', tmp):
                            flag1 = True
                            break
                    if flag1:
                        continue
                    if len(re.subn(u'^（\d+）', '', tmp)[0]) < 5 or len(re.sub(u'\d|-|、', '', tmp)) > 15:
                        continue
                    if tmp not in case_id_list:
                        tmp = re.sub(self.case_id_unser_str, '', tmp)
                        if re.search(u'号\d+$', tmp):
                            tmp = re.sub(u'\d+$', '', tmp)
                        if self.case_id_filter(tmp):
                            continue
                        case_id_list.append(tmp.strip())
                        flag = True
                        break
            if flag:
                flag = False
                continue
        if len(case_id_list) == 0:
            return '', case_id_list
        return case_id_list[0], list(set(case_id_list))

    def reduceBracket(self, case_id):
        """给case_id的年份添加括号"""
        case_id = re.subn(u'[^、()\u4e00-\u9fa5\xd7\dXxO-]', '', case_id)[0]
        m = re.search(u'((?:20|19)\d{2})[^\u4e00-\u9fa5]?([^)].*)', case_id)
        if m:
            case_id = u'\uff08' + m.group(1) + u'\uff09' + m.group(2)  # 添加括号
        case_id = case_id.replace('(', u'\uff08').replace(')', u'\uff09')  # 英文括号替换为中文括号
        return case_id

    def addBracket(self, case_id):
        """给case_id的年份添加括号"""
        m = re.match(u'((?:20|19)\d{2})', case_id)
        if m:
            case_id = re.sub(m.group(1), u'\uff08' + m.group(1) + u'\uff09', case_id)  # 添加括号
        case_id.replace('(', u'\uff08').replace(')', u'\uff09')  # 英文括号替换为中文括号
        return case_id

    def get_judge_content(self, content):
        """获取判决结果"""
        for reg in self.judge_pattern:

            seach = re.search(re.compile(reg), content)
            if seach:
                result = seach.group(1)
                if u'下:' in result:  # 处理二审的案件，找最后一个审判结果
                    judge = self.get_judge_content(result)
                    if judge:
                        return judge
                else:
                    return result
            else:
                continue
        return ''

    def norm_content(self, doc_content):
        '''格式化文档内容'''
        doc_content = unicode(doc_content)
        for key in self.wenshu_conf.replace_str_list:
            doc_content = doc_content.replace(key, '')
        return doc_content

    def replace_str(self, data_list):
        result_list = []

        for item in data_list:
            for key in self.wenshu_conf.replace_str_list:
                item = unicode(item)
                item = item.replace(key, '')
            flag = False
            for wrong_str in self.wenshu_conf.wrong_str_list:
                if wrong_str in item:
                    flag = True
                    break
            if not flag:
                result_list.append(item)
        return result_list

    def format_litigants(self, plaintiff_list, defendant_list, litigants):
        '''格式化当事人，被告，原告'''
        litigant_list = toolsutil.my_split(litigants, self.seps)
        plaintiff_list = [unicode(x) for x in plaintiff_list if x in litigant_list]
        defendant_list = [unicode(x) for x in defendant_list if x in litigant_list]

        for wrong_str in self.wenshu_conf.wrong_str_list:
            plaintiff_list = [x for x in plaintiff_list if wrong_str not in x]
            defendant_list = [x for x in defendant_list if wrong_str not in x]

        return litigant_list, plaintiff_list, defendant_list

    def get_procedure_from_content(self,content):
        '''从内容中获取审理程序'''
        content = unicode(content)
        if content.find(u'二审') != -1 or content.find(u'再审') != -1 :
            procedure = u'再审'
        elif content.find(u'一审') != -1:
            procedure = u'二审'
        else:
            procedure = u'一审'
        return procedure

    def get_procedure(self, case_id, content):
        if u'初' in case_id:
            return u'一审'
        elif u'终' in case_id:
            return u'二审'
        else:
            for key, value in self.procedure_type_map.iteritems():
                for w in value:
                    if w in content:
                        return key
        return ''

    def strQ2B(self, ustring):
        '''全角转半角'''
        # reload(sys)
        # sys.setdefaultencoding('utf-8')
        ustring = unicode(ustring)
        rstring = ''
        for char in ustring:
            inside_code = ord(char)

            if inside_code == 79:  # 中文右括号
                inside_code = 48
            if inside_code == 12304 or inside_code == 12308:  # 中文右括号
                inside_code = 91
            if inside_code == 12305 or inside_code == 12309:  # 中文左括号
                inside_code = 93
            if inside_code == 12288 or inside_code == 32:  # 去除空格
                continue
            # if inside_code == 12289:   # 中文顿号转英文分号符
            #     inside_code = 59
            if inside_code == 12290:  # 中文句号
                inside_code = 46
            if inside_code == 8220 or inside_code == 8221:  # 中文双引号
                inside_code = 34
            if inside_code == 8216 or inside_code == 8217:  # 中文单引号
                inside_code = 39
            if (inside_code >= 65281 and inside_code <= 65374):
                inside_code -= 65248

            rstring += unichr(inside_code)
        return rstring

    def getJudiciaryList2nd(self, content):
        """获取审判委员会成员"""
        content = unicode(content)
        content_list = re.split(u'、|，|。|本裁定|附.*?[(法律)录]|申请执行|此页无正文', content)
        content_list.reverse()
        judiciary_list = []

        for row_content in content_list:
            if not re.search(u'二.*?年.*?[日 　]', row_content):
                continue
            row_content = re.sub(u'二.*?年.*?[日 　]', '', row_content)
            row_content = re.sub(u'[^\u4e00-\u9fa5]', '', row_content)

            for keyword in self.judiciary_keywords:
                row_content = re.sub(keyword.decode(), ' ', row_content)
            judiciary_list = row_content.split()
            if len(judiciary_list) > 0:
                break
        judiciary_list = [judiciary for judiciary in judiciary_list if 2 <= len(judiciary) <= 6]
        if len(judiciary_list) == 1 and len(judiciary_list[0]) > 3:
            judiciary_list = []
        return judiciary_list

    def getJudiciaryList(self, content):
        """获取审判委员会成员"""
        content = unicode(content)
        content_list = re.split(u'、|，|。', content)
        content_list.reverse()
        judiciary_list = []
        for row_content in content_list:
            if not re.search(u'二.*?年.*?[日 　]', row_content):
                continue
            row_content = re.sub(u'： |：|　|二.*?年.*?[日 　]', '', row_content)
            patern = u'(?:审[^\u4e00-\u9fa5]*?判[^\u4e00-\u9fa5]*?长|审[^\u4e00-\u9fa5]*?判[^\u4e00-\u9fa5]*?员|陪[^\u4e00-\u9fa5]*?审[^\u4e00-\u9fa5]*?员|执[^\u4e00-\u9fa5]*?行[^\u4e00-\u9fa5]*?员|法[^\u4e00-\u9fa5]*?官[^\u4e00-\u9fa5]*?助[^\u4e00-\u9fa5]*?理|院[^\u4e00-\u9fa5]*?长|书[^\u4e00-\u9fa5]*?记[^\u4e00-\u9fa5]*?员|速[^\u4e00-\u9fa5]*?录[^\u4e00-\u9fa5]*?员)(\S*)'
            judiciary_list = re.findall(patern, unicode(row_content))
            if len(judiciary_list) > 0:
                break
        return judiciary_list

    def getPlaintiffDefendant(self, content):
        """获取原告被告"""
        decontent = copy.deepcopy(content)
        pdcontent = re.match(u'.*?一案', content)
        for replace_str in self.wenshu_conf.replace_space_str1:
            decontent = decontent.replace(replace_str, ' ')
            decontent = decontent.replace(u'与', '，被告')
        if pdcontent:
            pdcontent = pdcontent.group().replace(u'与', ' ')
        else:
            return [], []
        content_list = self.get_plaintiff_defendant_content(pdcontent)
        defendant_ret = self.defendant_index.query(pdcontent)
        defendant_ret = self.remove_duplicate_tuple(defendant_ret)
        plaintiff_ret = self.plaintiff_index.query(pdcontent)
        plaintiff_ret = self.remove_duplicate_tuple(plaintiff_ret)

        defendant_list = self.get_litigant(defendant_ret, content_list)
        plaintiff_list = self.get_litigant(plaintiff_ret, content_list)
        if len(defendant_list) == 0:
            content_list = self.get_plaintiff_defendant_content(decontent)
            defendant_ret = self.defendant_index.query(decontent)
            defendant_list = self.get_litigant(defendant_ret, content_list)

        plaintiff_list = [x for x in plaintiff_list if x not in defendant_list]

        if u'检察院' in ','.join(plaintiff_list) and len(defendant_list) >= 1:
            defendant_list = [re.sub(u"^人", "", defendant) for defendant in defendant_list]
        return (plaintiff_list, defendant_list)

    def getLitigantFromNotice(self, content):
        """内容是通知书形式"""
        notice = re.search(u'通.?知.?书.{5,25}号(.*?)：', content)
        if notice:
            deplain = notice.group(1)
            deplain = re.sub(u'[^\u4e00-\u9fa5]', ' ', deplain)
            plaintiff_list = deplain.split()
            return plaintiff_list, []
        else:
            return [], []

    def get_bulletin_date(self, content):
        '''从正文中抽取公告日期'''
        content = unicode(content)[:100]
        print content
        date_ret = toolsutil.re_find_one('\d{4}-\d{1,2}-\d{1,2}',content)
        if date_ret:
            return toolsutil.norm_date_time(date_ret)
        else:
            return ""

    def do_parser(self, content, litigants):
        '''解析文书内容主程序'''

        content = unicode(content)
        doc_content = re.sub('\t|\n', ' ', content)
        content_filter = self.strQ2B(doc_content)
        judge_content = self.get_judge_content(content_filter)
        max_money,all_money = self.get_money_list(judge_content)

        judiciary_list = self.getJudiciaryList(content)
        length = [len(i) for i in judiciary_list]
        if len(judiciary_list) == 0 or max(length) > 6 or min(length) <= 1:
            judiciary_list = self.getJudiciaryList2nd(content)
        (case_id, ref_ids) = self.getCaseRefId(doc_content)
        (plaintiff_list, defendant_list) = self.get_plaintiff_defendant(doc_content)

        if len(plaintiff_list + defendant_list) == 0:
            (plaintiff_list, defendant_list) = self.getPlaintiffDefendant(doc_content)
        if len(plaintiff_list + defendant_list) == 0:
            (plaintiff_list, defendant_list) = self.getLitigantFromNotice(doc_content)
        plaintiff_list = self.replace_str(plaintiff_list)
        defendant_list = self.replace_str(defendant_list)

        if litigants:
            litigants = unicode(litigants)

            for key in self.wenshu_conf.replace_str_list:
                litigants = litigants.replace(key, '')
            litigant_list, plaintiff_list, defendant_list = self.format_litigants(plaintiff_list, defendant_list,
                                                                                  litigants)
        else:
            litigant_list = plaintiff_list + defendant_list
            litigants = ','.join(litigant_list)
        litigant_info_list = self.get_litigant_type(litigants)
        court = self.get_court(content)
        procedure = self.get_procedure(case_id, content)
        case_date = self.get_case_date(content)

        chain_case_id = []
        if u'初' in case_id:
            chain_case_id.append(case_id)

        parser_data = {
            "case_date": case_date,
            "max_money": max_money,
            "all_money": all_money,
            "ref_ids": ref_ids,
            "case_id": case_id,
            "court": court,
            "judge_content": judge_content,
            "judiciary_list": judiciary_list,
            "plaintiff_list": plaintiff_list,
            "defendant_list": defendant_list,
            "litigant_list": litigant_list,
            "litigant_info_list": litigant_info_list,
            "litigants": litigants,
            "procedure": procedure,
            "chain_case_id": chain_case_id
        }

        return parser_data





if __name__ == "__main__":
    from i_entity_extractor.common_parser_lib.mongo import MongDb
    import json
    obj = Wenshu_parser()
    mongo_conf = {
        'host': '172.16.215.2',
        'port': 40042,
        'final_db': 'final_data',
        'username': "readme",
        'password': "readme",
    }
    db = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
                mongo_conf['username'],
                mongo_conf['password'])

    cursor = db.db["judgement_wenshu"].find({})
    num = 0
    import time
    begin_time = time.time()
    for item in cursor:
        # (case_id, ref_ids) = obj.getCaseRefId(item.get('doc_content', ''))
        # print '-'*30
        # print case_id
        # print ', '.join(ref_ids)
        # continue
        try:
            num += 1
            content = item.get("doc_content", '')
            content = "张建康与杨海峰、宝鸡市众信出租汽车有限责任公司、中国人民财产保险股份有限公司宝鸡市分公司机动车交通事故责任纠纷一审民事判决书2016-11-08文书内容宝鸡市金台区人民法院民 事 判 决 书2016陕0303民初1038号原告张建康，男，生于1974年11月3日，汉族，住宝鸡市金台区西新村被告杨海峰，男，生于1984年10月20日，汉族，住宝鸡市渭滨区马营镇郭家村被告宝鸡市众信出租汽车有限责任公司，住所地宝鸡市金台区法定代表人王乃民，任公司总经理委托代理人周侃，系公司员工，特别授权被告中国人民财产保险股份有限公司宝鸡市分公司，住所地宝鸡市渭滨区负责人王晓林，任公司总经理委托代理人王乖明，系公司员工，一般代理原告张建康与被告杨海峰、宝鸡市众信出租汽车有限责任公司以下简称“众信出租车公司”、中国人民财产保险股份有限公司宝鸡市分公司以下简称“人保财险宝鸡分公司”机动车交通事故责任纠纷一案，本院于2016年4月12日立案受理依法由审判员李琼适用简易程序公开开庭进行了审理原告、被告杨海峰、被告众信出租车公司的代理人、被告人保财险宝鸡分公司的代理人到庭参加了诉讼本案现已审理终结原告张建康起诉称，2015年5月19日21时许，原告张建康驾驶自行车沿宏文路由南向北行驶至解放军第三医院后门处时，与停放在该处非机动车道内被告杨海峰驾驶的第二被告所有的陕CT7885号小型轿车发生碰撞，致原告受伤，车辆受损原告经及时救治，确诊为1、轻度颅脑损伤2、+外伤性冠折伴牙髓炎3、右下肢软组织挫伤2015年6月2日，宝鸡市公安局交通警察支队金台大队事故认定书认定，原告负本次事故的同等责任，被告杨海峰负本次事故同等责任原告受伤后，在宝鸡市第三人民医院救治，住院6天出院后，原告经宝鸡中园法医司法鉴定所鉴定，原告因交通事故受伤致+外伤性冠折，后期需行牙冠修复，两颗牙齿每颗费用需人民币500元，四年更换一次经了解，肇事车辆陕CT7885号车辆在第三被告处投保有交强险及商业险，被告众信出租车公司系车主现原告诉至本院，请求判令1、被告共同赔偿原告因交通事故造成的损失1769356元不含被告杨海峰已垫付的500元，被告人保财险宝鸡分公司在交强险及商业三者险范围内直接向原告支付，超出部分由被告杨海峰与被告众信出租车公司承担2、本案诉讼费由被告承担原告为支持其诉讼请求，向本院递交以下证据1、宝鸡市第三人民医院医疗费发票4张、住院病人费用明细汇总，证明原告住院花费情况2、司法鉴定费发票1张、交通费发票31张，证明原告住院期间花费了交通费的事实，及出院后进行鉴定花费了鉴定费的事实3、张建斌工资表、误工证明、张建斌身份证复印件，证明原告的护理人因护理原告扣发工资的事实4、交通事故认定书，证明事故发生的事实及责任划分5、宝鸡市第三人民医院住院病案、出院证、门诊病历，证明原告因事故住院的事实6、司法鉴定意见书，证明原告经鉴定牙齿需要后续治疗的事实被告杨海峰答辩称，对事故发生的事实无异议，但原告系醉酒骑行自行车，对同等责任划分有异议，我驾驶的车辆系被告众信出租车公司所有，在被告保险公司处投保有交强险及商业三者险，对原告的合理损失应当由被告保险公司首先在保险限额内承担赔偿责任，我给原告垫付了500元医疗费及60元交通费被告杨海峰为支持其答辩意见，向本院递交以下证据收条一张，证明被告杨海峰给原告垫付了500元医疗费的事实被告众信出租车公司答辩称，对事故真实性无异议，但对责任认定书中的责任划分有异议，原告醉驾自行车，我们在被告人保财险宝鸡分公司处投保了交强险及商业三者险，原告的合理损失首先由被告保险公司在保险限额内理赔，我们不承担诉讼费被告众信出租车公司未向本院递交证据被告人保财险宝鸡分公司答辩称，对被告杨海峰驾驶的由被告众信出租车公司所有的陕CT7885号小型轿车在我公司投保交强险及商业三者险的事实我公司不持异议，对事故发生的事实不持异议，但对事故认定书认定的同等责任有异议，对原告的合理损失，我公司愿在交强险分项限额内赔偿，超出的部分在商业三者险限额内按照责任比例承担赔偿责任被告人保财险宝鸡分公司为支持其答辩意见，向本院递交以下证据交强险及商业险保单抄件，证明被告的车辆在其公司投保交强险和商业险的事实对原、被告提交的证据，经过庭审质证，本院做如下分析认定对原告提交的证据1、2中的司法鉴定费发票、证据3中的护理人张建斌身份证复印件4、5、6真实性杨海峰与被告众信公司不持异议，被告人保财险宝鸡分公司对证据1中的住院费发票、住院病人费用明细单、证据2中的司法鉴定费发票、证据3中张建斌身份证复印件、证据4、证据5、证据6真实性不持异议，本院对原告证据1中的住院费收据、住院病人费用明细单、证据2中的鉴定费发票、证据4、证据5、证据6的真实性予以认定，被告人保财险宝鸡分公司对证据1中的门诊费收据真实性不认可，认为门诊费收据上的挂号收据没有盖章，其余2张门诊费收据公章和住院费收据公章不同，本院认为，门诊费挂号收据及门诊检查收据日期为2015年6月8日及2015年6月13日，科别为口腔科，根据原告的门诊病历，原告在2015年6月8日及6月13日分别进行了一次口腔科门诊检查治疗，时间及检查项目相符，票据系正规发票，且门诊检查需要挂号是医院常规规则，门诊费产生具备合理性，故本院对该三张门诊收费票据予以认定三被告对证据4责任划分有异议，本院认为，事故认定书系公安交警部门经过事故现场勘查后依法做出，事故认定书下方亦向当事人提示了对事故认定书有异议的，可以在三日内向上一级公安机关交通管理部门提出书面复核申请，被告在指定期间并未申请复核，现该认定书已经生效，本院对三被告的质证意见不予采信，对事故认定书的证明目的予以认可三被告对证据2中的交通费发票真实性不认可，本院认为，被告杨海峰当庭陈述其在事故当天送原告去医院花费了60元交通费，原告对此予以认可，且原告提供的票据中确实包含了事故当天晚上的出租车发票共计8030元，原告在2015年5月20日凌晨1点到宝鸡市第三人民医院住院，2015年5月20日的发票时间与住院时间能相印证，本院对2015年5月19日及5月20日的出租车发票5张予以认定，其余交通费发票，均系正规发票，但原告未能对费用支出作出解释，本院对其余票据的证明目的不予认定三被告对证据3中的工资表和误工证明真实性不认可，认为工资表和扣款证明是后补的，且工资已经超过了3500元，应当提交纳税证明，本院认为，被告杨海峰出具的给原告垫付500元的收条系张建斌出具，可以证实张建斌在医院护理原告的事实，工资表和证明能够相互印证，且加盖有宝鸡市周礼乐团文化有限责任公司的印章，均能证实张建斌因请假6天被扣款720元的事实，被告虽质证认为按照护理人每天120元的工资收入，其月工资已经超出纳税标准，但工资表上反映出护理人的月工资均在两千多元，故本院对被告的质证意见不予采信，对该组证据予以认定三被告认为证据5中出院证和住院病案中的出院记录相矛盾，住院病案中的出院医嘱记载原告出院后休息1周，出院证中记载原告出院后休息2周，本院认为，因两份证据存在矛盾，参照原告的伤情，应作出不利的解释，即原告出院后误工期应当按照1周计算，故本院对出院证的证明目的不予认定对被告杨海峰提交的收条，原告及被告众信出租车公司、人保财险宝鸡分公司对真实性及证明目的均不持异议，本院予以认定对被告人保财险宝鸡分公司提交的交强险及商业险保单，原告及被告杨海峰、众信出租车公司对真实性及证明目的均不持异议，本院予以认定根据原、被告的当庭陈述及本院认定的证据，经审理查明如下法律事实2015年5月19日21时50分，原告驾驶自行车沿宏文路由南向北行驶至解放军第三医院后门时，与停放在该处非机动车道内被告杨海峰驾驶的陕CT7885号小型轿车发生碰撞，致原告受伤，车辆受损，造成道路交通事故本次事故经宝鸡市公安局交警支队金台大队作出宝公金交认字2015第0272号《道路交通事故认定书》认定，原告张建康负本次事故的同等责任，被告杨海峰负本次事故同等责任原告于2015年5月20日凌晨被送往宝鸡市第三人民医院住院治疗，经诊断为1、轻度颅脑损伤；2、+外伤性冠折伴牙髓炎；3、右下肢软组织挫伤住院6天，期间医嘱留陪人原告于2015年5月26日出院，出院时医嘱1、休假一周，继续服药并处理牙齿；2、定期门诊复查；3、如有不适及时来院就诊原告住院期间花费住院医疗费237060元，其中被告杨海峰垫付500元原告出院后于2015年5月29日、6月8日、6月13日门诊检查，花费门诊医疗费3393元原告住院期间由张建斌护理，张建斌为宝鸡市周礼乐团文化有限责任公司职工，其因护理原告6天被扣发工资720元2016年3月22日，原告向陕西宝鸡中园法医司法鉴定所申请鉴定，该所于2016年3月24日作出陕宝中园司鉴所2016临鉴字第229号司法鉴定意见书，鉴定意见为原告因交通事故受伤被致成+外伤性冠折，后期需行牙冠修复，共需冠修两颗牙齿，每颗牙齿冠修费用需人民币500元，四年更换一次原告本次鉴定花费鉴定费800元此外，被告杨海峰在事故发生当晚为原告垫付交通费60元另查，被告杨海峰所驾驶的陕CT7885号小型轿车系众信出租车公司所有，原告系众信公司驾驶员，陕CT7885号小型轿车在被告人保财险宝鸡分公司投保有交强险及商业险，交强险保险限额为122000元，保险期限为2015年1月22日至2016年1月21日，商业三者险保险限额1000000元，为保险期限为2015年1月23日至2016年1月22日本次事故发生在保险期间本院认为，根据《中华人民共和国侵权责任法》第十六条规定，侵害他人造成人身损害的，应当赔偿医疗费、护理费、交通费等为治疗和康复支出的合理费用，以及因误工减少的收入原告因交通事故受伤，被告杨海峰系被告众信出租车公司的司机，其驾驶车辆营运系职务行为，被告众信出租车公司作为车辆的所有人应当对原告的损失承担赔偿责任，被告杨海峰不承担赔偿责任根据《中华人民共和国道路交通安全法》第七十六条规定，因被告被告众信出租车公司所有的陕CT7885号车辆在被告人保财险宝鸡分公司投保有交强险及商业三者险，故原告损失首先由被告人保财险宝鸡分公司在交强险分项限额内予以赔付，超出交强险部分由人保财险宝鸡分公司在商业三者险限额内根据责任划分予以赔付，交强险及商业三者险不足赔付的部分由被告众信出租车公司赔偿对原告的损失，本院做如下认定1、医疗费原告花费住院医疗费237060元，出院后花费门诊医疗费3393元，共计576360元，均有票据证实，本院予以认定，其中被告杨海峰垫付500元因原告经鉴定，需冠修两颗牙齿，每颗牙齿冠修费用需人民币500元，四年更换一次，根据人均寿命72岁计算，原告受伤时满40周岁，按照四年更换一次，原告共需更换8次按照每次两颗牙1000元计算，原告的后续治疗费为8000元原告花费的医疗费及后续治疗费共计1376360元2、住院伙食补助费原告住院6天，按照本地国家机关一般公务人员出差伙食补助标准60元每天计算，原告的住院伙食补助费为360元，本院对原告该项请求予以支持3、误工费原告主张误工费按照200元每天计算20天，本院认为，根据原告提交的住院病案及出院证，同一医生对出院休息时间医嘱不同，两份证据存在矛盾，应当作出对原告不利的解释，故原告出院后的误工天数应当按照7天计算，原告住院6天，共计误工13天，原告称其从事个体经营烧烤，但未提交工商登记证明及误工证明，故本院认为原告主张200元每天过高，参照本地一般劳务人员工资标准100元每天计算，原告的误工费为1300元4、护理费原告主张护理费720元，有护理人所在单位出具的工资表及误工证明证实，本院予以认定5、鉴定费原告因鉴定花费鉴定费800元，属于其受伤后合理必要的花费，本院对原告该项请求予以支持，被告人保财险宝鸡分公司虽辩称该费用其不承担，但未提供法律依据，故本院对其辩解意见不予支持6、交通费交通费应当与原告及其必要的陪护人员就医治疗的时间、次数相印证，原告主张交通费200元，事发当晚花费交通费8030元，其余为其住院期间及出院后门诊检查的花费，本院综合原告的住址及其就医看病的次数认为，原告主张200元符合实际情况，本院对交通费200元予以支持，其中被告杨海峰为原告垫付交通费60元综上，原告的花费有医疗费含后续治疗费1376360元、住院伙食补助费360元、误工费1300元、护理费720元、鉴定费800元、交通费200元，共计1714360元原告的损失首先由被告人保财险宝鸡分公司在交强险医疗费10000元限额内赔偿医疗费10000元、在交强险死亡伤残110000元限额内赔偿误工费1300元、护理费720元、鉴定费800元、交通费200元被告人保财险宝鸡分公司在交强险项下共计赔偿原告13020元超出交强险赔偿限额的部分为医疗费376360元、住院伙食补助费360元，共计412360元根据《陕西省实施办法》第六十九条第三项规定，机动车与非机动车发生交通事故负同等责任的，机动车一方承担60%的责任，故被告人保财险宝鸡分公司在商业三者险项下赔偿原告412360元的60%即247416元，剩余164944元由原告自行承担因原告的损失未超出保险限额，被告众信出租车公司不再承担赔偿责任因被告杨海峰为原告垫付了医疗费500元、交通费60元，被告人保财险宝鸡分公司在理赔时向被告杨海峰返还560元，向原告赔偿1493416元综上，依照《中华人民共和国侵权责任法》第十六条、第四十八条、《中华人民共和国道路交通安全法》第七十六条、《最高人民法院关于审理人身损害赔偿案件适用法律若干问题的解释》第十七条、第十九条、第二十条、第二十一条、第二十二条、第二十三条、《陕西省实施办法》第六十九条第三项之规定，判决如下一、被告中国人民财产保险股份有限公司宝鸡市分公司于本判决生效之日起三十日内在交强险项下赔偿原告张建康13020元，在商业三者险项下赔偿原告247416元，交强险及商业三者险共计赔付原告1549416元实际理赔时，赔偿原告张建康1493416元，返还被告杨海峰560元二、驳回原告张建康对被告杨海峰的诉讼请求三、驳回原告张建康对被告宝鸡市众信出租汽车有限责任公司的诉讼请求如果未按本判决指定的期间履行给付金钱义务，应当依照《中华人民共和国民事诉讼法》第二百五十三条之规定，加倍支付迟延履行期间的债务利息本案案件受理费242元，减半收取121元，由原告张健康承担4840元，由被告杨海峰承担7260元如不服本判决，可在接到判决书之日起十五日内向本院递交上诉状，并按对方当事人的人数提出副本，同时交纳上诉案件受理费，上诉于陕西省宝鸡市中级人民法院上诉期满后七日内未交纳上诉案件受理费的，按自动撤回上诉处理审判员　　李琼二〇一六年六月十三日书记员　　孙丹"

            parser_data = obj.get_bulletin_date(content)
            print parser_data
            # print "---------------------------------"
            # print "title:",item.get("case_name")
            # for key, value in parser_data.items():
            #     if isinstance(value, list):
            #         for i in value:
            #             if isinstance(i, dict):
            #                 for key2, value2 in i.items():
            #                     print key2, ":", value2
            #             else:
            #                 print key, ":", i
            #     elif isinstance(value, dict):
            #         for key2, value2 in value.items():
            #             print key2, ":", value2
            #     else:
            #         print key, ":", value
            if num >= 10:
                break
        except:
            print traceback.format_exc()


    print "time_cost:",time.time()-begin_time

    from bson.objectid import ObjectId
    data = {
        "_id": ObjectId("5807d377c65bb72e656acd9d"),
    "province" : "吉林",
    "max_money" : 5136.0,
    "chain_case_id" : [
        "（2015图民初字第513号"
    ],
    "litigant_info_list" : [
        {
            "type" : "地产物业公司",
            "name" : "图们市城乡建设开发有限公司"
        },
        {
            "type" : "个人",
            "name" : "张强"
        }
    ],
    "case_date" : "2015-07-27 00:00:00",
    "bulletin_date" : "",
    "href" : "http://wenshu.court.gov.cn/content/content?DocID=a094f6a5-7a6d-4529-85ba-7c94b1eac74e",
    "_record_id" : "789c24206d4f61d4e944ee5d7ec24f7b",
    "litigant_list" : [
        "图们市城乡建设开发有限公司",
        "张强"
    ],
    "case_cause" : "",
    "court" : "吉林省图们市人民法院",
    "judiciary_list" : [
        "金旭",
        "崔正植",
        "李馥亿",
        "周雪峰"
    ],
        "_src": [
            {
                "url": "http://wenshu.court.gov.cn/CreateContentJS/CreateContentJS.aspx?DocID=a094f6a5-7a6d-4529-85ba-7c94b1eac74e",
                "site_id": -4110544990130519783,
                "site": "wenshu.court.gov.cn"
            }],
    "company_list" : [
        "图们市城乡建设开发有限公司",
        "图们市国税局住宅楼接受我公司"
    ],
    "_in_time" : "2016-10-20 04:11:43",
    "ref_ids" : [
        "（2015）图民初字第513号"
    ],
    "case_id" : "（2015）图民初字第513号",
    "_utime" : "2017-02-09 11:03:30",
    "defendant_list" : [
        "张强"
    ],
    "litigants" : "图们市城乡建设开发有限公司,张强",
    "case_name" : "图们市城乡建设开发有限公司与张强供用热力合同纠纷一审民事判决书",
    "judge_content" : "被告张强于本判决发生法律效力之日立即给付原告图们市城乡建设开发有限公司供热费用5136元.如果未按本判决指定的期间履行给付金钱义务,应当按照《中华人民共和国民事诉讼法》第二百五十三条之规定,加倍支付迟延履行期间的债务利息.案件受理费50元,由被告张强负担.如不服本判决,可在判决书送达之日起十五日内,向本院递交上诉状,并按对方当事人人数提出副本,上诉于延边朝鲜族自治州中级人民法院.",
    "doc_content" : "图们市城乡建设开发有限公司与张强供用热力合同纠纷一审民事判决书 2016-11-08  {C} 吉林省图们市人民法院 民事判决书 （2015图民初字第513号 原告图们市城乡建设开发有限公司，住所：图们市二七街178-29号。 法定代表人朴京淳，经理。 委托代理人张志勇。 委托代理人李京洙，吉林李京洙律师事务所律师。 被告张强。 委托代理人许艳婷（张强妻子。 原告图们市城乡建设开发有限公司（下称城乡供热公司诉被告张强供用热力合同纠纷一案，本院于2015年4月28日立案受理后，依法组成合议庭，于2015年7月24日公开开庭进行了审理。原告委托代理人张志勇、李京洙、被告委托代理人许艳婷到庭参加诉讼。本案现已审理终结。 原告城乡供热公司诉称，图们市国税局住宅楼接受我公司供热，而被告系该住宅楼2-8-2室的业主。现被告尚欠我公司2013年冬季至2015年春季的取暖费5136元。故提起诉讼，请求依法判令被告立即支付拖欠我公司的取暖费5136元。 被告辩称，没有缴纳取暖费是因为原告供热温度太低，我家每天都使用电热板，我们曾多次找原告反映供热温度不达标并要求测温，但原告没有来测温也没有解决问题。 经审理查明：被告张强系图们市国税局住宅楼2-8-2室的业主，该住宅接受原告的供热。现被告张强未缴纳2013年冬季至2015年春季期间的供热费用5136元（2568元/年×2年。 认定上述事实的证据有：营业执照复印件一份、图政发【2009】13号文件一份。 本院认为，被告居住的住宅楼坐落在原告供热区域内，接受原告供热，因此双方已形成了事实上的供用热力合同关系。被告接受该服务后应履行缴纳供热费用的义务。现被告以供热温度不达标为由拒绝交纳供热费用，但原告予以否认，且被告未提供相关证据佐证，因此被告的抗辩不能成立。故对原告的诉讼请求予以支持。依照《中华人民共和国合同法》第一百八十二条、第一百八十四条、《最高人民法院关于适用的解释》第九十条、《吉林省城市供热条例》第二十条之规定，判决如下： 被告张强于本判决发生法律效力之日立即给付原告图们市城乡建设开发有限公司供热费用5136元。 如果未按本判决指定的期间履行给付金钱义务，应当按照《中华人民共和国民事诉讼法》第二百五十三条之规定，加倍支付迟延履行期间的债务利息。 案件受理费50元，由被告张强负担。 如不服本判决，可在判决书送达之日起十五日内，向本院递交上诉状，并按对方当事人人数提出副本，上诉于延边朝鲜族自治州中级人民法院。 审　判　长　　金　旭 人民陪审员　　崔正植 人民陪审员　　李馥亿 二〇一五年七月二十七日 书记员周雪峰",
    "plaintiff_list" : [
        "图们市城乡建设开发有限公司"
    ],
    "case_type" : "民事案件",
    "is_legal" : 1,
    "all_money" : 5186.0,
    "doc_id" : "a094f6a5-7a6d-4529-85ba-7c94b1eac74e",
    "procedure" : "一审"
}
    print json.dumps(data,ensure_ascii=False,encoding="utf-8")


