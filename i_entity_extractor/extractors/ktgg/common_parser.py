# coding=utf8

from i_entity_extractor.common_parser_lib import toolsutil
import re
import ktgg_conf
import esm
import copy
import os

class CommonParser:
    def __init__(self, parser_tool, log):
        self.parser_tool      = parser_tool
        self.log              = log
        self.court_place_len  = 20
        self.max_court_len    = 20
        self.min_court_len    = 5
        self.min_litigant_len = 2
        self.max_litigant_len = 40
        self.strip_list       = ['\t\r\n','\r\n', '\n\n', '\r','\n']
        self.seps             = ['\r','\n','。','，']
        self.litiants_seps    = [',', ':', '，', '：', '。','、',u'与',u'和',u'及',";","；",'\t',' ']

        self.litigant_regex_list = []
        for litigant_pattern in ktgg_conf.litigant_pattern_list:
            self.litigant_regex_list.append(re.compile(litigant_pattern))

        self.court_place_regex = re.compile(u'在(\S+庭)|(第\S+庭)')
        self.judge_regex       = re.compile(u'[合议庭成员,承办人,审判长]：(\S+)')
        self.court_time_regex  = re.compile(u'\d+年\d+月\d+日.*?\d{1,2}[:：]\d{1,2}|\d+年\d+月\d+日.*?\S+[时点分]|\d+月\d+日.*?\S+[时点分]|\d+月\d+日.*?\d{1,2}:\d{1,2}|\d+年\d+月\d+日|二[〇0O○]\S+年\S+月\S+日\d{1,2}:\d{1,2}|二[〇0O○]\S+年\S+月\S+日\S+[时点分]')
        self.court_regex       = re.compile(u'在(\S+人民法院)')

        self.plaintiff_index = esm.Index()
        for keyword in ktgg_conf.plaintiff_keyword_list:
            self.plaintiff_index.enter(keyword)
        self.plaintiff_index.fix()

        self.defendant_index = esm.Index()
        for keyword in ktgg_conf.defendant_keyword_list:
            self.defendant_index.enter(keyword)
        self.defendant_index.fix()

        self.current_path = os.getcwd()
        self.basic_path = self.current_path[:self.current_path.rfind("i_entity_extractor")]
        self.config_path = self.basic_path + "i_entity_extractor/extractors/ktgg/simple2court_kv.conf"
        self.court_list = open(self.config_path).read().split('\n')
        self.court_kv = {}
        for court in self.court_list:
            tmp_list = court.split(',')
            if len(tmp_list) != 2:
                continue
            self.court_kv[unicode(tmp_list[0])] = unicode(tmp_list[1])


    def before_parser(self, extract_data):
        '''解析预处理'''
        province = extract_data.get('province', '')
        content  = extract_data.get('content', '')
        self.log.info("%sktgg_common_parser" % province)
        extract_data_list = []

        if province in [u'河南']:
            extract_data["content"] = extract_data.get("title","")
            extract_data_list.append(extract_data)
        elif province in [u"海南",u"辽宁"]:
            if unicode(content).find(u'基本案情') != -1:
                content = unicode(content)
                court_time = toolsutil.re_findone(re.compile(u'开庭时间[:：]{0,1}(\S+)'),content)
                if court_time:
                    extract_data["court_time"]  = toolsutil.norm_date_time(court_time)

                court_place = toolsutil.re_findone(re.compile(u'开庭地点[:：]{0,1}(\S+庭)'),content)
                if court_place:
                    extract_data["court_place"] = court_place

                case_id_content = toolsutil.re_findone(re.compile(u'案号[:：]{0,1}(\S+)'), content)
                if case_id_content:
                    case_id = self.parser_tool.caseid_parser.get_case_id(case_id_content)
                    extract_data["case_id"] = case_id

                court = toolsutil.re_findone(re.compile(u'法院[:：]{0,1}(\S+)'), content)
                if court:
                    extract_data["court"] = court

                judge_data = toolsutil.re_findone(re.compile(u'审判长[:：]{0,1}(\S+)|主办人[:：]{1,2}(\S+)'),content)
                judge      = ""
                if judge_data:
                    judge = self.get_value_from_tuple(judge_data)
                if judge:
                    extract_data["judge"] = judge

                plaintiffs_data = toolsutil.re_findone(re.compile(u'原告人[:：]{0,1}(\S+)|公诉人[:：]{1,2}(\S+)|原告[:：]{1,2}(\S+)'), content)
                plaintiffs      = ""
                if plaintiffs_data:
                    plaintiffs      = self.get_value_from_tuple(plaintiffs_data)
                if plaintiffs:
                    extract_data["plaintiffs"]  = plaintiffs

                defendants_data = toolsutil.re_findone(re.compile(u'被告人[:：]{0,1}(\S+)|被告[:：]{1,2}(\S+)'), content)
                defendants      = ""
                if defendants_data:
                    defendants  = self.get_value_from_tuple(defendants_data)
                if defendants:
                    extract_data["defendants"]  = defendants

                content = toolsutil.re_findone(re.compile(u'基本案情[:：]{0,1}(\S+)'),content)
                if content:
                    extract_data["content"] = content
                extract_data_list.append(extract_data)
            else:
                extract_data_list.append(extract_data)
        else:
            extract_data_list.append(extract_data)



        return extract_data_list


    def get_value_from_tuple(self,tuple_data):
        '''获取元组中有值的值'''
        result = ""
        for item in tuple_data:
            if item:
                result = item
                break
        return result

    def get_court(self,content):
        '''获取法院'''
        court = self.parser_tool.court_parser.get_court(content)
        if not court:
            ret = toolsutil.re_findone(self.court_regex, content)
            if ret:
                court = ret
            else:
                court = ""
        return court

    def get_court_place(self,content):
        '''获取开庭地点'''
        content = unicode(content).replace(" ", "")
        content_list = toolsutil.my_split(content, self.seps)
        court_place = self.parser_tool.court_place_parser.get_court_place(content)
        if not court_place:
            for row_content in content_list:
                court_place_list = toolsutil.re_findone(self.court_place_regex, unicode(row_content))
                if court_place_list:
                    for item in court_place_list:
                        if item:
                            court_place = item
                            break
                if court_place and len(court_place) < self.court_place_len:
                    for replace_str in ktgg_conf.court_place_replace_str_list:
                        court_place = court_place.replace(replace_str, '')
                    break
        return court_place


    def get_data_from_litigants(self, extract_data):
        '''抽取数据中无内容，从当事人中抽取原告被告'''
        court = unicode(extract_data.get("court", "").strip())
        court_place = unicode(extract_data.get("court_place", "").strip())
        new_court = self.court_kv.get(court, "")
        if new_court:
            extract_data["court"] = new_court

        court = self.parser_tool.court_parser.get_court(extract_data.get("court",""))
        if not court_place:
            extract_data["court_place"] = self.parser_tool.court_place_parser.get_court_place(extract_data.get("court", ""))
        if court:
            extract_data["court"] = court

        plaintiffs = extract_data.get("plaintiffs","")
        defendants = extract_data.get("defendants", "")
        litigants  = extract_data.get("litigants", "")
        if litigants == "":
            litigants  = plaintiffs + ',' + defendants
            if litigants.startswith(',') or litigants.endswith(','):
                litigants = ""
        if litigants:
            litigants = unicode(litigants)

            for replace_str in ktgg_conf.replace_str_list:
                litigants = litigants.replace(replace_str,"")

            tmp_list = toolsutil.my_split(litigants,[';','；','\r\n','\t','\n','\r'])
            if len(tmp_list) == 2:
                defendants_ret = self.defendant_index.query(tmp_list[0])
                plaintiffs_ret = self.plaintiff_index.query(tmp_list[0])
                if defendants_ret:
                    defendants = tmp_list[0]
                    plaintiffs = tmp_list[1]
                else:
                    if plaintiffs_ret:
                        defendants = tmp_list[1]
                        plaintiffs = tmp_list[0]
        else:
            plaintiff_list = extract_data.get("plaintiff_list", "")
            defendant_list = extract_data.get("defendant_list", "")
            litigant_list  = extract_data.get("litigant_list", "")

            if isinstance(plaintiff_list, basestring):
                plaintiff_list = toolsutil.my_split(extract_data.get("plaintiff_list", ""),self.litiants_seps)
                defendant_list = toolsutil.my_split(extract_data.get("defendant_list", ""),self.litiants_seps)
                litigant_list  = toolsutil.my_split(extract_data.get("litigant_list", ""),self.litiants_seps)
            plaintiffs = ','.join(plaintiff_list)
            defendants = ','.join(defendant_list)
            litigants  = ','.join(litigant_list)


        litigant_list, litigants, plaintiff_list, defendant_list = self.format_litigants(plaintiffs,defendants,litigants)
        return self.get_entity_data(extract_data, litigant_list, litigants, plaintiff_list, defendant_list)

    def get_data_from_content(self,extract_data):
        '''抽取数据中有内容'''
        src_content  = extract_data.get("content")
        content      = unicode(src_content).replace(" ","")
        content_list = toolsutil.my_split(content,self.seps)
        content      = self.norm_content(content)
        #1 解析案由
        case_cause_list = []
        if not extract_data.has_key("case_cause"):
            case_cause_list = self.parser_tool.case_cause_parser.get_case_causes(content)
            case_cause      = ','.join(case_cause_list)
            extract_data["case_cause"] = case_cause
        else:
            extract_data["case_cause"] = extract_data["case_cause"].replace(u"一案","")
            case_cause_list.append(extract_data["case_cause"])
        #2 解析案号
        if not extract_data.has_key("case_id"):
            case_id = self.parser_tool.caseid_parser.get_case_id(content)
            extract_data["case_id"] = case_id
        #3 解析法院
        if not extract_data.has_key("court"):
            extract_data["court"] = self.get_court(content)
        else:
            court_list = toolsutil.my_split(extract_data.get("court",""),['：',':'])
            if len(court_list) > 0:
                extract_data["court"] = court_list[-1]
        #4 解析法官
        if not extract_data.has_key("judge"):
            judge = ""
            src_content = unicode(src_content)
            tmp_content_list = toolsutil.my_split(src_content,['\r\n','\r','\n'])
            for row_content in tmp_content_list:
                judge = toolsutil.re_findone(self.judge_regex, unicode(row_content))
                if judge:
                    judge_list = toolsutil.my_split(judge,[' ','，','：'])
                    judge      = ','.join(judge_list)
                    break
                else:
                    judge = ""
            extract_data["judge"] = judge
        #5 解析开庭时间
        if not extract_data.has_key("court_time"):
            for row_content in content_list:
                court_time = toolsutil.re_findone(self.court_time_regex, unicode(row_content))
                if court_time:
                    for week in ktgg_conf.week_day_list:
                        court_time = court_time.replace(week, " ")
                else:
                    court_time = ""
                extract_data["court_time"] = court_time
                break

        #6 解析开庭地点
        if not extract_data.has_key("court_place"):
            extract_data["court_place"] = self.get_court_place(content)

        #7 解析当事人／原告／被告
        #---获取当事人内容
        litigants = plaintiffs = defendants = ''
        find_flag = False
        src_content = src_content.replace(" ","")
        tmp_content_list = toolsutil.my_split(src_content,['\r','\n','。'])
        for litigant_regex in self.litigant_regex_list:
            for row_content in tmp_content_list:
                litigants = toolsutil.re_findone(litigant_regex, unicode(row_content))
                if litigants:
                    find_flag = True
                    break
            if find_flag:
                break
            else:
                litigants = ""

        if litigants == "":
            litigants = content

        #---通过当事人内容获取原告被告
        plaintiffs, defendants, litigants = self.get_plaintiff_defendant(litigants, extract_data.get("case_id", ""), extract_data.get("court", ""), case_cause_list)
        if litigants:
            litigant_list, litigants, plaintiff_list, defendant_list = self.format_litigants(plaintiffs, defendants, litigants)
        else:
            litigant_list = plaintiff_list = defendant_list = []
        for item in self.strip_list:
            extract_data['content'] = extract_data.get('content').replace(item, ' ')
        return self.get_entity_data(extract_data, litigant_list, litigants, plaintiff_list, defendant_list)

    def get_plaintiff_defendant(self,litigants_content, case_id, court, case_cause_list):
        '''获取原告和被告'''

        #1 格式化当事人内容
        for cause in case_cause_list:
            litigants_content = litigants_content.replace(cause, '')
        case_id = case_id.replace('(','（').replace(')','）')
        litigants_content = litigants_content.replace('(','（').replace(')','）')

        replace_court         = court if court else ''
        litigants_content     = litigants_content.replace(case_id, '').replace(replace_court, '')
        src_content           = litigants_content
        for replace_str in ktgg_conf.replace_str_list:
            src_content = src_content.replace(replace_str, "")
        plaintiffs,defendants,litigants = self.get_plaintiff_defendant_bykeyword(src_content)


        #2 按照上诉／诉等关键字分割原告和被告
        if plaintiffs == "" and defendants == "":
            tmp_list  = toolsutil.my_split(litigants_content,ktgg_conf.litigants_sep_list)
            if len(tmp_list) == 2:
                plaintiffs = tmp_list[0]
                defendants = tmp_list[1]
                litigants  = ','.join(tmp_list)
            else:
                litigants = litigants_content

        return plaintiffs, defendants, litigants

    def get_plaintiff_defendant_bykeyword(self, litigants_content):
        #1 按照原告被告关键字分割原告被告

        defendants = plaintiffs = litigants = ""

        src_content = litigants_content

        for replace_str in ktgg_conf.defendant_keyword_list:
            litigants_content = litigants_content.replace(replace_str,"")

        for replace_str in ktgg_conf.plaintiff_keyword_list:
            litigants_content = litigants_content.replace(replace_str,"")

        litigants_list = toolsutil.my_split(litigants_content,self.litiants_seps)
        litigants      = ','.join(litigants_list)

        litigants_content = toolsutil.utf8_encode(src_content)
        plaintiff_ret     = self.plaintiff_index.query(litigants_content)
        defendant_ret     = self.defendant_index.query(litigants_content)

        if defendant_ret or plaintiff_ret:
            plaintiff_list = []
            defendant_list = []
            for litigant in litigants_list:
                litigant = unicode(litigant)
                if plaintiff_ret:
                    plaintiffs_key = unicode(plaintiff_ret[0][1])
                    tmp = plaintiffs_key + litigant
                    if tmp in src_content:
                        plaintiff_list.append(litigant)
                if defendant_ret:
                    defendants_key = unicode(defendant_ret[0][1])
                    tmp = defendants_key + litigant
                    if tmp in src_content:
                        defendant_list.append(litigant)
            plaintiffs = ','.join(plaintiff_list)
            defendants = ','.join(defendant_list)

        return plaintiffs,defendants,litigants

    def format_litigants(self, plaintiffs, defendants, litigants):
        '''格式化当事人，被告，原告'''
        for replace_str in ktgg_conf.defendant_keyword_list:
            litigants  = litigants.replace(replace_str, "")
            plaintiffs = plaintiffs.replace(replace_str, "")
            defendants = defendants.replace(replace_str, "")

        for replace_str in ktgg_conf.plaintiff_keyword_list:
            litigants  = litigants.replace(replace_str, "")
            plaintiffs = plaintiffs.replace(replace_str, "")
            defendants = defendants.replace(replace_str, "")

        replace_str_list = self.strip_list + ktgg_conf.replace_str_list + ktgg_conf.format_str_list
        for replace_str in replace_str_list:
            replace_str = unicode(replace_str)
            litigants   = litigants.replace(replace_str,"")
            plaintiffs  = plaintiffs.replace(replace_str,"")
            defendants  = defendants.replace(replace_str,"")

        plaintiff_list = toolsutil.my_split(plaintiffs, self.litiants_seps)
        defendant_list = toolsutil.my_split(defendants, self.litiants_seps)
        litigant_list  = toolsutil.my_split(litigants, self.litiants_seps)

        defendant_list = [x for x in defendant_list if x and len(unicode(x)) >= self.min_litigant_len and len(unicode(x)) <= self.max_litigant_len]
        plaintiff_list = [x for x in plaintiff_list if x and x not in defendant_list and len(unicode(x)) >= self.min_litigant_len and len(unicode(x)) <= self.max_litigant_len]
        litigant_list  = [x for x in litigant_list if x and len(unicode(x)) >= self.min_litigant_len and len(unicode(x)) <= self.max_litigant_len]

        if litigant_list == []:
            if plaintiff_list:
                litigant_list = litigant_list + plaintiff_list
            if defendant_list:
                litigant_list = litigant_list + defendant_list

        litigant_list = sorted(litigant_list)
        litigants     = ",".join(litigant_list)

        return litigant_list, litigants, plaintiff_list, defendant_list

    def get_entity_data(self, extract_data, litigant_list, litigants, plaintiff_list, defendant_list):
        '''组装实体解析数据'''
        entity_data = copy.deepcopy(extract_data)
        entity_data["litigants"]      = litigants
        entity_data["litigant_list"]  = litigant_list
        entity_data["plaintiff_list"] = plaintiff_list
        entity_data["defendant_list"] = defendant_list
        for key, value in entity_data.items():
            if isinstance(value, basestring):
                entity_data[key] = toolsutil.my_strip(unicode(value), self.strip_list)
        return entity_data

    def norm_content(self, content):
        '''格式化内容'''
        content = unicode(content)
        for item in self.strip_list:
            content = content.replace(item, '')
        return content
