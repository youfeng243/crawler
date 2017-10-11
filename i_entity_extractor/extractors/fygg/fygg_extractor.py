# coding=utf-8
# 法院公告实体解析

import json
import sys
import traceback
sys.path.append('../')
sys.path.append('../../')

import copy
from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
from i_entity_extractor.common_parser_lib import toolsutil


class FyggExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.litiants_seps = [',', ':', '，', '：', '。', '、', ";", "；", '\t']

    def format_extract_data(self, extract_data, topic_id):
        '''实体解析入口'''
        entity_data = copy.deepcopy(extract_data)
        province = None
        if entity_data.has_key("court"):
            court    = extract_data.get('court')
            province = self.parser_tool.province_parser.get_province(court)

        defendants = extract_data.get("defendants","")
        plaintiffs = extract_data.get("plaintiffs","")
        defendant_list = []
        plaintiff_list = []

        if defendants and plaintiffs:
            plaintiff_list = toolsutil.my_split(plaintiffs,self.litiants_seps)
            defendant_list = toolsutil.my_split(defendants, self.litiants_seps)


        info = {}
        if entity_data.has_key("bulletin_content"):
            content        = extract_data.get('bulletin_content')
            case_cause     = self.parser_tool.case_cause_parser.get_case_cause(content)
            case_id        = self.parser_tool.caseid_parser.get_case_id(content)
            info           = self.parser_tool.fygg_parser.do_parser(content)
            info["case_cause"] = case_cause
            info["case_id"]    = case_id

        if defendant_list == [] and plaintiff_list == []:
            defendant_list = info.get("defendant_list",[])
            plaintiff_list = info.get("plaintiff_list", [])

        litigant_list = list(set(plaintiff_list + defendant_list))
        litigants     = ','.join(litigant_list)

        entity_data["norm_bulletin_content"] = info.get("norm_content")
        entity_data["case_id"]        = info.get("case_id")
        entity_data["case_cause"]     = info.get("case_cause")
        entity_data["entity_list"]    = info.get("entities")
        entity_data["plaintiff_list"] = plaintiff_list
        entity_data["defendant_list"] = defendant_list
        entity_data["bulletin_type"]  = info.get("bulletin_type")
        entity_data["litigant_list"]  = litigant_list
        entity_data["litigants"]      = litigants

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

    topic_id = 33
    from i_entity_extractor.entity_extractor_route import EntityExtractorRoute
    from i_entity_extractor.common_parser_lib.mongo import MongDb
    import json

    route = EntityExtractorRoute(conf)
    topic_info = route.all_topics.get(topic_id, None)
    begin_time = time.time()
    obj = FyggExtractor(topic_info, log)


    extract_data = {
    "province" : "四川",
    "defendant_list" : [
        "叙永嘉年华城市建设发展有限公司"
    ],
    "court" : "叙永县人民法院",
    "entity_list" : [],
    "url" : "http://rmfygg.court.gov.cn/bulletin/court/2016/08/03/4812065.html",
    "plaintiff_list" : [
        "重庆金典融资担保有限公司"
    ],
    "norm_bulletin_content" : "叙永嘉年华城市建设发展有限公司,本院根据重庆金典融资担保有限公司的申请于2016年6月28日裁定受理叙永嘉年华城市建设发展有限公司破产清算一案，并于2016年6月28日指定四川长江会计师事务所有限公司为叙永嘉年华城市建设发展有限公司管理人。叙永嘉年华城市建设发展有限公司的债权人应自公告之日起60日内，向叙永嘉年华城市建设发展有限公司管理人（通讯地址：四川省叙永县叙永镇环城中路原盛世华都售房部；邮政编码：646400；联系电话：（0830）6224615、（0830）3119206、13018159367）申报债权。逾期申报者，可以在破产财产最后分配前补充申报，但对此前已进行的分配无权要求补充分配，同时要承担为审查和确认补充申报债权所产生的费用。叙永嘉年华城市建设发展有限公司的债务人或者财产持有人应尽快向叙永嘉年华城市建设发展有限公司管理人清偿债务或交付财产。第一次债权人会议将于2016年10月15日上午9时在叙永县叙永镇二环路鱼凫学术报告厅召开，请各债权人准时参加。出席会议的债权人系法人或其他组织的，应提交营业执照、法定代表人或负责人身份证明书，如委托代理人出席会议，应提交特别授权委托书、委托代理人的身份证件或律师执业证，委托代理人是律师的还应提交律师事务所的指派函。债权人系自然人的，应提交个人身份证明。委托代理人出席会议，应提交特别授权委托书、委托代理人的身份证件或律师执业证，委托代理人是律师的还应提交律师事务所的指派函。,[四川]叙永县人民法院",
    "litigants" : "叙永嘉年华城市建设发展有限公司,重庆金典融资担保有限公司",
    "_in_time" : "2016-10-20 15:12:54",
    "_src" : [
        {
            "url" : "http://rmfygg.court.gov.cn/bulletin/court/2016/08/03/4812065.html",
        }
    ],
    "bulletin_date" : "2016-08-03 00:00:00",
    "case_id" : "（0830）6224615、（0830",
    "bulletin_type" : "破产文书",
    "_record_id" : "eacb0ead4cce110a1de44dda98d77e39",
    "pdf" : "http://rmfygg.court.gov.cn/psca/lgnot/bulletin/download/4812065.pdf",
    "litigant_list" : [
        "叙永嘉年华城市建设发展有限公司",
        "重庆金典融资担保有限公司"
    ],
    "_utime" : "2016-10-20 15:18:37",
    "case_cause" : "破产清算"
}

    entity_data = obj.format_extract_data(extract_data,topic_id)
    entity_data = obj.after_process(entity_data)
    print json.dumps(entity_data,ensure_ascii=False,encoding='utf8')
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
