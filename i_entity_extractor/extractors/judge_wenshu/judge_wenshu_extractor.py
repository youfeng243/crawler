# coding=utf8
import sys
import json
import time
sys.path.append('../../')
from i_entity_extractor.extractors.default.default_extractor import DefaultExtractor
import copy


class JudgeWenshuExtractor(DefaultExtractor):
    def __init__(self, topic_info, log):
        DefaultExtractor.__init__(self, topic_info, log)
        self.case_type_map = {"1": u"刑事案件", "2": u"民事案件", "3": u"行政案件", "4": u"赔偿案件", "5": u"执行案件"}
        self.case_id_type_map = {u"刑": u"刑事案件", u"民": u"民事案件",u"商": u"民事案件", u"行": u"行政案件", u"赔": u"赔偿案件", u"执": u"执行案件", }

    def format_extract_data(self, extract_data, topic_id):
        '''实体解析抽取数据'''
        entity_data   = copy.deepcopy(extract_data)
        case_date     = extract_data.get("case_date")
        doc_content   = extract_data.get('doc_content')
        litigants     = extract_data.get('litigants')
        court         = extract_data.get("court")
        province      = extract_data.get("province")
        case_cause    = extract_data.get("case_cause")
        case_type     = extract_data.get("case_type")
        case_id       = extract_data.get("case_id")
        procedure     = extract_data.get("procedure")
        case_name     = extract_data.get("case_name")



        # 详情页处理
        tmp_entity_data = {}
        company_list = None
        ref_ids      = None
        if doc_content and u'文档内容为空' not in unicode(doc_content):
            tmp_entity_data = self.parser_tool.wenshu_parser.do_parser(doc_content, litigants)
            if case_id == None:
                case_id = tmp_entity_data.get('case_id', '')
            ref_ids = tmp_entity_data.get('ref_ids', [])
            if case_id and case_id not in ref_ids:
                ref_ids.append(case_id)

            if case_cause == None:
                case_cause = self.get_case_cause(doc_content)

        #company_list = self.parser_tool.company_parser.get_company_list(doc_content, wenshu_conf.company_end_list)
        if case_date == None:
            case_date = tmp_entity_data.get("case_date")
        if court == None:
            court = tmp_entity_data.get("court")
        if court != None and  province == None:
            province = self.parser_tool.province_parser.get_province(court)

        if case_type != None:
            if self.case_type_map.has_key(str(case_type)):
                case_type = self.case_type_map[str(case_type)]
            else:
                case_type = str(case_type).replace(' ', '')
        else:
            if case_id != None:
                for key, value in self.case_id_type_map.items():
                    if key in case_id:
                        case_type = value

        litigants = tmp_entity_data.get("litigants")

        if procedure == None:
            case_name = case_name if case_name else ""
            procedure = self.get_procedure_from_title(case_name)
            if procedure == "":
                procedure = tmp_entity_data.get("procedure")


        entity_data["max_money"]          = tmp_entity_data.get("max_money")
        entity_data["doc_content"]        = doc_content
        entity_data["ref_ids"]            = ref_ids
        entity_data["all_money"]          = tmp_entity_data.get("all_money")
        entity_data["company_list"]       = company_list
        entity_data["judge_content"]      = tmp_entity_data.get("judge_content")
        entity_data["case_cause"]         = case_cause
        entity_data["province"]           = province
        entity_data["plaintiff_list"]     = tmp_entity_data.get("plaintiff_list")
        entity_data["defendant_list"]     = tmp_entity_data.get("defendant_list")
        entity_data["litigants"]          = litigants
        entity_data["litigant_list"]      = tmp_entity_data.get("litigant_list")
        entity_data["litigant_info_list"] = tmp_entity_data.get("litigant_info_list")
        entity_data["judiciary_list"]     = tmp_entity_data.get("judiciary_list")
        entity_data["chain_case_id"]      = tmp_entity_data.get("chain_case_id")
        entity_data["court"]              = court
        entity_data["case_type"]          = case_type
        entity_data["procedure"]          = procedure
        entity_data["case_id"]            = case_id
        entity_data["case_date"]          = case_date
        entity_data["case_name"]          = case_name

        return entity_data

    def get_case_cause(self, content):
        '''获取案由'''
        case_cause = self.parser_tool.case_cause_parser.get_case_causes(content)
        if len(case_cause) > 0:
            return case_cause[0]
        else:
            return ''

    def get_procedure_from_title(self,title):
        '''从标题中获取审判程序'''
        title = unicode(title)
        if title.find(u'一审') != -1:
            procedure = u'一审'
        elif title.find(u'二审') != -1:
            procedure = u'二审'
        elif title.find(u'再审') != -1:
            procedure = u'再审'
        elif title.find(u'刑罚变更') != -1 or title.find(u'减刑')!= -1:
            procedure = u'刑罚变更'
        else:
            procedure = u""
        return procedure





if __name__ == '__main__':
    import pytoml
    import sys

    sys.path.append('../../')
    from i_entity_extractor.conf import get_config
    from bdp.i_crawler.i_extractor.ttypes import BaseInfo, CrawlInfo, ExtractInfo, PageParseInfo

    with open('../../entity.toml', 'rb') as config:
        config = pytoml.load(config)
    conf = get_config(config)

    topic_id = 32
    from i_entity_extractor.entity_extractor_route import EntityExtractorRoute
    from i_entity_extractor.common_parser_lib.mongo import MongDb
    import json
    from i_entity_extractor.common_old import log

    route = EntityExtractorRoute(conf)
    topic_info = route.all_topics.get(topic_id, None)
    begin_time = time.time()
    obj = JudgeWenshuExtractor(topic_info, log)

    mongo_conf = {
        'host': '172.16.215.16',
        'port': 40042,
        'final_db': 'app_data',
        'username': "work",
        'password': "haizhi",
    }
    db = MongDb(mongo_conf['host'], mongo_conf['port'], mongo_conf['final_db'],
                mongo_conf['username'],
                mongo_conf['password'])

    cursor = db.db["judgement_wenshu"].find({})
    for item in cursor:
        item.pop("_id")
        print json.dumps(item,encoding='utf8',ensure_ascii=False)
        break
    num = 0

    extract_data = {
    "_in_time": "2017-04-12 14:25:38.652455",
    "_site_record_id": "b371fa10e638d52856e3764541643ab3",
    "_src": [
        {
            "site": "www.caseshare.cn",
            "site_id": -7955487906553582000,
            "url": "http://www.caseshare.cn/full/117705424.html"
        }
    ],
    "_utime": "2017-04-12 14:25:38.652455",
    "bulletin_date": "2010-01-20",
    "case_id": "(2009)召民一初字第606号",
    "case_name": "张秀争等诉杨光俊等生命权、健康权纠纷案",
    "court": "河南省漯河市召陵区人民法院",
    "doc_content": "原告张秀争。\t委托代理人张华。\t原告张某某。\t原告张珍。\t原告朱风厂。\t原告冀兰。\t以上五原告共同委托代理人娄梦，河南恩达律师事务所律师。\t被告杨光俊。\t被告朱红。\t被告霍中杰。\t委托代理人翟明伟，河南强人律师事务所律师。\t原告张秀争等五人诉被告杨光俊、朱红、霍中杰生命权、健康权纠纷一案，本院立案受理后，依法组成合议庭，对本案公开开庭进行了审理。原告张秀争及五原告共同委托代理人娄梦、被告杨光俊、被告朱红、被告霍中杰的委托代理人翟明伟到庭参加诉讼。本案现已审理终结。\t五原告诉称：2003年3月，朱秀华被诊断为糖尿病患者。遵医瞩按时服药、注射胰岛素。身体基本健康，每年例行检查。2009年4月18日，朱秀华因糖尿病住院治疗，4月25日治愈出院。出院当天，被告朱红要朱秀华到她家住一段时间，朱红把朱秀华接到她家中，宣称完美产品可以治好朱秀华的糖尿病，开始让朱秀华服用完美产品，并逐渐减少注射胰岛素，后来停止为朱秀华注射胰岛素。2009年8月7日，朱秀华的糖尿病复发，被告朱红把她送到漯河市第五人民医院治疗，经抢救无效死亡。被告杨光俊、霍中杰系朱红的指导老师。朱红的行为是在其指导老师的唆使和指导下进行的。三被告虚假宣传完美产品的功效，并减少、直到停止让朱秀华注射胰岛素是导致朱秀华死亡的直接原因，其对朱秀华死亡应当承担法律责任。要求法院判令三被告赔偿原告方各项损失共计211488元，并承担本案诉讼费用。\t被告杨光俊辩称：朱红从事完美的工作已三年了，事情是意外，朱红的业务不精通，该事与我无关。\t被告朱红辩称：我干的时间很短，业务并不精通。我问杨光俊能不能治疗糖尿病，杨光俊承诺可以。后由霍中杰配药，杨光俊指导，让朱秀华出院接受杨光俊指导用完美产品治病，并降低注射胰岛素，最后停止注射胰岛素。造成朱秀华死亡，应由杨光俊、霍中杰承担赔偿责任。\t被告霍中杰辩称：霍中杰既不是完美产品的生产者也不是销售者，也不存在什么所谓的唆使或指导行为，霍中杰与本案无任何关系，原告的起诉没有任何事实根据与法律依据，法院应驳回原告对被告霍中杰的起诉或诉讼请求。\t经审理查明：朱秀华生前系糖尿病患者，经医院治疗后病情稳定，后出院回家休养，医瞩要求其按时注射胰岛素。朱秀华出院后被朱红接到家中居住，朱红宣称食用“完美”产品可以治愈糖尿病。朱秀华听信朱红的说法，开始服用“完美”产品，并逐步减少直至停止注射胰岛素。2009年8月7日，朱秀华糖尿病复发，被送至漯河市第五人民医院抢救，后抢救无效，于2009年8月8日死亡。\t另查明：被告杨光俊系朱红的“完美”产品指导老师，在对朱红进行指导时，存在虚假宣传、片面夸大“完美”产品功效的行为。让朱秀华停止注射胰岛素，也是朱红在杨光俊的指导下进行的。\t本院认为：朱红宣称食用“完美”产品可以治疗糖尿病，杨光俊作为朱红的指导老师，指导朱红时存在虚假宣传、片面夸大“完美”产品功效的行为，朱秀华听信朱红说法，减少直至停止注射胰岛素，致使朱秀华糖尿病复发，抢救无效死亡。杨光俊、朱红的行为与朱秀华的死亡后果之间存在因果关系，应当承担相应的民事责任。朱秀华系成年人，减少直至停止注射“胰岛素”，致损害结果发生，其自身也应负一定责任。综合案情，本院酌定杨光俊、朱红二人承担60%的责任为宜，因杨光俊、朱红的行为属共同行为，故二人应对因朱秀华死亡所产生的60％的赔偿责任互负连带责任。关于原告方可获得赔偿的范围与数额为：1.死亡赔偿金，为89080元（4454元×20年）。2.丧葬费，按在岗职工6个月的工资计算，为12408（24816元÷12个月×6个月）。3.精神抚慰金，原告方要求80000元过高，本院酌定60000元为宜。4.被告扶养人生活费，被扶养人为：张某某，生于1996年8月16日，其生活费为15520元（3044元×5年）；朱风厂，生于1933年4月20日，其生活费为15520元（3044元×5年）；冀兰，生于1934年12月8日，其生活费为15520元（3044元×5年）。原告方要求的其他费用，因未能提供相应证据，本院不予支持。以上各项共计207148元，被告杨光俊、朱红应承担60%的赔偿责任，为124288.8元。原告方要求被告霍中杰也承担相应的责任，因缺乏相应的证据，对原告的此项诉讼请求，本院不予支持。根据《中华人民共和国民法通则》第一百零六条、第一百一十九条、《最高人民法院关于审理人身损害赔偿案件适用法律若干问题的解释》第四条、第十七条之规定，判决如下：\t一、被告杨光俊、朱红于判决生效后五日内赔偿原告张秀争、张某某、张珍、朱风厂、冀兰各项损失共计124288.8元。\t二、被告杨光俊、朱红互负连带责任。\t如果未按本判决指定的期间履行给付金钱义务，应当按照《中华人民共和国民事诉讼法》第二百二十九条之规定，加倍支付迟延履行期间的债务利息。\t三、驳回原告张秀争、张某某、张珍、朱风厂、冀兰的其他诉讼请求。\t本案诉讼费4530元，由被告杨光俊、朱红承担。\t如不服本判决，可在判决书送达之日起十五日内，向本院递交上诉状及副本一式十份，上诉于河南省漯河市中级人民法院。\t\t审判长王卫东\t审判员常丽\t审判员刘杰\t二○一○年元月二十日\t书记员董文君",
    "doc_id": "http://www.caseshare.cn/full/117705424.html"
}

    entity_data = obj.format_extract_data(extract_data,topic_id)
    entity_data = obj.after_process(entity_data)
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


    print "time_cost:",time.time() - begin_time
