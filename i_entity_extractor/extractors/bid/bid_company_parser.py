# coding=utf8
import sys
import time
import traceback

sys.path.append("..")

from i_entity_extractor.common_parser_lib import toolsutil
import esm
import bid_conf



class Bid_company_parser:
    def __init__(self, company_parser):
        self.company_length_limit = 25  # unicode
        self.zhongbiao_offset = 100  # utf8
        self.zhaobiao_offset = 100  # uft8
        self.agent_offset = 100  # utf8
        self.zhaobiao_company_index = esm.Index()
        self.agent_company_index = esm.Index()
        self.zhongbiao_company_index = esm.Index()
        self.bid_type_zhongbiao_index = esm.Index()
        self.company_parser = company_parser
        self.houxuanzhongbiao_company_index = esm.Index()

        self.init_index(self.zhaobiao_company_index, bid_conf.zhaobiao_company_keyword_list)
        self.init_index(self.agent_company_index, bid_conf.agent_keyword_list)
        self.init_index(self.zhongbiao_company_index, bid_conf.zhongbiao_company_keyword_list)
        self.init_index(self.houxuanzhongbiao_company_index, bid_conf.houxuanzhongbiao_company_keyword_list)

    def init_index(self, esm_index, company_keyword_list):
        for keyword in company_keyword_list:
            esm_index.enter(keyword)
        esm_index.fix()

    def do_parser(self, content, title):
        '''获取公司'''

        # 1 根据关键字查找公司名

        houxuanzhongbiao_company_list = self.get_company_list(content, self.houxuanzhongbiao_company_index,
                                                              bid_conf.zhongbiao_company_end_list,
                                                              self.zhongbiao_offset)
        zhongbiao_company_list = self.get_company_list(content, self.zhongbiao_company_index,
                                                       bid_conf.zhongbiao_company_end_list, self.zhongbiao_offset)

        zhaobiao_company_list = self.get_company_list(content, self.zhaobiao_company_index,
                                                      bid_conf.zhaobiao_company_end_list, self.zhaobiao_offset)
        agent_company_list = self.get_company_list(content, self.agent_company_index, bid_conf.agent_company_end_list,
                                                   self.agent_offset)

        zhaobiao_company = None
        if zhaobiao_company_list:
            if len(zhaobiao_company_list) >= 2:
                find_flag = False
                for item in zhaobiao_company_list:
                    if u'公司' not in unicode(item):
                        zhaobiao_company = item
                        find_flag = True
                        break
                if not find_flag:
                    zhaobiao_company = zhaobiao_company_list[0]
            else:
                zhaobiao_company = zhaobiao_company_list[0]

        agent = ''
        if agent_company_list:
            agent = agent_company_list[0]

        # 2 若招标或者代理公司未找到，则匹配(\S*)受(\S+)委托
        content = unicode(content)
        ret = toolsutil.re_find_one(u'(\S*)受(\S+)委托', content)
        if ret:
            tmp_agent = self.company_parser.get_one_company(ret[0], bid_conf.agent_company_end_list)
            tmp_zhaobiao_company = self.company_parser.get_one_company(ret[1], bid_conf.zhaobiao_company_end_list)
            agent = tmp_agent if tmp_agent else agent
            zhaobiao_company = tmp_zhaobiao_company if tmp_zhaobiao_company else zhaobiao_company

        # 3 若招标公司还未找到，则从标题中寻找
        if not zhaobiao_company:
            zhaobiao_company = self.company_parser.get_one_company(title, bid_conf.zhaobiao_company_end_list)

        result_data = self.filter_company_list(zhaobiao_company, zhongbiao_company_list, houxuanzhongbiao_company_list,
                                               agent)

        result_data["zhaobiao"] = unicode(result_data["zhaobiao"])
        result_data["agent"] = unicode(result_data["agent"])

        for i in range(len(result_data["zhongbiao"])):
            result_data["zhongbiao"][i] = unicode(result_data["zhongbiao"][i])

        for i in range(len(result_data["houxuan_zhongbiao"])):
            result_data["houxuan_zhongbiao"][i] = unicode(result_data["houxuan_zhongbiao"][i])

        return result_data

    def get_company_list(self, content, esm_index, end_list, offset):
        '''获取包含招标中标公司列表'''
        company_list = []
        if not content:
            return []

        content = content.replace(' ', '')
        content = unicode(content).replace('\t', ' ').replace(' ', '')
        content = content.encode('utf-8')

        ret_list = esm_index.query(content)
        if ret_list:
            for ret in ret_list:
                pos = ret[0][0]
                result_content = content[pos:pos + offset]
                company = self.company_parser.get_one_company(result_content, end_list)
                if company:
                    company_list.append(company)

        company_list = list(set(company_list))

        return company_list

    def filter_company_list(self, zhaobiao_company, zhongbiao_company_list, houxuanzhongbiao_company_list, agent):
        '''格式化中标公司'''
        houxuanzhongbiao_company_list = [x for x in houxuanzhongbiao_company_list if x not in zhongbiao_company_list]
        if agent and zhaobiao_company and agent == zhaobiao_company:
            result_data = {
                "zhaobiao": zhaobiao_company,
                "zhongbiao": zhongbiao_company_list,
                "houxuan_zhongbiao": houxuanzhongbiao_company_list,
                "agent": agent
            }
            return result_data

        zhongbiao_company_list = [x for x in zhongbiao_company_list if x not in [zhaobiao_company] and x not in [agent]]
        houxuanzhongbiao_company_list = [x for x in houxuanzhongbiao_company_list if
                                         x not in [zhaobiao_company] and x not in [agent]]

        result_data = {
            "zhaobiao": zhaobiao_company,
            "zhongbiao": zhongbiao_company_list,
            "houxuan_zhongbiao": houxuanzhongbiao_company_list,
            "agent": agent
        }

        return result_data

    def bid_type_parser(self, title):
        '''标类型解析'''
        bid_type = u'招标'

        for keyword in bid_conf.bid_type_keyword_list:
            if keyword[0] in title:
                bid_type = unicode(keyword[1])
                break

        return bid_type


if __name__ == "__main__":
    from i_entity_extractor.common_parser_lib.company_parser import CompanyParser
    from pymongo import MongoClient
    litigants = ""
    sys.path.append('../../')
    company_parser = CompanyParser('../../dict/company_pre.conf')

    obj = Bid_company_parser(company_parser)
    print obj.bid_type_parser("广西贺州至巴马高速公路（钟山至昭平段）设计施工总承包招标评标结果公示")

    content = ' 1. 谈判条件   本谈判项目林州市文化广播电视新闻出版局LED显示屏、舞台灯光、音响、座椅采购项目，采购人为林州市文化广播电视新闻出版局，谈判代理机构为河南联达工程管理有限公司。项目已具备谈判条件，现对该项目竞争性谈判方式采购。      2. 项目概况与谈判范围   2.1项目名称：林州市文化广播电视新闻出版局LED显示屏、舞台灯光、音响、座椅采购项目   2.2采购编号：LCG2015-24-017   2.3谈判范围：第一标段：LED显示屏、舞台灯光、音响等 02 02 第二标段：座椅      3. 供应商资格要求   3.1具备生产或销售本次招标项目内容的生产厂家或经销商，有完成本次招标项目的技术力量、经济实力和完善的售后服务体系；   3.2投标人必须符合《中华人民共和国政府采购法》第二十二条规定；          3.3投标人报名前须在林州市公共资源交易中心备案，相关备案资料查阅林州市公共资源交易中心网站；参加本项目的法定代表人、委托代理人应在林州市公共资源交易中心备案，否则投标无效；   3.4本次投标不接受联合体投标。      4. 谈判文件的获取   4.1凡有意参加投标者，请于 2015年4月1日至 2015年4月3日17时30分前凭企业用户名密码登陆《林州市公共资源交易中心网》下载谈判文件；其他渠道获取的谈判文件的，其投标文件将被拒绝。   4.2 谈判文件每套售价100 元/标段，提交投标文件时交纳，售后不退。供应商须在投标文件递交时缴纳上述费用，否则招标人将拒绝接受其投标文件；      5. 投标文件的递交   5.1 投标文件递交的截止时间（投标截止时间，下同）为 2015 年 4 月 7 日 9 时 00 分，地点为林州市公共资源交易中心二楼开标厅。   5.2 逾期送达的或者未送达指定地点的投标文件，采购人不予受理。      6. 发布公告的媒介   本次谈判公告同时在《中国采购与招标网》、《河南招标采购综合网》、《河南省政府采购网》、《林州市公共资源交易中心网》上发布。      7.其他   7.1、供应商在林州市公共资源交易中心备案后，方可登陆网站获取采购文件（备案流程详见林州市公共资源交易中心，网站“中心公告”栏目中《关于企业备案的通知》）。   7.2、供应商获取文件后，如对采购文件内容有质疑应在提交疑问截止时间前以不记名形式上传至林州市公共资源交易中心网站。   7.3、本次谈判项目如有变更或延期，供应商均可在林州市公共资源交易中心网站直接下载补充文件，供应商应随时关注本中心网站，如有遗漏，后果自负。   7.4、评审时由谈判小组对供应商的资格证明材料进行资格审核，不符合项目资格条件的供应商的报价文件将被拒绝，供应商应自负风险费用，提供虚假材料的将进一步追究其责任。   7.5、采购文件未载明的相关事项必须遵守相关法律法规及规定。   7.6、本次谈判小组推荐的中标意见，将按规定时间在《中国采购与招标网》、《河南招标采购综合网》、《河南省政府采购网》、《林州市公共资源交易中心网》公示，各供应商对推荐中标意见如有异议，可在公示期内向代理机构提出书面质疑，如对代理机构的答复仍有异议，可向林州市政府采购管理办公室书面投诉。(具体程序按豫财购[2004]23号《政府采购信息公告管理办法》、《政府采购供应商投诉处理办法》、《政府采购货物和服务招标投标管理办法》文件执行。      8. 联系方式   采 购 人：林州市文化广播电视新闻出版局 02 0202   联 系 人：李先生 02   联系电话：13526189698 02 02   招标代理机构：河南联达工程管理有限公司   联 系 人：李先生   联系电话：13526110202   监督电话：0372-6282628      林州市文化广播电视新闻出版局'
    result_data = obj.do_parser(content, '')

    for company in result_data.get("houxuan_zhongbiao", ""):
        print "houxuanzhongbiao:", company
    for company in result_data.get("zhongbiao", ""):
        print "zhongbiao:", company

    print "zhaobiao:", result_data.get("zhaobiao", "")
    print "agent:", result_data.get("agent", "")

    '''host = '101.201.102.37'
    port = 28019
    database = 'final_data'
    coll = 'bid_detail'
    client = MongoClient(host, port)
    db = client[database][coll]
    cursor = db.find().skip(300)
    num = 0
    begin_time = time.time()
    print "start"
    for item in cursor:
        print "start"
        try:
            num += 1
            src_url = item.get('_src')[0]['url']
            content = item.get('bid_content')
            title = item.get('title', '')
            content = ' 本招标项目前郭县司法局印刷图书项目已由前郭县财政局政府采购监督管理工作办公室、前郭尔罗斯蒙古族自治县机关事务管理局批准建设，项目业主为前郭尔罗斯蒙古族自治县司法局，建设资金来自财政拨款，项目出资比例为100%。项目已具备招标条件，现采用竞争性谈判方式对前郭县司法局印刷图书项目进行招标。          一．采购方式：竞争性谈判          二．采购单位：前郭尔罗斯蒙古族自治县司法局          三．采购项目编号：Qgzfcgzx-2015002          四．采购项目内容：印刷采购各类司法图书          五．采购预算：人民币壹拾玖万陆仟捌佰伍拾元整（¥196850.00元）          六．报名及谈判文件发放时间和地点须知：          1.报名时间：2015年3月31日至2015年4月2日(法定公休日、法定节假日休息)，每日上午_8时至11_时，下午13_时至16时。          2、报名地点：前郭县建设工程交易中心（乌兰大街1680号）          七．报名时需提供的资质证件：          报名及领取竞争性谈判招标文件须提供法人授权书及其授权人身份证原件，法人单位的营业执照、印刷经营许可证、税务登记证、组织机构代码证原件和复印件（加盖本公司公章）。          八.竞争性谈判时间和地点：          1、投标文件递交时间：2015年4月17日13时00分至13时30分止。          2、竞争性谈判时间：2015年4月17日13时30分。          3、竞争性谈判地点：前郭县建设工程交易中心（乌兰大街1680号）          九．联系方式          招标人：前郭尔罗斯蒙古族自治县司法局          联系人：李慧超          电02话：13756702811          招标代理机构：02前郭县建设工程招标造价咨询有限责任公司          地址：02前郭县乌兰大街1680号邮编：13800002          联系人：02王颖新、吕佳          电话：020438-210724202传真：0438-2107242'
            result_data = obj.do_parser(content, title)



            print src_url, "'", content, "'"
            print "bid_type:", item.get("bid_type")
            for company in result_data.get("houxuan_zhongbiao", ""):
                print "houxuanzhongbiao:", company
            for company in result_data.get("zhongbiao", ""):
                print "zhongbiao:", company

            print "zhaobiao:", result_data.get("zhaobiao", "")
            print "agent:", result_data.get("agent", "")

            if num % 100 == 0:
                print num, "time_cost:", time.time() - begin_time
                break
        except:
            print traceback.format_exc()'''
