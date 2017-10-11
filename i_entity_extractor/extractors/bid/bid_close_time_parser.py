# coding=utf8
import sys
import time
import traceback
import bid_conf
import esm
from i_entity_extractor.common_parser_lib import toolsutil

class BidCloseTimeParser:
    def __init__(self, date_parser):
        self.content_length = 50
        self.close_index = esm.Index()
        for keyword in bid_conf.bid_close_keyword_list:
            self.close_index.enter(keyword)
        self.close_index.fix()
        self.deal_num = 10
        self.date_parser = date_parser

    def do_parser(self, content):
        '''从内容中提取区域城市'''
        close_time = ''
        if not content:
            return close_time
        content = content.encode('utf8').replace(' ', '')
        close_time = self.get_time(content, self.content_length)

        for num in range(self.deal_num):
            if not close_time:
                close_time = self.get_time(content, self.content_length + (num + 1) * 15)
            else:
                break

        close_time = toolsutil.norm_date_time(close_time)
        return close_time

    def get_time(self, content, content_length):
        '''获取时间'''
        close_time = ''
        close_ret = self.close_index.query(content)
        if close_ret:
            for ret in close_ret:
                pos = ret[0][1]
                relate_content = content[pos:]
                close_time = self.date_parser.get_date_list(relate_content)
                if close_time:
                    break
        return close_time


if __name__ == "__main__":
    from pymongo import MongoClient
    from i_entity_extractor.common_parser_lib.date_parser import DateParser

    litigants = ""
    sys.path.append('../../')
    date_parser = DateParser()

    obj = BidCloseTimeParser(date_parser)

    host = '101.201.102.37'
    port = 28019
    database = 'final_data'
    coll = 'bid_detail'
    client = MongoClient(host, port)
    db = client[database][coll]
    cursor = db.find().skip(500)
    num = 0
    begin_time = time.time()
    for item in cursor:
        try:
            num += 1
            src_url = item.get('_src')[0]['url']
            content = item.get('bid_content')
            title = item.get('title', '')
            # content = ' 南阳理工学院2016全校多媒体教室建设项目成交结果公示					类型：	河南招标网	--中标信息		发布时间：	2016-6-16																河南	招标投标网	(http://www.hnzbtb.com)与你携手共同发展!				南阳理工学院2016全校多媒体教室建设项目开标会议于2016年6月16日上午9:00时（北京时间）在河南省城建建设管理有限公司四楼开标室准时召开并进行评审，现将结果公告如下：	一、招标项目名称及编号	项目名称：南阳理工学院2016全校多媒体教室建设项目	招标编号：nyzjgk2016-95	二、招标项目简要说明：南阳理工学院2016全校多媒体教室建设项目（具体采购内容及要求详见采购文件）	三、评标信息：	评标日期：2016年6月16日	评标地点：河南省城建建设管理有限公司四楼开标室	评标委员会成员： 孙君安、杜恒、张凯、李玉常、宋定宇	监督人员:封晓鸿、江军辉、畅为航	五、成交信息：	成交供应商：河南云朵教育科技有限公司	投标报价：492800.00元	单位地址：郑州市高新技术产业开发区西三环路283号11幢6层33号货物、品牌名称 规格型号、技术指标 厂家 产地 单位 数量 单价 供货安装期 质保期	投影机（工程机） 爱普生cb-4550	技术指标详见技术规格偏差一览表 爱普生（中国）有限公司 中国	深圳 台 20 10800元 按采购人需求安装供货 二年	品牌计算机 hp pro 280g2	技术指标详见技术规格偏差一览表 中国惠普有限公司 中国	上海 台 20 4650元	功放 ckmusic ck-2126	技术指标详见技术规格偏差一览表 深圳市创控电子有限公司 中国	深圳 台 20 1600元	音箱 ckmusic ck-1166	技术指标详见技术规格偏差一览表 深圳市创控电子有限公司 中国	深圳 对 20 1480元	中央控制台 道图dt115a	技术指标详见技术规格偏差一览表 济南道图工贸有限公司 中国	济南 台 20 1500元	中控 纬创dj1600	技术指标详见技术规格偏差一览表 广州新纬创电子科技有限公司 中国	广州 台 20 1200元	鹅颈话筒 ckmusic e6	技术指标详见技术规格偏差一览表 深圳市创控电子有限公司 中国	深圳 支 20 270元	电动幕布 金力泰	技术指标详见技术规格偏差一览表 张家港市三星银屏器材有限公司 中国	张家港 台 20 1160元	窗帘及凳子 现场定制 套 20 1980元	六、本项目联系方式：	采 购 人：南阳理工学院	地 址：南阳市长江路80号	联 系 人：祁老师	电 话：0377-62075392	招标代理：河南省城建建设管理有限公司	地 址：南阳市工业路62号（南阳市科技展览馆三楼）	联 系人；刘女士	电 话：0377--63189086	成交结果公告期限：自成交结果公告发布之日起一个工作日	各有关当事人对评标结果有异议的，可以在成交结果公告期限届满之日起7个工作日内，以书面形式向采购人和采购代理机构提出质疑（加盖单位公章且法人签字），由法定代表人或其原授权代表携带企业营业执照副本原件及本人身份证件（原件）一并提交（邮寄、传真件不予受理），并以质疑函接受确认日期作为受理时间。逾期未提交或未按照要求提交的质疑函将不予受理。	2016年6月16日	招标文件							来源：河南招标投标网-河南最具权威网站,专业的招标采购网站																		相关信息																·		郑州市管城回族区卫生监督所采购卫生监督电动执法车项目竞争性谈判公告					·		濮阳经济技术开发区社会事业局所需全民健身路径工程项目公开招标公告					·		南阳市城乡一体化示范区新店乡2016年通村公路建设项目第4标段二次招标公告（市政公用或公路）					·		焦作市公安局关于老干科办公楼电梯采购项目的竞争性谈判公告					·		河南工业大学新校区理工组团结构建材中心一层地面改造工程项目谈判公告 '
            print src_url, "'", content, "'"
            ret = obj.do_parser(content)
            print ret

            if num % 200 == 0:
                print num, "time_cost:", time.time() - begin_time
                break
        except:
            print traceback.format_exc()
