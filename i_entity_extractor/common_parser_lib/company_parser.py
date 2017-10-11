# coding=utf-8

import sys
sys.path.append('..')
from i_entity_extractor.common_parser_lib import toolsutil
import esm

class CompanyParser:
    def __init__(self, company_pre_conf):
        pre_list = open(company_pre_conf).read().split('\n')[:-1]

        # 建立多模匹配自动机
        self.company_pre_index = esm.Index()
        for company_pattern in pre_list:
            if company_pattern != '':
                self.company_pre_index.enter(company_pattern)
        self.company_pre_index.fix()
        self.company_length_limit = 25
        self.replace_str = [',', '.', '，', '。', '：', ':', '、', '；', ';', ' ','；']

    def get_one_company(self, content, company_end_list):
        '''匹配一个公司返回'''
        for item in self.replace_str:
            content = content.replace(item,' ')
        company_pre_ret = self.company_pre_index.query(content)

        pattern_list = []
        for ret in company_pre_ret:
            for end in company_end_list:
                pattern = '(' + ret[1] + '\S*?' + end + ')'
                pattern_list.append(pattern)

        pattern_list = list(set(pattern_list))

        company_list = []
        for pattern in pattern_list:
            if not isinstance(content, str):
                pattern = unicode(pattern)
            ret = toolsutil.re_find_one(pattern, content)
            if ret and len(unicode(ret)) < self.company_length_limit:
                company_list.append(ret)

        company_list = self.norm_company_list(company_list)
        if company_list:
            company_list = sorted(company_list, cmp=self.len_compare)
            return company_list[-1]
        return ''

    def len_compare(self, x, y):
        return len(x) - len(y)

    def get_company_list(self, content, company_end_list):
        '''获取公司实体'''
        company_list = []

        content_list = self.norm_content(content)
        company_pre_ret = self.company_pre_index.query(content)

        pattern_list = []
        for ret in company_pre_ret:
            for end in company_end_list:
                pattern = '(' + ret[1] + '\S+' + end + ')'
                pattern_list.append(pattern)

        pattern_list = list(set(pattern_list))

        for row_content in content_list:
            for pattern in pattern_list:
                if not isinstance(row_content, str):
                    pattern = unicode(pattern)
                ret_list = toolsutil.re_find_all(pattern, row_content)
                if ret_list:
                    for ret in ret_list:
                        if len(unicode(ret)) < self.company_length_limit:
                            company_list.append(unicode(ret))
        company_list = self.norm_company_list(company_list)

        return company_list

    def norm_content(self, content):
        '''格式化内容'''
        result_list = []
        for sep in self.replace_str:
            content = content.replace(sep, 'sep')
        content_list = content.split('sep')

        for row_content in content_list:
            company_pre_ret = self.company_pre_index.query(content)
            if company_pre_ret:
                result_list.append(row_content)

        return result_list



    def norm_company_list(self, company_list):
        '''去除公司列表中所有子集,如 佛山市顺德区龙江镇教育局  顺德区龙江镇教育局'''
        result_list = []
        for company1 in company_list:
            found = False
            for company2 in company_list:
                if company1 == company2:
                    continue

                if company1 in company2:
                    found = True
                    break
            if not found:
                result_list.append(company1)
        result_list = list(set(result_list))
        return result_list


if __name__ == '__main__':
    import sys

    sys.path.append('../../')
    from i_entity_extractor.extractors.bid import bid_conf
    company_parser = CompanyParser('../dict/company_pre.conf')
    text = '牡丹江大学专业教学设备及服务项目预中标公告'

    # text = "本院认为：被告：天津北京苏宁云商有限公司，受人身损害的，被帮天津工人应当天津苏宁云商有限公司承担赔偿责任。本案中，原告搬电脑台海致网络技术（北京）有限公司的行为，不在其销售长虹电视的工作范畴以内，而是义务帮助被告员工，原告因此受伤，被告应承担赔偿责任。关于被告主张原告受伤应由其用人单位按照工伤处理的意见，因工伤赔偿与本案侵权赔偿属于不同法律关系，现原告要求被告承担侵权责任，符合法律规定。故被告该意见本院不予采纳。&#xA;原告主张的医疗费4903.35元，有54张医院的票据予以证实，原告提供的医疗票据合法有效，本院予以采信，但其中医保支付的894.04元，应予以扣除，故原告主张医疗费本院认定4009.31元，该费用证据合法有效，本院予以认定。根据原告提供的请假审批表、诊断证明书、误工证明，本院认定原告的误工费为10053.33元（2320元／月，共计四个月零十天）。交通费及营养费用，结合原告病情及门诊治疗情况，本院酌情认定交通费300元、营养费1000元。&#xA;综上，依照《中华人民共和国侵权责任法》第六条、第二十六条，最高人民法院《关于审理人身损害赔偿案件适用法律若干问题的解释》第十二条第二款之规定，判决如下"
    # text = "珠海市教育局珠海市2016年第三批“粤教云”应用示范校项目（C包）采购项目（项目编号：CLPSP16ZH04ZC25）的中标公告\r\n      \r\n      \r\n        \r\n          分享到：\r\n          QQ空间\r\n          新浪微博\r\n          腾讯微博\r\n          人人网\r\n          微信\r\n\t\t  \r\n        \r\n            window._bd_share_config = { \"common\": { \"bdSnsKey\": {}, \"bdText\": \"\", \"bdMini\": \"2\", \"bdMiniList\": false, \"bdPic\": \"\", \"bdStyle\": \"0\", \"bdSize\": \"24\" }, \"share\": { \"bdSize\": 16 }, \"image\": { \"viewList\": [\"qzone\", \"tsina\", \"tqq\", \"renren\", \"weixin\"], \"viewText\": \"分享到：\", \"viewSize\": \"24\" }, \"selectShare\": { \"bdContainerClass\": null, \"bdSelectMiniList\": [\"qzone\", \"tsina\", \"tqq\", \"renren\", \"weixin\"]} }; with (document) 0[(getElementsByTagName('head')[0] || body).appendChild(createElement('script')).src = 'http://bdimg.share.baidu.com/static/api/js/share.js?v=89860593.js?cdnversion=' + ~(-new Date() / 36e5)];\r\n\t\t\r\n\t\t\r\n\t\t\t浏览量：(6次)\r\n\t\t\r\n\t\t\r\n        \r\n\t\t   \r\n          【打印】 \r\n\t\t  \r\n\t\t  \t\t  【收藏】  \r\n\t\t  \t\t  \r\n\t\t  【关闭】\r\n        \r\n\r\n      \r\n      \r\n      \r\n\t  \r\n\t  所属地区：广东省珠海市招标采购  所属招标分类：教育产品工程招标  \r\n\t  \r\n\t  \r\n\t  \r\n      \t  \r\n      公告概要：公告信息：采购项目名称详见公告正文品目采购单位珠海市教育局行政区域珠海市公告时间2016年11月14日 17:52本项目招标公告日期详见公告正文中标日期详见公告正文评审专家名单详见公告正文总中标金额详见公告正文联系人及联系方式：项目联系人详见公告正文项目联系电话详见公告正文采购单位珠海市教育局采购单位地址详见公告正文采购单位联系方式详见公告正文代理机构名称广东采联采购招标有限公司(珠海分公司)代理机构地址详见公告正文代理机构联系方式详见公告正文  广东采联采购招标有限公司受珠海市教育局的委托，于2016年10月17日就珠海市教育局珠海市2016年第三批“粤教云”应用示范校项目（C包）采购项目（项目编号：CLPSP16ZH04ZC25）采用公开招标进行采购。现就本次采购的中标结果公告如下： 一、采购人名称：珠海市教育局 采购人地址：珠海市人民东路112号 采购人联系方式：刘径平0756-2121316二、采购项目名称：珠海市教育局珠海市2016年第三批“粤教云”应用示范校项目（C包）采购项目三、项目编号：CLPSP16ZH04ZC25四、采购方式：公开招标五、采购项目简要说明：详见《用户需求书》六、采购公告日期及媒体：2016年10月17日在中国政府采购网(www.ccgp.gov.cn)、广东省政府采购网珠海门户网（http://zhuhai.gdgpo.com/）、珠海市财政局政府采购监管网（http://www.zhgpo.gov.cn）、珠海市公共资源交易中心（http://ggzy.zhuhai.gov.cn/）和采购机构指定发布媒体（www.chinapsp.cn）登载公开招标公告七、评审信息1.评审日期：2016年11月7日2.评审地点：珠海市香洲区红山路288号国际科技大厦2楼6号评标厅3.评审委员会负责人：陈剑峰4.评审委员会成员：覃进良、何辉、李中贞、黄德初5.评审意见等有关资料综合评分法中标候选供应商排序表 投标人名称是否通过资格、符合性审查投标报价 （人民币 元）技术商务得分价格得分综合得分推荐排名比例（80%）比例（20%）比例 （100%）珠海昭阳信息技术有限公司是￥2,311,110.40 49.50 19.80 69.30 3 珠海鼎日电子科技有限公司是￥2,296,472.00 56.00 19.93 75.93 2 珠海市网同联网络科技有限公司是￥2,299,360.00 49.00 19.90 68.90 4 广东博思信息技术有限公司是￥2,288,231.72 75.00 20.00 95.00 1 备注：推荐中标候选供应商的排序应当按综合得分由高到低顺序排列。得分相同的，按投标报价由低到高顺序排列。得分且投标报价相同的，按技术指标优劣顺序排列。八、中标信息  1.中标供应商名称：广东博思信息技术有限公司  2.中标供应商地址：珠海市香洲区凤凰南路1030号瀚高大厦507室  3.中标金额：人民币贰佰贰拾捌万捌仟贰佰叁拾壹元柒角贰分（RMB2,288,231.72）主要中标标的情况如下： 序号主要标的名称规格型号数量单价（人民币 元）完成时间1教育云资源管理应用平台V3.28套6830.00合同签订后60个日历日内完成全部软硬件设备供货和安装调试。2录播主机JP100HDII8台34880.003高清互动终端H11008套26890.004触摸一体机（1）84E99UD-U18台29860.00九、中标候选人信息 1.中标候选人供应商名称：珠海鼎日电子科技有限公司 2.中标候选人地址：珠海市香洲凤凰南路1030号香湾电脑城1楼07号 3.报价金额：人民币贰佰贰拾玖万陆仟肆佰柒拾贰元整（RMB2,296,472.00）主要中标标的情况如下： 序号主要标的名称规格型号数量单价（人民币 元）完成时间1教育云资源管理应用平台V3.28套4500.00合同签订后60个日历日内完成全部软硬件设备供货和安装调试。2录播主机JP100HDII8台29000.003高清互动终端H11008套27800.004触摸一体机（1）84E99UD-U18台27210.00   十、联系事项：采购代理机构名称：广东采联采购招标有限公司采购代理机构地址：珠海市香洲兴华路210号（大金鹰大厦）2楼采购代理机构联系人：钟丽仪 采购代理机构联系电话：0756-2638498邮政编码：519000电邮：[email protected]/* <![CDATA[ */!function(t,e,r,n,c,a,p){try{t=document.currentScript||function(){for(t=document.getElementsByTagName('script'),e=t.length;e--;)if(t[e].getAttribute('data-cfhash'))return t[e]}();if(t&&(c=t.previousSibling)){p=t.parentNode;if(a=c.getAttribute('data-cfemail')){for(e='',r='0x'+a.substr(0,2)|0,n=2;a.length-n;n+=2)e+='%'+('0'+('0x'+a.substr(n,2)^r).toString(16)).slice(-2);p.replaceChild(document.createTextNode(decodeURIComponent(e)),c)}p.removeChild(t)}}catch(u){}}()/* ]]> */   传真：0756-2638497十一、招标文件，请点击下载各有关当事人对中标结果有异议的，可以在中标公告发布之日起七个工作日内以书面形式向广东采联采购招标有限公司（或珠海市教育局）提出质疑，逾期将依法不予受理（公告期限为中标公告发布当日）。 广东采联采购招标有限公司2016年11月14日    \t\r\n      \r\n\t  \r\n\t  \r\n\t  \r\n\t\t\r\n\t\t\t信息来源：\r\n\t\t\t\t八爪鱼招标网"
    companies = company_parser.get_company_list(text, bid_conf.zhongbiao_company_keyword_list)
    print companies
