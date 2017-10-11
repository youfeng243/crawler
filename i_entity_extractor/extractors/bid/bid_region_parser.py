# coding=utf8
import sys
import time
import traceback
import bid_conf
import esm
from i_entity_extractor.common_parser_lib import toolsutil


class BidRegionParser:
    def __init__(self, province_parser):
        self.content_length = 50
        self.province_parser = province_parser
        self.phone_index = esm.Index()
        for keyword in bid_conf.phone_keyword_list:
            self.phone_index.enter(keyword)
        self.phone_index.fix()

        self.address_index = esm.Index()
        for keyword in bid_conf.address_keyword_list:
            self.address_index.enter(keyword)
        self.address_index.fix()

    def do_parser(self, content):
        '''从内容中提取区域城市'''
        region = ""
        if not content:
            return region
        content = content.encode('utf8').replace(' ', '').replace('\t','').replace('\n','')

        #region = self.get_region_from_phone(content)

        if region == "":
            region = self.get_region_from_address(content)
        region = unicode(region)
        if not region:
            region = ""

        return region

    def get_region_from_phone(self, content):
        content_ret = self.phone_index.query(content)
        region_count_map = {}
        if content_ret:
            for ret in content_ret:
                pos = ret[0][0]
                phone_content = content[pos:pos + self.content_length]
                phone_num = toolsutil.re_find_one('\d+|\d+-\d+', phone_content)
                if phone_num:
                    region = self.province_parser.get_region_from_phonenum(phone_num)
                    if region:
                        if region not in region_count_map.keys():
                            region_count_map[region] = 0
                        region_count_map[region] += 1

        for k, v in sorted(region_count_map.items(), lambda x, y: cmp(x[1], y[1]), reverse=True):
            return k
        return ''

    def get_region_from_address(self, content):
        '''从地址中提取区域城市'''
        region = ''
        content_ret = self.address_index.query(content)
        if content_ret:
            pos = content_ret[-1][0][0]
            address_content = content[pos:pos + self.content_length]
            region = self.province_parser.get_region(address_content,1)

        return region


if __name__ == "__main__":
    from i_entity_extractor.common_parser_lib.province_parser import ProvinceParser
    from pymongo import MongoClient

    litigants = ""
    sys.path.append('../../')
    province_parser = ProvinceParser('../../dict/province_city.conf', '../../dict/phonenum_city.conf',
                                     '../../dict/region_city.conf',"../../dict/city.conf")

    obj = BidRegionParser(province_parser)

    host = '101.201.102.37'
    port = 28019
    database = 'final_data'
    coll = 'bid_detail'
    client = MongoClient(host, port)
    db = client[database][coll]
    cursor = db.find({'bid_type': '中标'}).limit(1000).skip(100)
    num = 0
    begin_time = time.time()
    content = "山东\t招标网\t(\thttp://www.sdzbw.com\t)与你携手共同发展!\t\t\t\t\t\t\t一、采购项目名称：医用臭氧治疗仪\t\t二、采购项目编号：JNCZJZ-2015-795\t\t三、采购项目分包情况：\t\t\t\t\t\t\t包号\t\t\t\t货物服务名称\t\t\t\t供应商资格要求\t\t\t\t本包预算金额\t\t\t\t\t\t未分包\t\t\t\t医用臭氧治疗仪\t\t\t\t（1）符合《政府采购法》第二十二条规定的条件；（2）具有本次招标项目的生产或经营范围，有能力提供本次采购项目及所要求的服务\t\t\t\t48万元\t\t\t\t\t\t\t四、获取谈判（磋商）文件：\t\t1.时间：2016年03月18日至2016年03月25日16点整\t\t2.地点：济南市政务服务中心政府采购部网站\t\t3.方式：网站“采购公告”栏目中,在对应项目公告最下方自行下载谈判文件及报名\t\t4.售价：免费\t\t五、递交响应文件时间及地点\t\t时间：2016年03月31日 08:30—09:30 (北京时间)\t\t地点：济南市市中区站前路9号市政务服务中心1号楼\t\t六、谈判（开启）时间及地点\t\t时间：2016年03月31日 09:30 (北京时间)\t\t地点：济南市市中区站前路9号市政务服务中心1号楼\t\t七、联系方式\t\t1.采购人：济南市民族医院\t\t地址：济南市民族医院\t\t联系人：陈俊杰\t\t联系方式：0531-86060159\t\t2.采购代理机构：济南市政务服务中心政府采购部\t\t地址：济南市市中区站前路9号市政务服务中心1号楼\t\t联系人：曹伯军\t\t联系方式：0531-68967547\t\t\t\t\t\t\t\t\t\t\t\t来源：山东招标网-山东最具权威网站,专业的招标采购网站\t\t\t[打印本页]\t\t\t[关闭本页]"

    ret = obj.do_parser(content)
    print ret
