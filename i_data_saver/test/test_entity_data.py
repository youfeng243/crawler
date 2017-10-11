# -*- coding: utf-8 -*-

import json
from bdp.i_crawler.i_data_saver import DataSaverService
from bdp.i_crawler.i_entity_extractor.ttypes import EntityExtractorInfo, EntitySource
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
#获取客户端
def getclient():
    try:
        transport = TSocket.TSocket('127.0.0.1', 12600)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = DataSaverService.Client(protocol)
        transport.open()
        return client,transport
    except Exception as e:
        print e.message
#关闭transport
def closetransport(transport):
    try:
        transport.close()
    except Exception as e:
        print e.message
data_foo = EntityExtractorInfo(
        entity_source = EntitySource(
            url =       'http://www.baidu.com',
            url_id =    12345,
            site =      'www.baidu.com',
            site_id =   23456,
            domain =    'baidu.com',
            domain_id = 34567,
            src_type =  'foo-src-type',
            download_time = 1470822227
        ),
        update_time =   1470822230,
        topic_id =      20,
        entity_data =   json.dumps({
            '_record_id': 'ffeeddccbbaa',
            'name': 'sunyaxing(startar)',
            'age': 26,
            'birth': '1990-04-10 11:31:44',
            'phone': [
                '13131313'
            ]
        })
    )

data_fygg = EntityExtractorInfo(
        update_time = None, 
        entity_data = json.dumps({
            "province": "北京市", 
            "court": "[北京]北京市丰台区人民法院", 
            "entity_list": [
                "钟双辉", 
                "马巧"
            ], 
            "bulletin_content": "钟双辉：本院受理原告马巧灵诉你民间借贷纠纷一案，现依法向你送达起诉状副本及开庭传票，自公告之日起经过60日即视为送达，提出答辩期为公告期满后15日，举证期限为 公告期满后的30日内。并定于举证期限届满后的第3天上午9时（遇节假日顺延）在本院花乡法庭公开开庭审理，逾期将依法缺席判决。", 
            "litigants": [
                "钟双辉", 
                "马巧灵"
            ], 
            "bulletin_date": "二〇一六年七月八日", 
            "case_id": "", 
            "in_time": None, 
            "bulletin_type": "起诉状副本及开庭传票", 
            "_record_id": "926bbba5116ad86b1ef2253d39e785a3", 
            "pdf": "http://rmfygg.court.gov.cn/psca/lgnot/bulletin/download/4763905.pdf", 
            "src_type": "system", 
            "plaintiffs": [
                "马巧灵"
            ], 
            "document": "起诉状副本及开庭传票 钟双辉 钟双辉：本院受理原告马巧灵诉你民间借贷纠纷一案，现依法向你送达起诉状副本及开庭传票，自公告之日起经过60日即视为送达，提出答辩期为公告期 满后15日，举证期限为公告期满后的30日内。并定于举证期限届满后的第3天上午9时（遇节假日顺延）在本院花乡法庭公开开庭审理，逾期将依法缺席判决。 [北京]北京市丰台区人民法院 刊登版面：G29 刊登日期：2016-07-17 上传日期：2016-07-17 下载打印本公告 (公告样报已直接寄承办法官) ", 
            "defendants": [
                "钟双辉"
            ], 
            "case_cause": "民间借贷纠纷",
            "_utime": '2222-22-22 11:11:11'
        }), 
        entity_source = EntitySource(
            domain = None, 
            url = 'http://www.baidu.com', 
            download_time = 1471254582, 
            site_id = 5, 
            site = None, 
            src_type = None, 
            url_id = None, 
            domain_id = None
        ),
        topic_id = 3
    )

data_ssgg = EntityExtractorInfo(
        update_time = None, 
        entity_data = '{"publish_time": "2016-07-14", "company": "\\u6df1\\u5733\\u71c3\\u6c14", "code": "601139", "in_time": "2016-08-12 11:14:51", "notice_id": "2016-025\\u53f7\\r\\n", "industry": "\\u516c\\u7528\\u4e8b\\u4e1a", "content": "\\u8bc1\\u5238\\u4ee3\\u7801\\uff1a601139       \\u8bc1\\u5238\\u7b80\\u79f0\\uff1a\\u6df1\\u5733\\u71c3\\u6c14       \\u516c\\u544a\\u7f16\\u53f7\\uff1a2016-025\\u53f7\\r\\n                    \\u6df1\\u5733\\u5e02\\u71c3\\u6c14\\u96c6\\u56e2\\u80a1\\u4efd\\u6709\\u9650\\u516c\\u53f8\\r\\n     \\u516c\\u5f00\\u53d1\\u884c2016\\u5e74\\u516c\\u53f8\\u503a\\u5238\\uff08\\u7b2c\\u4e00\\u671f\\uff09\\u53d1\\u884c\\u7ed3\\u679c\\u516c\\u544a\\r\\n    \\u53d1\\u884c\\u4eba\\u53ca\\u8463\\u4e8b\\u4f1a\\u5168\\u4f53\\u6210\\u5458\\u4fdd\\u8bc1\\u672c\\u516c\\u544a\\u5185\\u5bb9\\u4e0d\\u5b58\\u5728\\u4efb\\u4f55\\u865a\\u5047\\u8bb0\\u8f7d\\u3001\\u8bef\\u5bfc\\u6027\\u9648\\u8ff0\\u6216\\u8005\\u91cd\\u5927\\u9057\\u6f0f\\uff0c\\u5e76\\u5bf9\\u5176\\u5185\\u5bb9\\u7684\\u771f\\u5b9e\\u6027\\u3001\\u51c6\\u786e\\u6027\\u548c\\u5b8c\\u6574\\u6027\\u627f\\u62c5\\u4e2a\\u522b\\u53ca\\u8fde\\u5e26\\u8d23\\u4efb\\u3002\\r\\n    \\u7ecf\\u4e2d\\u56fd\\u8bc1\\u5238\\u76d1\\u7763\\u7ba1\\u7406\\u59d4\\u5458\\u4f1a\\u8bc1\\u76d1\\u8bb8\\u53ef\\u30102016\\u30111284\\u53f7\\u6587\\u6838\\u51c6\\uff0c\\u6df1\\u5733\\u5e02\\u71c3\\u6c14\\u96c6\\u56e2\\u80a1\\u4efd\\u6709\\u9650\\u516c\\u53f8\\uff08\\u4ee5\\u4e0b\\u7b80\\u79f0\\u201c\\u53d1\\u884c\\u4eba\\u201d\\uff09\\u83b7\\u51c6\\u9762\\u5411\\u516c\\u4f17\\u6295\\u8d44\\u8005\\u516c\\u5f00\\u53d1\\u884c\\u603b\\u989d\\u4e0d\\u8d85\\u8fc7\\u4eba\\u6c11\\u5e0110\\u4ebf\\u5143\\u7684\\u516c\\u53f8\\u503a\\u5238\\uff08\\u4ee5\\u4e0b\\u7b80\\u79f0\\u201c\\u672c\\u6b21\\u503a\\u5238\\u201d\\uff09\\u3002\\r\\n    \\u53d1\\u884c\\u4eba\\u672c\\u6b21\\u503a\\u5238\\u91c7\\u7528\\u5206\\u671f\\u53d1\\u884c\\u7684\\u65b9\\u5f0f\\uff0c\\u6839\\u636e\\u300a\\u6df1\\u5733\\u5e02\\u71c3\\u6c14\\u96c6\\u56e2\\u80a1\\u4efd\\u6709\\u9650\\u516c\\u53f8\\u516c\\u5f00\\u53d1\\u884c2016\\u5e74\\u516c\\u53f8\\u503a\\u5238\\uff08\\u7b2c\\u4e00\\u671f\\uff09\\u53d1\\u884c\\u516c\\u544a\\u300b\\uff0c\\u6df1\\u5733\\u5e02\\u71c3\\u6c14\\u96c6\\u56e2\\u80a1\\u4efd\\u6709\\u9650\\u516c\\u53f8\\u516c\\u5f00\\u53d1\\u884c2016\\u5e74\\u516c\\u53f8\\u503a\\u5238\\uff08\\u7b2c\\u4e00\\u671f\\uff09\\uff08\\u4ee5\\u4e0b\\u7b80\\u79f0\\u201c\\u672c\\u671f\\u503a\\u5238\\u201d\\uff09\\u7684\\u53d1\\u884c\\u89c4\\u6a21\\u4e3a\\u4eba\\u6c11\\u5e015\\u4ebf\\u5143\\u3002\\u672c\\u671f\\u503a\\u5238\\u53d1\\u884c\\u4ef7\\u683c\\u4e3a\\u6bcf\\u5f20100\\u5143\\uff0c\\u91c7\\u53d6\\u7f51\\u4e0a\\u9762\\u5411\\u793e\\u4f1a\\u516c\\u4f17\\u6295\\u8d44\\u8005\\u516c\\u5f00\\u53d1\\u884c\\u548c\\u7f51\\u4e0b\\u9762\\u5411\\u673a\\u6784\\u6295\\u8d44\\u8005\\u8be2\\u4ef7\\u914d\\u552e\\u76f8\\u7ed3\\u5408\\u7684\\u65b9\\u5f0f\\u53d1\\u884c\\u3002\\r\\n    \\u672c\\u671f\\u503a\\u5238\\u53d1\\u884c\\u5de5\\u4f5c\\u5df2\\u4e8e2016\\u5e747\\u670813\\u65e5\\u7ed3\\u675f\\uff0c\\u672c\\u671f\\u503a\\u5238\\u5b9e\\u9645\\u53d1\\u884c\\u89c4\\u6a21\\u4e3a5\\u4ebf\\u5143\\uff0c\\u53d1\\u884c\\u5177\\u4f53\\u60c5\\u51b5\\u5982\\u4e0b\\uff1a\\r\\n    1\\u3001\\u7f51\\u4e0a\\u53d1\\u884c\\r\\n    \\u672c\\u671f\\u503a\\u5238\\u7f51\\u4e0a\\u9884\\u8bbe\\u7684\\u53d1\\u884c\\u6570\\u91cf\\u4e3a\\u4eba\\u6c11\\u5e010.5\\u4ebf\\u5143\\uff0c\\u6700\\u7ec8\\u7f51\\u4e0a\\u4e00\\u822c\\u516c\\u4f17\\u6295\\u8d44\\u8005\\u7684\\u8ba4\\u8d2d\\u6570\\u91cf\\u4e3a\\u4eba\\u6c11\\u5e010.5\\u4ebf\\u5143\\uff0c\\u5360\\u672c\\u6b21\\u53d1\\u884c\\u89c4\\u6a2110%\\u3002\\r\\n    2\\u3001\\u7f51\\u4e0b\\u53d1\\u884c\\r\\n    \\u672c\\u671f\\u503a\\u5238\\u7f51\\u4e0b\\u9884\\u8bbe\\u7684\\u53d1\\u884c\\u6570\\u91cf\\u4e3a\\u4eba\\u6c11\\u5e014.5\\u4ebf\\u5143\\uff0c\\u6700\\u7ec8\\u7f51\\u4e0b\\u673a\\u6784\\u6295\\u8d44\\u8005\\u7684\\u8ba4\\u8d2d\\u6570\\u91cf\\u4e3a\\u4eba\\u6c11\\u5e014.5\\u4ebf\\u5143\\uff0c\\u5360\\u672c\\u6b21\\u53d1\\u884c\\u89c4\\u6a2190%\\u3002\\r\\n    \\u7279\\u6b64\\u516c\\u544a\\u3002\\r\\n    \\u3010\\u672c\\u9875\\u65e0\\u6b63\\u6587\\uff0c\\u4e3a\\u300a\\u897f\\u5b89\\u9686\\u57fa\\u7845\\u6750\\u6599\\u80a1\\u4efd\\u6709\\u9650\\u516c\\u53f8\\u516c\\u5f00\\u53d1\\u884c2016\\u5e74\\u516c\\u53f8\\u503a\\u5238\\uff08\\u7b2c\\u4e00\\u671f\\uff09\\u53d1\\u884c\\u7ed3\\u679c\\u516c\\u544a\\u300b\\u4e4b\\u76d6\\u7ae0\\u9875\\u3011\\r\\n                                         \\u53d1\\u884c\\u4eba\\uff1a\\u897f\\u5b89\\u9686\\u57fa\\u7845\\u6750\\u6599\\u80a1\\u4efd\\u6709\\u9650\\u516c\\u53f8\\r\\n                                                                   \\u5e74\\u6708\\u65e5\\r\\n    \\u3010\\u672c\\u9875\\u65e0\\u6b63\\u6587\\uff0c\\u4e3a\\u300a\\u897f\\u5b89\\u9686\\u57fa\\u7845\\u6750\\u6599\\u80a1\\u4efd\\u6709\\u9650\\u516c\\u53f8\\u516c\\u5f00\\u53d1\\u884c2016\\u5e74\\u516c\\u53f8\\u503a\\u5238\\uff08\\u7b2c\\u4e00\\u671f\\uff09\\u53d1\\u884c\\u7ed3\\u679c\\u516c\\u544a\\u300b\\u4e4b\\u76d6\\u7ae0\\u9875\\u3011\\r\\n                                              \\u4e3b\\u627f\\u9500\\u5546\\uff1a\\u56fd\\u4fe1\\u8bc1\\u5238\\u80a1\\u4efd\\u6709\\u9650\\u516c\\u53f8\\r\\n                                                                   \\u5e74\\u6708\\u65e5", "_record_id": "5b7b350bed3cfd55d434f4158282c310", "src_type": "system", "type": "\\u6743\\u76ca\\u53d8\\u66f4", "abstract": "\\u8bc1\\u5238\\u4ee3\\u7801\\uff1a601139       \\u8bc1\\u5238\\u7b80\\u79f0\\uff1a\\u6df1\\u5733\\u71c3\\u6c14       \\u516c\\u544a\\u7f16\\u53f7\\uff1a2016-025\\u53f7\\r\\n                    \\u6df1\\u5733\\u5e02\\u71c3\\u6c14\\u96c6\\u56e2\\u80a1\\u4efd\\u6709\\u9650\\u516c\\u53f8\\r\\n     \\u516c\\u5f00\\u53d1\\u884c2016\\u5e74\\u516c\\u53f8\\u503a\\u5238\\uff08\\u7b2c\\u4e00\\u671f\\uff09\\u53d1\\u884c\\u7ed3\\u679c\\u516c\\u544a\\r\\n    \\u53d1\\u884c\\u4eba\\u53ca\\u8463\\u4e8b\\u4f1a\\u5168\\u4f53\\u6210\\u5458\\u4fdd\\u8bc1\\u672c\\u516c\\u544a\\u5185\\u5bb9\\u4e0d\\u5b58\\u5728\\u4efb\\u4f55\\u865a\\u5047\\u8bb0\\u8f7d\\u3001\\u8bef\\u5bfc\\u6027\\u9648\\u8ff0\\u6216\\u8005\\u91cd\\u5927\\u9057\\u6f0f\\uff0c\\u5e76\\u5bf9\\u5176\\u5185\\u5bb9\\u7684\\u771f\\u5b9e\\u6027\\u3001\\u51c6\\u786e\\u6027\\u548c\\u5b8c\\u6574\\u6027\\u627f\\u62c5\\u4e2a\\u522b\\u53ca\\u8fde\\u5e26\\u8d23\\u4efb\\u3002\\r\\n    \\u7ecf\\u4e2d\\u56fd\\u8bc1\\u5238\\u76d1\\u7763\\u7ba1\\u7406\\u59d4\\u5458\\u4f1a\\u8bc1\\u76d1\\u8bb8\\u53ef\\u30102016\\u30111284\\u53f7\\u6587\\u6838\\u51c6\\uff0c\\u6df1\\u5733\\u5e02\\u71c3\\u6c14\\u96c6\\u56e2\\u80a1\\u4efd\\u6709\\u9650\\u516c\\u53f8\\uff08\\u4ee5\\u4e0b\\u7b80\\u79f0\\u201c\\u53d1\\u884c\\u4eba\\u201d\\uff09\\u83b7\\u51c6\\u9762\\u5411\\u516c\\u4f17\\u6295\\u8d44"}', 
        entity_source = EntitySource(
            domain = None, 
            url = u'http://data.eastmoney.com/notice/20160714/2Wvl2atfIiNEWu.html', 
            download_time = None, 
            site_id = 111, 
            site = None, 
            src_type = None, 
            url_id = None, 
            domain_id = None
        ), 
        topic_id = 36
    )

data_ktgg = EntityExtractorInfo(
        update_time=None, 
        entity_data='{"litigants": ["\\u738b\\u51ef", "\\u674e\\u8273\\u6770"], "court_place": "\\u7b2c\\u5341\\u4e09\\u6cd5\\u5ead", "is_cancel": "\\u672a\\u53d6\\u6d88", "bulletin_date": "2016-07-08", "judge": "", "provice": "\\u5317\\u4eac", "defendants": ["\\u674e\\u8273\\u6770"], "case_cause": "\\u6c11\\u95f4\\u501f\\u8d37\\u7ea0\\u7eb7", "court": "\\u5317\\u4eac\\u5e02\\u5bc6\\u4e91\\u533a\\u4eba\\u6c11\\u6cd5\\u9662", "court_time": "2016-07-08 14:00:00", "case_id": "", "_record_id": "", "plaintiffs": ["\\u738b\\u51ef"], "_id": "123", "court_use": ""}', 
        entity_source=None, 
        topic_id=None
    )
def control():
    client,transport=getclient()
    print client.check_data(data_ssgg)
    closetransport(transport)


if __name__ == '__main__':
    control()
