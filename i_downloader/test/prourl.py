# -*- coding: utf-8 -*-
province={
#浙江
'zhejiang':{
    'kw':{
        'refer_url': 'http://%s/common/captcha/doReadKaptcha.do?%s',
        'check_body': u"/businessPublicity.jspx?id=",
        'identifying_code_url': 'http://www.ahcredit.gov.cn/validateCode.jspx',
        'session_msg': {

        },
        },
    'url':'',
    'post_data':{},
    'searchname':''
},
#内蒙古
'neimenggu':{
    'kw':{
        'refer_url': 'http://%s/common/captcha/doReadKaptcha.do?%s',
        'check_body': u"/businessPublicity.jspx?id=",
        'identifying_code_url': 'http://www.nmgs.gov.cn:7001/aiccips/verify.html',
        'need_identifying': True,
        'identifying_code_check_url': 'http://www.nmgs.gov.cn:7001/aiccips/CheckEntContext/checkCode.html',
        'session_msg': {

        },
    },
    'url':'http://www.nmgs.gov.cn:7001/aiccips/CheckEntContext/showInfo.html',
    'post_data':{},
    'searchname':''
},
#北京
'beijing':{

},
#江西
'jiangxi':{

},
#辽宁
'liaoning':{

},
#江苏
'jiangsu':{

},
#广东
'guangdong':{

},
#甘肃
'gansu':{

},
#吉林
'jilin':{

},
#安徽
'anhui':{
    'kw':{
        'refer_url': 'http://www.ahcredit.gov.cn/search.jspx',
        'check_body': u"/businessPublicity.jspx?id=",
        'identifying_code_url': 'http://www.ahcredit.gov.cn/validateCode.jspx',
        'session_msg': {

            },
    },
    'url':'http://www.ahcredit.gov.cn/queryListData.jspx',
    'post_data':{'checkNo': '',},
    'searchname':'entName'
},
#湖北
'hubei':{
    'kw':{
        'refer_url': 'http://xyjg.egs.gov.cn/ECPS_HB/search.jspx',
        'check_body': u"/businessPublicity.jspx?id=",
        'identifying_code_url': 'http://xyjg.egs.gov.cn/ECPS_HB/validateCode.jspx',
        'session_msg': {

            },
    },
    'url':'http://xyjg.egs.gov.cn/ECPS_HB/searchList.jspx',
    'post_data':{'checkNo': '',},
    'searchname':'entName'
},
#青海
'qinghai':{
    'kw':{
        'refer_url': 'http://218.95.241.36:83/search.jspx',
        'check_body': u"/businessPublicity.jspx?id=",
        'identifying_code_url': 'http://218.95.241.36:83/validateCode.jspx',
        'session_msg': {

            },
    },
    'url':'http://218.95.241.36:83/searchList.jspx',
    'post_data':{'checkNo': '',},
    'searchname':'entName'
},
'hubeihz':{
    'kw':{
        'refer_url': 'http://xyjg.egs.gov.cn/ECPS_HB/search.jspx',
        'check_body': u"/businessPublicity.jspx?id=",
        'identifying_code_url': 'http://xyjg.egs.gov.cn/ECPS_HB/validateCode.jspx',
        'session_msg': {

            },
    },
    'url':'http://xyjg.egs.gov.cn/ECPS_HB/searchList.jspx?HZPOST=eyJjaGVja05vIjogIiIsICJlbnROYW1lIjogIlx1NmUyOVx1NmNjOVx1NmM0OVx1NTUxMFx1N2E3%0AYVx1OTVmNFx1ODhjNVx1OTk3MFx1OGJiZVx1OGJhMVx1OTBlOCJ9%0A',
    'post_data':{'checkNo': '','entName':''},
    'searchname':'entName'
},
#贵州
'guizhou':{
    'url':'http://www.gzcredit.gov.cn/Service/CreditService.asmx/searchDetail',
    'post_data':{'condition/qymc':'贵阳建筑勘察设计有限公司','condition/cydw':''}
},

#海南
'hainan':{
    'kw':{
        'refer_url': 'http://aic.hainan.gov.cn:1888/search.jspx',
        'check_body': u"/businessPublicity.jspx?id=",
        'identifying_code_url': 'http://aic.hainan.gov.cn:1888/validateCode.jspx?type=0&id=0.23107510153204203',
        'session_msg': {

            },
    },
    'url':'http://aic.hainan.gov.cn:1888/searchList.jspx',
    'post_data':{'checkNo': '',},
    'searchname':'entName'
},
#西藏
'xizang':{
    'kw':{
        'refer_url': 'http://gsxt.xzaic.gov.cn/search.jspx',
        'check_body': u"/businessPublicity.jspx?id=",
        'identifying_code_url': 'http://gsxt.xzaic.gov.cn/validateCode.jspx?type=0&id=0.24700427010937875',
        'session_msg': {

            },
    },
    'url':'http://gsxt.xzaic.gov.cn/searchList.jspx',
    'post_data':{'checkNo': '',},
    'searchname':'entName'
},
#福建
'fujian':{
    'kw':{
        'refer_url':'http://wsgs.fjaic.gov.cn/creditpub/home',
        'identifying_code_url':'',
        'identifying_code_check_url':'',
        'session_msg':{
               'session.token':'session.token'
        },
    },
    'url':'http://wsgs.fjaic.gov.cn/creditpub/search/ent_info_list',
    'post_data':{
        'searchType': '1',
        'captcha': ''},
    'searchname':'condition.keyword'
},
#河北
'hebei':{
    'kw':{
        'refer_url':'http://www.hebscztxyxx.gov.cn/notice/home',
        'identifying_code_url':'',
        'identifying_code_check_url':'',
        'session_msg':{
               'session.token':'session.token'
        },
    },
    'url':'http://www.hebscztxyxx.gov.cn/notice/search/ent_info_list',
    'post_data':{
        'searchType': '1',
        'captcha': ''},
    'searchname':'condition.keyword'
},
#上海
'shanghai':{
    'kw':{
        'refer_url':'http://www.sgs.gov.cn/notice/home',
        'identifying_code_url':'',
        'identifying_code_check_url':'',
        'session_msg':{
               'session.token':'session.token'
        },
    },
    'url':'http://www.sgs.gov.cn/notice/search/ent_info_list',
    'post_data':{
        'searchType': '1',
        'captcha': ''},
    'searchname':'condition.keyword'
},
#云南
'yunnan':{
    'kw':{
        'refer_url':'http://gsxt.ynaic.gov.cn/notice/',
        'session_msg':{
               'session.token':'session.token'
        },
    },
    'url':'http://gsxt.ynaic.gov.cn/notice/search/ent_info_list',
    'post_data':{
        'searchType': '1',
        'captcha': ''},
    'searchname':'condition.keyword'
},
#宁夏
'ningxia':{
    'kw':{
        'refer_url':'http://gsxt.ngsh.gov.cn/ECPS/index.jsp',
        'identifying_code_url':'http://gsxt.ngsh.gov.cn/ECPS/verificationCode.jsp?_=3232',
        'need_identifying':True,
        'identifying_code_check_url':'http://gsxt.ngsh.gov.cn/ECPS/qyxxgsAction_checkVerificationCode.action',
        'session_msg':{
        },
    },
    'url':'http://gsxt.ngsh.gov.cn/ECPS/qyxxgsAction_queryXyxx.action',
    'post_data':{
        'selectValue': '百度',
        'isEntRecord': '', 'loginInfo.regno': '', 'loginInfo.entname': '',
        # 'loginInfo.idNo': '', 'loginInfo.mobile': '', 'loginInfo.password': '',
        # 'loginInfo.verificationCode': '', 'otherLoginInfo.name': '', 'otherLoginInfo.password': '',
        # 'otherLoginInfo.verificationCode': ''
    },
    'searchname': 'selectValue',
    'need_identifying':True
},
#山东
'shandong':{
    'kw':{
        'refer_url':'http://218.57.139.24',
        'check_body':"enckeyword=\\",
        'identifying_code_url':'http://218.57.139.24/securitycode?12',
        'session_msg':{
            '_csrf':'_csrf'
        },
    },
    'url':'',
    'post_data':{
    },
    'searchname': 'selectValue',
    'need_identifying':True
},
#财报详情
'caibao':{
    'url':'http://data.eastmoney.com/notice_n/20160622/JTgr1mMAvXANPH4ELpJmgr.html',
},
#财报种子
'caibaoseed':{
    'url':'http://data.eastmoney.com/Notice_n/Noticelist.aspx?type=&market=hk&code=01224&date=&page=2',
},
#广州详情
'guangzhoubid':{
    'url': 'http://www.gzg2b.gov.cn/Sites/_Layouts/ApplicationPages/News/NewsDetail.aspx?st=news&id=77E3B481-D501-4F41-A171-38027C33426F',
},
#广州种子
'guangzhouseedbid':{
    'url': 'http://www.gzg2b.gov.cn/Sites/_Layouts/ApplicationPages/News/News.aspx?ColumnName=公示公告',
    'post_data': {"currentPage": "2"},
},
#东莞详情
'dongguanbid':{
    'url': 'http://www.gzg2b.gov.cn/Sites/_Layouts/ApplicationPages/News/News.aspx?ColumnName=公示公告',
    'post_data': {"currentPage": "2"},
},
#东莞种子
'dongguanseedbid':{
    'url':'https://www.dgzb.com.cn/ggzy/website/WebPagesManagement/findListByPage?fcInfotype=1&tenderkind=A&projecttendersite=SS',
    'post_data': {"currentPage": "2"}
},
'hubo':{
    'url':'http://api.11315.com/webApp/index/newAdd/1/10/?&token=f7e83f0ffe5f74968c272ef16d1abc23cc106985f045f748&version=i_1.4.0'
},
'hanzi':{
    'url':'http://wenshu.court.gov.cn/List/ListContent?HZPOST=eyJEaXJlY3Rpb24iOiAiYXNjIiwgIkluZGV4IjogIjEiLCAiT3JkZXIiOiAiXHU2Y2Q1XHU5NjYy%0AXH U1YzQyXHU3ZWE3IiwgIlBhZ2UiOiAxMiwgInBhcmFtIjogIlx1NGUwYVx1NGYyMFx1NjVlNVx1%0ANjcxZjoyMDE2LTEwLTA4IFRPIDIwMTYtMTAtMDksXHU2ODQ4XHU0ZWY2XHU3YzdiXHU1NzhiOlx1%0ANmMxMVx1NGU4Ylx1Njg0OFx1NGVmNixcdTZjZDVcdTk2NjJcdTU3MzBcdTU3ZGY6XHU1MzE3XHU0%0AZWFjXHU1ZTAyIn0%3D%0A'
},
'zhaoshanxi':{
    'url':'http://www.sxszbb.com/sxztb/jyxx/001001/MoreInfo.aspx?CategoryNum=001001&HZPOST=eyJNb3JlSW5mb0xpc3QxJHR4dFRpdGxlIjogIiIsICJfX0VWRU5UQVJHVU1FTlQiOiAzMiwgIl9f%0ARVZFTlRUQVJHRVQiOiAiTW9yZUluZm9MaXN0MSRQYWdlciIsICJfX0VWRU5UVkFMSURBVElPTiI6%0AICIvd0VXQXdLWHplS1NEQUwxNlAyUkF3TGt1cjM0QmFNTTAzdlhIZHVTZXZzaVlscDRBb3VManlP%0AaSIsICJfX1ZJRVdTVEFURSI6ICIvd0VQRHdVS0xUVTRNelV6TmpnNU5BOWtGZ0lDQVE5a0ZnSm1E%0AdzhXQmg0TFltZERiR0Z6YzA1aGJXVUZDRTFwWkdSc1pVSm5IZ3RqWVhSbFoyOXllVTUxYlFVR01E%0AQXhNREF4SGdaemFYUmxhV1FDQVdRV0FnSUNEMlFXQWdJRkQyUVdBbVlQWkJZQ1pnOFBGZ2dlQzFK%0AbFkyOXlaR052ZFc1MEF1RW1IaEJEZFhKeVpXNTBVR0ZuWlVsdVpHVjRBZ0llRGtOMWMzUnZiVWx1%0AWm05VVpYaDBCWk1CNks2dzViMlY1b0M3NXBXdzc3eWFQR1p2Ym5RZ1kyOXNiM0k5SW1Kc2RXVWlQ%0AanhpUGpRNU5qRThMMkkrUEM5bWIyNTBQaURtZ0x2cG9iWG1sYkR2dkpvOFptOXVkQ0JqYjJ4dmNq%0AMGlZbXgxWlNJK1BHSStNalE1UEM5aVBqd3ZabTl1ZEQ0ZzViMlQ1WW1ONmFHMTc3eWFQR1p2Ym5R%0AZ1kyOXNiM0k5SW5KbFpDSStQR0krTWp3dllqNDhMMlp2Ym5RK0hnbEpiV0ZuWlZCaGRHZ0ZFeTl6%0AZUhwMFlpOXBiV0ZuWlhNdmNHRm5aUzlrWkdSSmVKNzc3OWRBSXh1bFpFTS9YS1F4bGJod1R3PT0i%0AfQ%3D%3D%0A'
},
'xing':{
    'url':'http://gsxt.cqgs.gov.cn/search_ent?entId=500902010100005761&id=500902200001532&type=4540'
},
'ziqiang':{
    'url':"http://www.innotree.cn/ajax/projectrank/2/getFilterProjects?page=3&size=15&industry=&period=&area=&keyword=&sort="
},
'gudong':{
    'url':'http://www.neeq.com.cn/disclosureInfoController/infoResult.do?callback=jQuery183026975088430624217_1478489832844&nodeType=0&disclosureType=7&publishDate=2016-11-03&_=1478490039414'
},
'bug':{
    'url':'http://finance.ifeng.com/a/20161105/14987292_0.shtml'
},
'tianyancha':{
    'url':'http://api.tianyancha.com/services/v3/t/details/wapCompany/239462893'
},
'buga':{
    'url':"http://www.baidu.com/s?wd=%B5%A4%D1%F4%CA%D0%BA%EC%C0%DD%CB%AE%C4%E0%D6%C6%D4%EC%D3%D0%CF%DE%B9%AB%CB%BE",
}

}
