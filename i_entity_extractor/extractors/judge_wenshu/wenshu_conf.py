# coding=utf8

plaintiff_keyword_list = [
    '原告',
    '案外人',
    '异议人',
    '自诉人',
    '上诉人',
    '申诉人',
    '起诉人',
    '申请人',
    '申报人',
    '请求人',
    '执行人',
    '复议人',
    '异议人',
    '再审人',
    '请求人',
    '申请单位',
    '执行单位',
    '被害单位',
    '移送部门',
    '公诉机关',
    '抗诉机关',
    '执行机关',
    '利害关系人',
]

defendant_keyword_list = [
    '异议相对人',
    '罪犯',
    '被告',
    '被上诉人',
    '被起诉人',
    '被申诉人',
    '被申请人',
    '被执行人',
    '被请求人',
    '被申请执行人',
    '赔偿义务机关',
    '诉前保全申请人',

]

judiciary_keyword_list = [
    '审判长',
    '审判员',
    '执行员',
    '陪审员',
    '法官助理',
    '书记员',
    '代理审判员',
    '人民陪审员',
    '人民审判员',
    '法官',
    '院长',
    '见习书记员'
    '代书记员'
    '助理',
    '速录员'
]

judge_keyword_list = [
    '裁定如下',
    '判决如下',
]
sep_keyword_list = [
    "纠纷", "称", "指控", "判决", "本院"
]

company_end_list = [
    '公司',
    '研究所',
    '商城',
    '商贸城',
    '园艺场',
    '设计院',
    '卫生院',
    '研究院',
    '工程处',
    '商行',
    '厂',
    '中心',
    '渔场',
    '门诊部',
    '检察院',
]

company_type_map = {
    "银行": "银行",
    "储蓄": "银行",
    "信用社": "银行",
    "合作联社": "银行",
    "信用合作社": "银行",
    "分行": "银行",
    "支行": "银行",
    "借贷": "借贷公司",
    "贷款": "借贷公司",
    "小贷": "借贷公司",
    "信贷": "借贷公司",
    "p2p": "借贷公司",
    "借款": "借贷公司",
    "速贷": "借贷公司",
    "微贷": "借贷公司",
    "贷": "借贷公司",
    "证券": "金融公司",
    "保险": "金融公司",
    "信托": "金融公司",
    "投资": "金融公司",
    "创投": "金融公司",
    "基金": "金融公司",
    "风投": "金融公司",
    "租赁": "金融公司",
    "融资": "金融公司",
    "资产": "金融公司",
    "财产": "金融公司",
    "法院": "政府组织",
    "卫生院": "政府组织",
    "政府": "政府组织",
    "委员会": "政府组织",
    "市委": "政府组织",
    "村委": "政府组织",
    "代表大会": "政府组织",
    "检察院": "政府组织",
    "司法": "政府组织",
    "税务": "政府组织",
    "局": "政府组织",
    "老人院": "政府组织",
    "敬老院": "政府组织",
    "警察": "政府组织",
    "监狱": "政府组织",
    "学校": "政府组织",
    "大学": "政府组织",
    "中学": "政府组织",
    "高中": "政府组织",
    "小学": "政府组织",
    "幼儿园": "政府组织",
    "交警": "政府组织",
    "房地产": "地产物业公司",
    "地产": "地产物业公司",
    "物业": "地产物业公司",
    "建筑": "地产物业公司",
    "建设": "地产物业公司",
    "中介": "中介经纪公司",
    "经纪": "中介经纪公司",
    "代理": "中介经纪公司",
    "科技": "科技公司",
    "软件": "科技公司",
    "信息": "科技公司",
    "通信": "科技公司",
    "硬件": "科技公司",
    "智能": "科技公司",
    "视频": "科技公司",
    "网络": "科技公司",
    "公司": "其他非金融公司",
    "厂": "其他非金融公司",
    "集团": "其他非金融公司",
    "医院": "其他非金融公司",
    "店": "其他非金融公司",
    '批发商行': '其他非金融公司',
    '商贸城': '商贸公司'
}

cause_last_keys = [
        '再审',
        '民事裁定书',
        '一审',
        '二审',
        '行政判决书',
        '行政裁定书',
        '执行裁定书',
        '裁定书',
        '判决书',
        '一案',
        '抗诉',
        '民事裁定书',
        '民事判决书',
        '刑事裁定书',
        '普通执行终结本次',
        '申请保全案件'
    ]

cause_first_keys = [
        '有限公司',
        '有限责任公司',
        '人民法院',
        '与',
        '等',
        '公安局',
        '委员会',
        '新闻出版局',
        '商贸',
        '之间',
        '工程管理站',
        '社会保障局',
        '社保局'

    ]

procedure_type_map = {
    # u'二审': [u'驳回上诉', u'二审', u'上诉人', u'撤回上诉', u'再审申请', u'再审民事裁定书', u'再审'],
    u'复核': [u'复核', u'核准'],
    u'刑罚变更': [u'有悔改表现', u'减刑']
}

first_replace_str_list = [u'（原审原告人）', u'（原审被告人）', u'(原审原告人)', u'(原审被告人)', u'(原审被告)', u'（原审被告）',
                          u'（原审原告）', u'(原审原告)', u'（原审第三人）', u'(原审第三人)', u'（一审被告、二审上诉人）',
                          u'（一审原告、二审被上诉人）']
replace_str_list = [u'.', u'。', u'：', u':', ')', '）', '(', '（', '\t']

replace_space_str = [u',', u'，', u'。', u'；', u';', '\t', u'（绰号', u'系被告', u'系原告', u'曾用名', u'（又名', u'（外号']

replace_space_str1 = [u'劳务合同', u'买卖合同', u'金融借款合同']

wrong_str_list = [
    u'通知书',
    u'委托',
    u'原告',
    u'被告',
    u'可供执行的财产',
    u'财产可供执行',
    u'上述'

]

noWords = u"院被告请人月街了复的街鉴电证醇简检调"
append_end = u'[之一二三四五六七八九十附1-9号-]*'
start_word = u'(?:判决书|裁定书|裁决书|决定书)([^'
province_bref = u'[京浙津皖沪闽渝赣港鲁澳豫蒙鄂新湘宁粤藏琼锡桂川蜀冀贵黔晋云辽吉陕甘黑青苏台内初民执终刑更]'
form = u'[京浙津皖沪闽渝赣港鲁澳豫蒙鄂新湘宁粤藏琼桂川蜀冀贵黔晋云辽吉陕甘黑青苏台内初民执终刑更字]'
idfilterword = [u'的', u'，', u',', u'。', u'已经', u'法律效力', u'发生', u'抵押']
case_id_unser_str = [u'再审', u'支付令', u'附$']

case_ref_pattern = [
    start_word + noWords + u']*?[初民执字终复][^' + noWords + u']*?号' + append_end + u')',
    start_word + noWords + u']{4,30})(?:原告|上诉|再审|复议)',
    start_word + noWords + u']{4,30})(?:申请|异议人)',
    start_word + noWords + u']{4,30}号' + append_end + u')',
    u'((?:19|20)\d{2}[^\d][^' + noWords + u']*?[初民执字终复][^' + noWords + u']*?号' + append_end + u')',
    u'((?:19|20)\d{2}[^d]\)?' + form + u'[^' + noWords + u']{3,20}号' + append_end + u')',
    u'((?:19|20)\d{2}[^\d][^' + noWords + u']*?[初民执字终复][^' + noWords + u']*?第\d+号?)民事判决'
]
case_id_split = u'，|。|：|,|;|\u201d|一案|受案|一个|并入|合同|作出|月|中心|通知|人民|与|及|、[^\d]|和[(（\d]'

judge_pattern = [
    u'(?:判决如下:?|裁定如下:?|决定如下:?)(.*?)(?:审判[长员]|执行[长员]|代理审|[此本]页无正文)',
    u'协议:(.*?)(代理)?审判员',
    u'(?:判决如下:?|裁定如下:?|决定如下:?)(.*?)(?:负担|生效)',
    u'(?:判决如下:?|裁定如下:?)(.*?维持原判)',
    u'(驳回上诉,维持原判)',
    u'综上,本院认为[,:](.*?)特此通知',
    u'规定,(责?令你.*?)特此通知',
    u'(?:判决内容|判决结果|规定:)(.*?).(?:执行员|审判员)',
    u'[(依照)(按照)(根据)].*?规定[:,](.*?[(效力)(结案)]?).?(?:审判员|二〇一)',
    u'(不在互联网公布)',
    u'(?:判决如下:?|裁定如下:?)(.+)',
    u'通知如下:?(.*?)二〇.*?日'
]


def comp(x, y):
    if len(x) > len(y):
        return 1
    elif len(x) < len(y):
        return -1
    else:
        if x > y:
            return 1
        elif x < y:
            return -1
        else:
            return 0
