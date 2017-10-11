# coding:utf-8
from plugins.gsxx_base_info.gsxx_base_info_plugin import extract

data = {
    "base_info": [
        {
            "key": "注册号",
            "value": "005472"
        },
        {
            "key": "企业名称",
            "value": "上海一品轩食品有限公司奉贤路分店"
        },
        {
            "key": "类型",
            "value": "外商投资企业分公司"
        },
        {
            "key": "负责人",
            "value": "陈锡奎"
        },
        {
            "key": "营业期限自",
            "value": "2002年12月24日"
        },
        {
            "key": "营业期限至",
            "value": "2012年9月25日"
        },
        {
            "key": "登记机关",
            "value": "上海市工商局"
        },
        {
            "key": "核准日期",
            "value": "2002年12月24日"
        },
        {
            "key": "成立日期",
            "value": "2002年12月24日"
        },
        {
            "key": "登记状态",
            "value": "注销"
        },
        {
            "key": "注销日期",
            "value": "2007年4月2日"
        },
        {
            "key": "",
            "value": ""
        },
        {
            "key": "营业场所",
            "value": "上海市静安区奉贤路63号"
        },
        {
            "key": "",
            "value": ""
        },
        {
            "key": "经营范围",
            "value": "销售母公司生产的西点，月饼，面、米食品，休闲食品，乳制品，卤制品，非碳酸饮料（涉及许可经营的凭许可证经营）。"
        }
    ],
    "changerecords": [],
    "contributor_information": [],
    "province": "上海"
}
import json
print json.dumps(extract("", "", data), ensure_ascii=False)