#!/usr/bin/env python
# coding:utf-8
from libs.plugin_handler import PluginHandler
ph = PluginHandler()
for module_info in ph.get_plugin_module_info("example_plugin;gsxx_base_info"):
    ph.load_plugin_module(module_info)
for module_info in ph.get_plugin_module_info("example_plugin;gsxx_base_info"):
    ph.load_plugin_module(module_info)

data = {
    "base_info": [
        {
            "key": u"注册号",
            "value": u"005472"
        },
        {
            "key": u"企业名称",
            "value": u"上海一品轩食品有限公司奉贤路分店"
        },
        {
            "key": u"类型",
            "value": u"外商投资企业分公司"
        },
        {
            "key": u"负责人",
            "value": u"陈锡奎"
        },
        {
            "key": u"营业期限自",
            "value": u"2002年12月24日"
        },
        {
            "key": u"营业期限至",
            "value": u"2012年9月25日"
        },
        {
            "key": u"登记机关",
            "value": u"上海市工商局"
        },
        {
            "key": u"核准日期",
            "value": u"2002年12月24日"
        },
        {
            "key": u"成立日期",
            "value": u"2002年12月24日"
        },
        {
            "key": u"登记状态",
            "value": u"注销"
        },
        {
            "key": u"注销日期",
            "value": u"2007年4月2日"
        },
        {
            "key": "",
            "value": ""
        },
        {
            "key": u"营业场所",
            "value": u"上海市静安区奉贤路63号"
        },
        {
            "key": "",
            "value": ""
        },
        {
            "key": u"经营范围",
            "value": u"销售母公司生产的西点，月饼，面、米食品，休闲食品，乳制品，卤制品，非碳酸饮料（涉及许可经营的凭许可证经营）。"
        }
    ],
    "changerecords": [],
    "contributor_information": [],
    "province": "上海"
}

for module_info in ph.get_plugin_module_info("example_plugin;gsxx_base_info"):
    extract = ph.get_plugin_entry(module_info)
    print extract("http://baidu.com", "<html><head></head><body></body></html>", {"data":data, "links":[]})


