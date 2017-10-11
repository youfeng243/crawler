#!/usr/bin/env python
# -*- coding:utf-8 -*-
from i_extractor.libs.parser_tool import unescape


class Domtree2Json(object):
    @classmethod
    def convert(cls, dom):
        if dom is None or not len(dom):
            return None
        if isinstance(dom, list):
            dom = dom[0]
        if dom.tag in ['html', "body", "root"]:
            dom = dom.xpath('//root')[0]
        return cls._convert(dom[0])
    @classmethod
    def _convert(cls, dom):
        result = None
        if dom.tag == "object":
            result = {}
            for child in dom.iterchildren():
                if child.tag == "key":
                    result[unescape(child.get('value'))] = cls._convert(child[0])
        elif dom.tag == "array":
            result = []
            for child in dom.iterchildren():
                result.append(cls._convert(child))
        elif dom.tag == "number":
            result = float(dom.text)
        elif dom.tag == "string":
            if dom.get('value') == "null":
                result = None
            else:
                result = etree.tostring(dom, encoding='utf-8')
                result = result[8:-9]
        elif dom.tag == "bool":
            text = dom.text
            if text == "true":
                result = True
            elif text == "false":
                result = False
        return result

class Json2HTML(object):
    @classmethod
    def _escapce(cls, string):
        return string.replace("<", "&lt;").replace(">", "&gt;")
    @classmethod
    def convert(cls, context):
        return "".join(["<root>", cls._convert(context), "</root>"])
    @classmethod
    def _convert(cls, context):
        if isinstance(context, bool):
            if context:
                return "<bool>true</bool>"
            else:
                return "<bool>false</bool>"
        if isinstance(context, int) or isinstance(context, long):
            return "<number>%s</number>" % str(context)
        elif isinstance(context, float):
            return "<number>%s</number>" %str(context)
        elif isinstance(context, basestring):
            return "<string>%s</string>" % (context)
        elif context == None:
            return "<string value='null'>null</string>"
        elif isinstance(context, dict):
            prefix = "<object>"
            l = [prefix]
            for k, v in context.items():
                l.append("<key value='%s'>" % cls._escapce(k))
                l.append(cls._convert(v))
                l.append("</key>")
            l.append("</object>")
            return "".join(l)
        elif isinstance(context, list):
            l = ['<array>']
            for v in context:
                l.append(cls._convert(v))
            l.append("</array>")
            return "".join(l)



if __name__ == "__main__":
    import json

    ss = """{"content":{"location":{"lat":23.009415,"lng":114.712598},"locid":"09c593e2e705693ee7c560f3d18feef4","radius":30,"confidence":1.0,"address_component":{"country":"中国","province":"广东省","city":"惠州市","district":"惠东县","street":"洪达路","street_number":"21","admin_area_code":441323},"formatted_address":"广东省惠州市惠东县洪达路21","pois":[{"name":"惠东县爱丁堡第七幼儿园","address":"惠州市惠东县","tag":"教育培训;幼儿园","location":{"lat":23.00842,"lng":114.71201},"uid":"16205232707480866915"},{"name":"中国建设银行","address":"惠东大道2419","tag":"金融;银行","location":{"lat":23.007582,"lng":114.710999},"uid":"18011547694874492927"},{"name":"中国工商银行","address":"大岭镇大岭镇惠东大道2425号","tag":"金融;银行","location":{"lat":23.007939,"lng":114.71055},"uid":"9717982221031825417"},{"name":"富汇大厦","address":"惠东县其他惠东大道","tag":"房地产","location":{"lat":23.007715,"lng":114.710838},"uid":"11765647749831432341"},{"name":"金华泰傢俬城","address":"惠东大道2419号富汇大厦附近","tag":"购物;家居建材","location":{"lat":23.007556,"lng":114.711089},"uid":"11492263619194798048"},{"name":"中国农业银行","address":"大岭镇惠东大道2294号","tag":"金融;银行","location":{"lat":23.005934,"lng":114.712442},"uid":"17953034074046070783"},{"name":"爱尚百货","address":"广东省惠州市惠东县惠东大道","tag":"购物;购物中心","location":{"lat":23.006417,"lng":114.711458},"uid":"11246259647176139418"},{"name":"东进实验学校","address":"惠东县东进大道33号","tag":"教育培训;中学","location":{"lat":23.012604,"lng":114.710469},"uid":"18011547656219787263"},{"name":"惠东县公安局大岭派出所","address":"东进大道10","tag":"政府机构;公检法机构","location":{"lat":23.0107,"lng":114.709463},"uid":"13209584766501343863"},{"name":"大岭友好","address":"小吃街9号","tag":"医疗;诊所","location":{"lat":23.005617,"lng":114.712127},"uid":"3758436716246173753"}],"location_description":"惠东县爱丁堡第七幼儿园东北136米"},"result":{"error":161,"loc_time":"2016-09-26 16:56:54"}}"""
    jsonobj = json.loads(ss)
    xx = Json2HTML.convert(jsonobj)
    from lxml import etree
    doc = etree.HTML(xx)

    #from html_extractor import etree2string
    #print etree2string(doc, unecapse_html=True)
    print json.dumps(Domtree2Json.convert(doc))