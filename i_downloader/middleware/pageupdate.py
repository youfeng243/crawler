# -*- coding: utf-8 -*-
import json
import re
html_pattern = """
<html xmlns='http://www.w3.org/1999/xhtml'>
<head></head
><body>
<div class='Title'>
{}</div><div class='PubDate'>
{}</div><div class='Html'>
{}</div></div></div></body></html>
"""
wenshu_regx = re.compile(ur'jsonHtmlData\s*=\s*"([\s\S]*})";', re.I | re.S)


class PageupdateMiddleware(object):
    def __init__(self, conf):
        self.conf = conf
        self.logger = conf.get('log')
    def process_request(self, request):
        return

    def process_response(self, request, response):


        if response==None:
            return response
        if response.content==None:
            return response
        html = response.content
        if response==None:
            return response
        elif request.url.find("http://www.neeq.com.cn/disclosureInfoController/infoResult.do")>=0:
            try:
                result=html.split('txtContent')[1].replace('\/','/').replace('---------------------------------------------------------------------------------','</pre><pre>')[3:-4]
                index=result.find('</br></br>')
                result_text='<pre>'+result[index+4:]
                request.contenta=result_text
                self.logger.info('page\ttranstion\tsuccess\turl:{}'.format(request.url))
                return response
            except Exception as e:
                self.logger.error('page\ttranstion\tfail\turl:{}'.format(request.url))
                return response

        elif request.url.find("wenshu.court.gov.cn/CreateContentJS/CreateContentJS.aspx")>=0:
            try:
                obj = eval(wenshu_regx.findall(html)[0].replace("\\\"", "\""))
                html = html_pattern.format(obj.get('Title', ''), obj.get('PubDate', ''),
                                           obj.get('Html'))
                request.contenta=html
                self.logger.error('page\ttranstion\tsuccess\turl:{}'.format(request.url))
            except Exception as e:
                self.logger.error('page\ttranstion\tfail\turl:{} by {}'.format(request.url, e.message))
        else:
            return response
        return response

if __name__ == "__main__":
    pass