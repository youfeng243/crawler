# -*- coding: utf-8 -*-
import json
import re
import traceback
from StringIO import StringIO

import requests
from lxml import etree

from i_downloader.util.get_config import config_parse

host='101.201.102.37'
port=9301
class SessionCommitMiddleware(object):
    def __init__(self, conf):
        self.conf = conf
        self.logger = conf.get('log')

    def process_request(self, request):
        for i in range(3):
            session_commit=request.session_commit
            if session_commit==None:
                return
            refer_url=session_commit.refer_url
            identifying_code_url=session_commit.identifying_code_url
            identifying_code_check_url=session_commit.identifying_code_check_url
            need_identifying =session_commit.need_identifying
            s = requests.Session()
            s.headers['User-Agent'] = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
            #获取token
            if refer_url!=None and refer_url!="":
                try:
                    refer_result = s.get(url=refer_url)
                    data = self.parser(refer_result, request.session_commit.session_msg)
                    request.post_data.update(data)
                except Exception as e:
                    self.logger.error("refer_url:%s\tfail"%(refer_url))
                    request.identify_status = 6
                    continue
            #验证码识别
            if identifying_code_url!=None and identifying_code_url!="":
                if need_identifying:
                    try:
                        captcha_result, cap_content= self.get_identifying_code_result(s,request,identifying_code_url)
                    except Exception as e:
                        self.logger.error('identifying_code_url:%s\tfail' % (identifying_code_url))
                        continue
                    post_date={}
                    #由配置文件获取验证码的post参数名
                    identify_name=config_parse(identifying_code_url.split('/')[2],'identify_code')
                    post_date[identify_name]=captcha_result
                    request.post_data.update(post_date)
                    #验证码结果检验
                    if identifying_code_check_url != None and identifying_code_check_url!="":
                        try:
                            result = self.check_captcha(s, identifying_code_check_url,captcha_result, cap_content)
                            if not result:
                                request.identify_status=6
                        except Exception as e:
                            self.logger.error('identifying_code_check_url:%s\tfail' % (identifying_code_check_url))
                            continue
                else:
                    try:
                        a=s.get(url=identifying_code_url)
                    except Exception as e:
                        self.logger.error('identifying_code_url:%s\tfail'%(identifying_code_url))

            request.session=s
            break

    def get_identifying_code_result(self, session, req, identifying_code_url):
        captcha_server_url = "http://{0}:{1}/get_captcha".format(host, port)
        captcha_result = False
        cap_content = ''
        for break_num in xrange(5):
            try:
                r = session.get(identifying_code_url)
                cap_content = r.content
                post_image = {'captcha': StringIO(cap_content)}
                post_data={}

                post_data['province']=config_parse(identifying_code_url.split('/')[2],'province')
                result = requests.post(captcha_server_url, data=post_data, files=post_image)
                result_json = json.loads(result.content)
                status = int(result_json['status'])
                # 识别失败
                if status is not 0:
                    self.logger.info('identify fail')
                    continue
                captcha_result = str(result_json['result'])
                break
            except Exception as e:
                self.logger.error('url:{0} the identify code identify fail the reason is {1}'.format(req.url,e.message))
        return captcha_result, cap_content

    def parser(self,r,kw):
        data={}
        if not kw:
            return data
        for k,v in kw.items():
            try:
                parten='%s.{1,3}:(.*?)\n' % k
                searchObj = re.search(parten, r.text)
                if searchObj!=None:
                    data[v] = searchObj.group().strip().split(':')[1].strip()[1:-1]
                else:
                    xdoc = etree.HTML(r.text.encode("utf-8"))

                    test = xdoc.xpath('//meta[@name="%s" and @content]'%(k))
                    data[v]=test[0].xpath("@content")[0]

            except Exception as e:
                self.logger.error('session_msg parser error %s' % k)
            return data
        return data

        # 通过post检查验证码是否识别正确
    def check_captcha(self, session, url,captcha_result, captcha_file):
        post_data = {'password': captcha_result}
        resp=session.post(url, data=post_data)
        message = None
        try:
            resp_obj = json.loads(resp.text)
            message = resp_obj.get(u'message', None)
        except:
            self.log.error(traceback.format_exc())
        if message == 'ok':

            self.logger.info('url:%s identify success'%url)
            return True
        else:
            self.logger.error('url:%s identify fail'%url)
        return False






