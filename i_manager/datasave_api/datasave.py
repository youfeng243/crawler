#!/usr/bin/env python
# -*- coding:utf-8 -*-
import json
import multiprocessing
import  traceback
import urllib2
from urllib import urlencode

from flask import current_app
from flask import jsonify
from flask import request

from i_util.i_crawler_services import ThriftDataSaver
from . import datasave_api as app


def call_other_server_reload_config(server_info, topic_id):
    try:
        current_app.config['logger'].info("start call server %s" % server_info['server'])
        host, port = server_info['server'].split(':')
        ThriftDataSaver(host=host, port=int(port)).reload(topic_id)
        current_app.config['logger'].info(
            "call datasaver server: %s reload topic success." % (server_info['server']))
    except Exception as e:
        current_app.config['logger'].info(
            "call datasaver server: %s reload topic failed." % (server_info['server']))
        current_app.config['logger'].error(e.message)

@app.route('/reload_topic', methods=['GET'])
def reload_topic():
    try:
        data = []
        data_saver = current_app.config['data_saver']
        args=request.args
        topic_id=int(args.get('topic_id', -1))
        data=data_saver.reload(topic_id)
        _servers = current_app.config['server_manager'].get_server_info('datasaver')
        for server_info in _servers["datasaver"]:
            try:
                if server_info['status'] != 0:
                    continue
                call_process = multiprocessing.Process(target=call_other_server_reload_config,
                                                       args=(server_info, topic_id,))
                call_process.start()

            except Exception as e:
                pass
        return jsonify({'status':True, 'data':str(data)})
    except :
        return jsonify({'status':False, 'error_info':traceback.format_exc()})

@app.route('/get_company', methods=['GET'])
def get_company():
    try:
        args=request.args
        schedule_debug = current_app.config['schedule_debug']
        logger = current_app.config['logger'];
        company_name=args.get('company',"")
        count=args.get('count',20)
        offset=args.get('offset',0)
        #data = schedule_debug.get_company(company_name);
        params = {
            'count':count,
            'type':'name',
            'offset':offset,
            'q':company_name
        }
        url = 'http://101.201.100.58:8888/api/search/es_search?' + urlencode(params)
        opener = urllib2.build_opener()
        opener.addheaders.append(('Cookie', 'PHPSESSID=s4a97r4vgr9usofgrbou2b0j65; session_dev=.eJwdjsGKwjAURX9leGsXbabdFFyMpFMqvBcsKeVlIw6tponZWEQS8d_tuLtwOIf7hOP5Ni0WqvPpukwbOM4jVE_4-oMKWJuZhbGU6gKHznIag5KjR7m7UjpktDLWu4CuL1j7gpOdjeNv1P5hdJ1UwxElRxp-3f_m9FOy4ERy74y-lKuXGXnIleyjcV0wkta-tTR0ntIlxwZLJbkgXeck2pxDm-GwdxgwqqYV5PqStI-m6bfw2sB9mW6f_yDg9Qa3_Eb2.CxLlbw.qwvDPxgyl7lP_IBlTmRAWmKFjT4'))
        data = opener.open(url).read()
        #data = urllib2.urlopen(url).read()
        json_data = json.loads(data).get('data',{})
        result = json_data.get('result', [])
        datas = []
        for info in result:
            for key, value in info.items():
                if 'suggest' in key:
                    info.pop(key)
            datas.append(info)
        total = json_data.get('total_count', 0)
        return jsonify({'status':True, 'data':json_data['result'], 'total':total, 'message':""})
    except:
        return jsonify({'status':False, 'message':traceback.format_exc()})

@app.route('/company_detail', methods=['GET'])
def company_detail():
    schedule_debug = current_app.config['schedule_debug']
    try:
        args=request.args
        status = True
        message = ""
        company_name = args.get('company',"")
        company_type = 0
        url = ""
        json_data = {}
        company_type = args.get('type',"0")
        try:
            company_type = int(company_type)
        except:
            company_type = 0
        if company_type < 1 or company_type > 14:
            data = {}
            status = False
            message = "type must be int(from 1 to 14)"
        else:
            params = {
                            'q':company_name
            }
            name = urlencode(params)[2:]
            if company_type == 1:
                company_type = 'basic'
                url = 'http://101.201.100.58:8888/api/company/' + name + '/' + company_type
            elif company_type == 12:
                company_type = 'risk'
                url = 'http://101.201.100.58:8888/api/company/' + name + '/' + company_type
            elif company_type == 13:
                company_type = 'listing'
                url = 'http://101.201.100.58:8888/api/company/' + name + '/' + company_type
            elif company_type == 7:
                company_type = 'intellectual_property'
                url = 'http://101.201.100.58:8888/api/company/' + name + '/' + company_type
            if url:
                opener = urllib2.build_opener()
                opener.addheaders.append(('Cookie', 'PHPSESSID=s4a97r4vgr9usofgrbou2b0j65; session_dev=.eJwdjsGKwjAURX9leGsXbabdFFyMpFMqvBcsKeVlIw6tponZWEQS8d_tuLtwOIf7hOP5Ni0WqvPpukwbOM4jVE_4-oMKWJuZhbGU6gKHznIag5KjR7m7UjpktDLWu4CuL1j7gpOdjeNv1P5hdJ1UwxElRxp-3f_m9FOy4ERy74y-lKuXGXnIleyjcV0wkta-tTR0ntIlxwZLJbkgXeck2pxDm-GwdxgwqqYV5PqStI-m6bfw2sB9mW6f_yDg9Qa3_Eb2.CxLlbw.qwvDPxgyl7lP_IBlTmRAWmKFjT4'))
                data = opener.open(url).read()
        #data = urllib2.urlopen(url).read()
                #data = urllib2.urlopen(url).read()
                json_data = json.loads(data).get('data',{})
            elif company_type == 14:
                json_data = schedule_debug.get_news(company_name);

        return jsonify({'status':status, 'data':json_data, "message":message})
    except:
        return jsonify({'status':False, 'error_info':traceback.format_exc()})
