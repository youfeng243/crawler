#!/usr/bin/env python
#-*- coding:utf-8 -*-
import datetime
import json
import multiprocessing
import time
import traceback
from copy import deepcopy

from flask import current_app
from flask import jsonify
from flask import request
from werkzeug.contrib.cache import SimpleCache

from bdp.i_crawler.i_downloader.ttypes import DownLoadReq, DownLoadRsp
from i_manager.background.models_sqlalchemy import ParserConfig
from i_util.i_crawler_services import ThriftExtractor
from i_util.tools import get_md5_i64, build_hzpost_url
from . import parser_api as app

page_cache = SimpleCache(threshold=20, default_timeout=5*50)

def call_other_server_reload_config(server_info, parser_id):
    try:
        current_app.config['logger'].info("start call server %s" % server_info['server'])
        host, port = server_info['server'].split(':')
        ThriftExtractor(host=host, port=int(port)).reload_parser_config(str(parser_id))
        current_app.config['logger'].info(
            "call extractor server: %s reload parser_config success." % (server_info['server']))
    except Exception as e:
        current_app.config['logger'].info(
            "call extractor server: %s reload parser_config failed." % (server_info['server']))
        current_app.config['logger'].error(e.message)


def get_page_cache(page_id):
    rv = page_cache.get(page_id)
    return rv


def set_page_cache(page_id, page_content):
    page_cache.set(page_id, page_content, timeout=30 * 60)


@app.route('/entry', methods=['POST'])
def entry():
    post_data = request.form
    page_size = post_data.get('page_size', 20)
    pageno = post_data.get('pageno', 1)
    label = post_data.get('label', '')
    name = post_data.get('name', '')
    urlformat = post_data.get('urlformat', '')
    session = current_app.config['Dsession']()
    try:
        query = session.query(ParserConfig)
        if name == '' and urlformat == '' and label == '':
            datas = query.filter().offset((pageno-1)*page_size).limit(page_size).all()
            count = query.filter().all()

        if name:
            datas = query.filter(ParserConfig.name == name).offset((pageno-1)*page_size).limit(page_size).all()
            count = query.filter(ParserConfig.name == name).all()

        if urlformat:
            datas = query.filter(ParserConfig.url_format == urlformat).offset((pageno-1)*page_size).\
                limit(page_size).all()
            count = query.filter(ParserConfig.url_format == urlformat).all()

        if label:
            datas = query.filter(ParserConfig.label.like('%'+label+'%')).offset((pageno-1)*page_size).\
                limit(page_size).all()
            count = query.filter(ParserConfig.label.like('%'+label+'%')).all()

        result = [{'create_time': data.create_time, 'creator_id': data.creator_id,
                   'datas_rule': json.loads(data.datas_rule), 'datas_type': data.datas_type,
                   'http_method': data.http_method, 'id': data.id, 'label': data.label, 'name': data.name,
                   'next_page_rule': json.loads(data.next_page_rule), 'next_page_type': data.next_page_type,
                   'plugin': data.plugin, 'post_params': data.post_params, 'topic_id': data.topic_id,
                   'update_time': data.update_time, 'url_format': data.url_format, 'urls_rule': data.urls_rule,
                   'weight': data.weight} for data in datas]
    except Exception, e:
        return jsonify({'count': page_size, 'data': result, 'ext': {'totalCounts': len(count), 'pageSize': page_size},
                        'status': False})

    return jsonify({'count': page_size, 'data': result, 'ext': {'totalCounts': len(count), 'pageSize': page_size},
                    'status': True})


@app.route('/save_parser_config', methods=['POST'])
def save_parser_config():
    if not request.json:
        current_app.config['logger'].info('no json')
    extractor = current_app.config['extractor']
    try:
        rsp = extractor.save_parser_config(json.dumps(request.json))
        current_app.config['logger'].info("save_parser_config:"+str(rsp))

        if rsp.get('status') == "True":
            parser_id = json.loads(rsp.get('message', "{}")).get('id', "-1")
            extractor_servers = current_app.config['server_manager'].get_server_info('extractor')
            for server_info in extractor_servers["extractor"]:
                try:
                    if server_info['status'] != 0:
                        continue
                    call_process = multiprocessing.Process(target=call_other_server_reload_config, args=(server_info, parser_id,))
                    call_process.start()

                except Exception as e:
                    pass
    except Exception as e:
        current_app.config['logger'].error(e.message)
        return jsonify({'status':False, 'data':{'error_info':e.message}})
    return jsonify({'status':False if rsp['status'] == 'False' else True, "data":json.loads(rsp['message']),
                    'count': 0 if rsp['status'] == 'False' else 1})


def build_test_parser_config_rsp(page_parse_info, entity_datas, schema_check_result, spend_time):
    datas =  page_parse_info.extract_info.extract_data

    if datas:
        datas = json.loads(datas)
    links = []
    if page_parse_info.extract_info.links:
        for link in page_parse_info.extract_info.links:
            links.append({"url":link.url, "anchor":link.anchor, "type":link.type})
    content_type = page_parse_info.crawl_info.content_type
    response = None
    if content_type and (content_type.find('text') or content_type.find('json')):
        response = page_parse_info.crawl_info.content
        if not isinstance(response, unicode):
            for code in ['utf-8', 'gb2312', 'GBK', 'utf-16']:
                try:
                    response = response.decode(code)
                except Exception as e:
                    pass
                else:
                    break

    else:
        response = repr(page_parse_info.crawl_info.content)

    common_datas = {
        'content':""if not page_parse_info.extract_info.page_text else page_parse_info.extract_info.page_text,
        'title':"" if not page_parse_info.extract_info.html_tag_title else page_parse_info.extract_info.html_tag_title,
        'public_time': "未知" if not page_parse_info.extract_info.content_time
                                else str(datetime.datetime.fromtimestamp(page_parse_info.extract_info.content_time))
    }
    status = {
        'ex_status':page_parse_info.extract_info.ex_status,
        'extract_error':page_parse_info.extract_info.extract_error,
        'topic_id':page_parse_info.extract_info.topic_id,
        'content_language':page_parse_info.extract_info.content_language,
        'download_elapsed':page_parse_info.crawl_info.elapsed,
        'download_status_code': page_parse_info.crawl_info.status_code,
        'download_http_code':page_parse_info.crawl_info.http_code,
        'content_type': page_parse_info.crawl_info.content_type,
        'src_type':page_parse_info.base_info.src_type,
        'spend_time':spend_time
         }
    rets = {'datas':datas,
            'common_datas':common_datas,
            'response':response,
            'status':status,
            "entity_datas":entity_datas,
            'links':links,
            'check_datas':schema_check_result
            }
    return rets

@app.route('/test_parser_config', methods=['POST'])
def test_parser_config():
    if not request.json:
        return jsonify({'status':'failed', 'data':'request error'})
    req_datas = request.json
    download_req = DownLoadReq()
    download_req.method  = req_datas.get('request_method', 'get')
    download_req.url = req_datas.get('request_url')
    download_req.download_type = req_datas.get('download_type')
    download_req.post_data = {}
    download_req.http_header = {}
    try:
        download_req.http_header = json.loads(req_datas.get('headers'))
    except Exception as e:
        download_req.http_header = None
    post_data = None
    try:
        post_data = json.loads(req_datas.get('request_params'))
    except Exception as e:
        pass
    parser_id = req_datas.get('parser_id', "-1")
    page_source = req_datas.get('page_source').strip()

    if page_source not in ['cache', 'downloader', 'pagedb', 'input']:
        page_source = 'cache'
    hz_url = download_req.url
    if post_data and download_req.method == "post":
        hz_url = build_hzpost_url(download_req.url, post_data)
        download_req.url = hz_url
    spend_time = {}
    try:
        page_id = get_md5_i64(hz_url)
        download_rsp = None
        stime = time.time()

        if page_source == 'pagedb':
            download_rsp = current_app.config['crawler_merge'].select_one(hz_url)
            if download_rsp.status == 1:
                download_rsp = None
        elif page_source == 'cache':
            download_rsp = get_page_cache(page_id)
        elif page_source == 'input':
            download_rsp = DownLoadRsp()
            download_rsp.url = hz_url
            download_rsp.status = 0
            download_rsp.content_type = "text"
            download_rsp.http_code = 200
            download_rsp.download_time = 0
            download_rsp.content= req_datas.get('input_page', "").encode('utf8')
            download_rsp.src_type = "input"
            download_rsp.elapsed = 50
        if not download_rsp:
            downloader = current_app.config['downloader']
            download_rsp = downloader.download(hz_url, download_req)
            download_rsp.url = hz_url
        spend_time['download_spend'] = (time.time() - stime)*1000
        set_page_cache(page_id, download_rsp)
        is_save = req_datas.get('is_save', 'false')
        if is_save == "true":
            download_rsp.parse_extends = json.dumps({'parser_id':parser_id})
            download_rsp_tube = current_app.config['put_beanstald_server'].get_tube_by_name('download_rsp_tube')
            if download_rsp_tube:
                current_app.config['put_beanstald_server'].save_record({'tube_name':download_rsp_tube, 'obj':download_rsp})
        #复制download_rsp, 防止多线程修改
        download_rsp = deepcopy(download_rsp)
        download_rsp.parse_extends = json.dumps({"parser_id": parser_id, "debug": True})
        extractor = current_app.config['extractor']
        stime = time.time()
        extract_rsp = extractor.extract(download_rsp)
        spend_time['extract_spend'] = (time.time() - stime) * 1000
        #实体解析数据列表
        entity_datas = None
        #schema检查结果
        schema_check_result = None
        entity_rsps = None
        cur_datetime = str(datetime.datetime.now())
        try:
            stime = time.time()
            extract_data = extract_rsp.extract_info.extract_data
            if extract_data:
                extract_data_dict = json.loads(extract_data)
                _src = {"url":extract_rsp.base_info.url,
                        "site_id":extract_rsp.base_info.site_id,
                        "site":extract_rsp.base_info.site
                        }
                if "datas" in extract_data_dict:
                    datas = extract_data_dict['datas']
                    tmp_datas = []
                    for d in datas:
                        d['_src'] = [_src]
                        tmp_datas.append(d)
                    extract_data_dict['datas'] = tmp_datas
                else:
                    extract_data_dict['_src'] = [_src]
                extract_rsp.extract_info.extract_data = json.dumps(extract_data_dict)
                entity_rsps = current_app.config['entity_extractor'].entity_extract(extract_rsp)
                spend_time['entity_spend'] = (time.time() - stime) * 1000
                entity_datas = []
                for data in entity_rsps.entity_data_list:
                    if data:
                        entity_datas.append(json.loads(data.entity_data))
                    else:
                        entity_datas.append(None)
        except Exception as e:
            if entity_rsps:
                entity_datas = {'sys_error':e.message,'error_message':entity_rsps.msg}
            else:
                entity_datas = {'sys_error':e.message}
        final_data = {}
        try:
            if entity_rsps.entity_data_list:
                entity_json = {"topic_id": entity_rsps.entity_data_list[0].topic_id, "data": json.loads(entity_rsps.entity_data_list[0].entity_data)}
                datasaver_resp = current_app.config['data_saver'].check_data(json.dumps(entity_json))
                final_data = json.loads(datasaver_resp.data)
        except Exception as e:
            final_data = {'sys_error':e.message}
        return jsonify({'status':True, 'data':build_test_parser_config_rsp(extract_rsp, entity_datas, final_data, spend_time)})
    except Exception as e:
        current_app.config['logger'].error(hz_url)
        current_app.config['logger'].info(traceback.format_exc())
        return jsonify({'status':False, 'data':e.message})

@app.route('/delete_parser_config', methods=['GET'])
def delete_parser_config():
    args = request.args
    session = current_app.config['Dsession']()
    try:
        if not session.query(ParserConfig).filter_by(id=int(args['id'])).count():
            raise Exception('no this parser')
        session.query(ParserConfig).filter_by(id=int(args['id'])).delete()
        session.commit()
        extractor_servers = current_app.config['server_manager'].get_server_info('extractor')
        #通知记录中的其他服务器进行更新
        for server_info in extractor_servers["extractor"]:
            try:
                if server_info['status'] != 0:
                    continue
                call_process = multiprocessing.Process(target=call_other_server_reload_config, args =(server_info,args['id'],))
                call_process.start()
            except Exception as e:
                pass
            current_app.config['extractor'].reload_parser_config(args['id'])
        return jsonify({'status':True, 'count':0, 'data':{'id': args['id']}})
    except Exception as e:
        return jsonify({'status':False, 'data':e.message})
    finally:
        if session:
            session.close()
