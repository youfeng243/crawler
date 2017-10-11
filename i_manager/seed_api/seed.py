#!/usr/bin/env python
# -*- coding:utf-8 -*-

from flask import current_app
from flask import jsonify
from flask import request
import pymongo

from i_manager.background.models_sqlalchemy import Seeds
from . import seed_api as app
import json


@app.route('/entry', methods=['GET'])
def entry():
    site = request.args.get('site', '')
    page_size = int(request.args.get('page_size', 20))
    pageno = int(request.args.get('pageno', 1))
    url = request.args.get('url', '')
    name = request.args.get('name', '')
    label = request.args.get('label', '')
    session = current_app.config['Dsession']()
    data = []
    try:
        if site == '' and url == '' and name == '' and label == '':
            datas = session.query(Seeds).filter().offset((pageno - 1) * page_size).limit(page_size).all()
            count = session.query(Seeds).filter().all()

        if site:
            datas = session.query(Seeds).filter(Seeds.site == site).offset((pageno-1)*page_size).limit(page_size).all()
            count = session.query(Seeds).filter(Seeds.site == site).all()

        if name:
            datas = session.query(Seeds).filter(Seeds.name == name).offset((pageno - 1) * page_size).\
                limit(page_size).all()
            count = session.query(Seeds).filter(Seeds.name == name).all()

        if url:
            datas = session.query(Seeds).filter(Seeds.url == url).offset((pageno - 1) * page_size).limit(
                page_size).all()
            count = session.query(Seeds).filter(Seeds.url == url).all()

        if label:
            datas = session.query(Seeds).filter(Seeds.label.like('%' + label + '%')).offset((pageno - 1) * page_size). \
                limit(page_size).all()
            count = session.query(Seeds).filter(Seeds.label == label).all()

        data = [{'cat': data.cat, 'check_body': data.check_body, 'check_body_not': data.check_body_not,
                 'check_size': data.check_size, 'crawl_item': data.crawl_item, 'doc_type': data.doc_type,
                 'download_type': data.download_type, 'http_header': data.http_header,
                 'id': data.id, 'is_once': data.is_once, 'item_check_body': data.item_check_body,
                 'item_check_body_not': data.item_check_body_not, 'item_download_type': data.item_download_type,
                 'label': data.label, 'max_dup_count_exit': data.max_dup_count_exit, 'max_tries': data.max_tries,
                 'method': data.method, 'mode': data.mode, 'name': data.name, 'page_num': data.page_num,
                 'period': data.period, 'priority': data.priority, 'proxy': data.proxy,
                 'proxy_time': data.proxy_time, 'site': data.site, 'site_id': data.site_id, 'timeout': data.timeout,
                 'topic_id': data.topic_id, 'url': data.url,
                 'page_turning_rule': json.loads(data.page_turning_rule),
                 'variable_params': json.loads(data.variable_params),
                 'session_commit': json.loads(data.session_commit),
                 'data': json.loads(data.data), 'http_header': json.loads(data.http_header),
                 'config_init_period': json.loads(data.config_init_period)} for data in datas]

        return jsonify({'status': True, 'count': len(data), 'data': data,
                        'ext': {'pageSize': page_size, 'totalCounts': len(count)}})
    except Exception, e:
        return jsonify({'status': False, 'data': e.message, 'ext': {'pageSize': page_size, 'totalCounts': len(count)}})


@app.route('/add', methods=['POST'])
def add():
    post_data = request.form
    session = current_app.config['Dsession']

    site = post_data.get('site', '')
    if not site:
        return jsonify({'status': False, 'data': 'site is null', 'count': 0})

    mode = post_data.get('mode', '')
    is_once = post_data.get('is_once', '')
    download_type = post_data.get('download_type', '')
    method = post_data.get('method', '')
    doc_type = post_data.get('doc_type', '')
    crawl_item = post_data.get('crawl_item', '')
    priority = post_data.get('priority', '')
    item_download_type = post_data.get('item_download_type', '')
    name = post_data.get('name', '')
    label = post_data.get('label', '')
    data = post_data.get('data', '')
    url = post_data.get('url', '')
    variable_params = post_data.get('variable_params', {})
    page_turning_rule = post_data.get('page_turning_rule', {})
    config_init_period = post_data.get('config_init_period', {})
    try:
        seed = Seeds(mode=mode, is_once=is_once, download_type=download_type, method=method, doc_type=doc_type,
                     crawl_item=crawl_item, priority=priority, item_download_type=item_download_type,
                     name=name, label=label, data=data, url=url, variable_params=json.dumps(variable_params),
                     page_turning_rule=json.dumps(page_turning_rule), config_init_period=json.dumps(config_init_period))
        session.add(seed)
        session.commit()
    except Exception, e:
        return jsonify({'status': False, 'data': e.message, 'count': 0})

    return jsonify({'status': True, 'data': 'insert success', 'count': 1})


@app.route('/edit', methods=['POST'])
def edit():
    post_data = request.form
    session = current_app.config['Dsession']

    seed_id = post_data.get('id', '')
    if not seed_id:
        return jsonify({'status': False, 'data': 'seed_id is invalid', 'count': 0})

    try:
        id = post_data.get('id', '')
        page_num = post_data.get('page_num', 1)
        max_dup_count_exit = post_data.get('max_dup_count_exit', 1000)
        site = post_data.get('site', '')
        proxy = post_data.get('proxy', '')
        period= post_data.get('period', 0)
        mode = post_data.get('mode', '')
        is_once = post_data.get('is_once', '')
        download_type = post_data.get('download_type', '')
        method = post_data.get('method', '')
        doc_type = post_data.get('doc_type', '')
        crawl_item = post_data.get('crawl_item', '')
        priority = post_data.get('priority', '')
        item_download_type = post_data.get('item_download_type', '')
        name = post_data.get('name', '')
        label = post_data.get('label', '')
        data = post_data.get('data', '')
        url = post_data.get('url', '')
        variable_params = post_data.get('variable_params', {})
        page_turning_rule = post_data.get('page_turning_rule', {})
        config_init_period = post_data.get('config_init_period', {})
        up_f = {'page_num': page_num, 'max_dup_count_exit': max_dup_count_exit, 'site': site, 'proxy': proxy,
                'period': period, 'mode': mode, 'is_once': is_once, 'download_type': download_type, 'method': method,
                'doc_type': doc_type, 'crawl_item': crawl_item, 'priority': priority,
                'item_download_type': item_download_type, 'name': name, 'label': label, 'data': json.dumps(data),
                'url': url, 'variable_params': json.dump(variable_params),
                'page_turning_rule': json.dumps(page_turning_rule), 'config_init_period': json.dumps(config_init_period)}
        query = session.query(Seeds)
        query.filter(Seeds.id == id).update(up_f)
        session.flush()
    except Exception, e:
        return jsonify({'status': False, 'data': e.message, 'count': 0})

    return jsonify({'status': True, 'data': 'update success', 'count': 1})


@app.route('/delete_seed', methods=['GET'])
def delete_parser_config():
    args = request.args
    session = current_app.config['Dsession']()
    try:
        if not session.query(Seeds).filter_by(id=int(args['id'])).count():
            raise Exception('no this seed')
        session.query(Seeds).filter_by(id=int(args['id'])).delete()
        session.commit()
        return jsonify({'status': True, 'count': 0, 'data': {'id': args['id']}})
    except Exception, e:
        return jsonify({'status': False, 'data': e.message})
    finally:
        if session:
            session.close()


@app.route('/restart_seed', methods=['GET'])
def restart_seed():
    seed_id = request.args.get('seed_id', '')
    site = request.args.get('site', '')
    try:
        if seed_id and site:
            scheduler = current_app.config['scheduler']
            scheduler.restart_seed(seed_id, site)
        else:
            raise Exception('no argument id')
    except Exception, e:
        return jsonify({'code': 500, 'status': False, 'data': e.message, 'count': 0})
    return jsonify({'code': 0, 'status': True, 'count': 1, 'data': ''})


@app.route('/mongodb_test', methods=['GET'])
def mongodb_test():
    host = request.args.get('host', '')
    host = str(host)
    port = request.args.get('port', '')
    port = int(port)
    db = request.args.get('db', '')
    username = request.args.get('username', '')
    password = request.args.get('password', '')
    table = request.args.get('table', '')

    try:
        if host and port:
            conn = pymongo.MongoClient(host=host, port=port)
            if username and password:
                conn[db].authenticate(username, password)

            if db and table:
                col = conn['final_data']['table']
                for record in col.find().limit(1):
                    print record

        else:
            raise Exception('no host or port')
    except Exception, e:
        return jsonify({'code': 500, 'status': False, 'data': e.message, 'count': 0})

    return jsonify({'code': 0, 'status': True, 'count': 1, 'data': ''})
