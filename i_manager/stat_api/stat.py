#!/usr/bin/env python
#-*- coding:utf-8 -*-

from flask import current_app
from flask import jsonify
from flask import request
from i_manager.background.models_sqlalchemy import ParserConfig, Topic, Seeds
import traceback
import sys
import datetime
import pymongo
import json
sys.path.append('../')
from conf import MONGODB
from . import stat_api as app


@app.route('/all_topic', methods=['GET'])
def all_topic():
    params_dict = {}
    args = request.args
    count = args.get('count', None)
    if count is not None:
        count = int(count)

    pageno = args.get('pageno', None)
    if pageno is not None:
        pageno = int(pageno)
        if pageno < 1:
            return jsonify({'status': False, 'data': {'error_info': 'pageno must greater than 1'}})

    id = args.get('id', None)
    name = args.get('name', None)
    table_name = args.get('table_name', None)
    statistics = current_app.config['statistics']
    try:
        data, ext, ret_count= statistics.find_topic_crawl_info(count, pageno, id, name, table_name)
        return jsonify({'count': ret_count, 'status': True, 'data': data, 'ext': ext})
    except:
        current_app.config['logger'].error(traceback.format_exc())
        return jsonify({'status': False, 'data': {'error_info':traceback.format_exc()}})


@app.route('/topic', methods=['GET'])
def topic():
    logger = current_app.config['logger'];
    args = request.args
    statistics = current_app.config['statistics']
    try:
        start_time = args.get('start_time', None);
        end_time = args.get('end_time', None);
        topic_id = args.get('topic_id', 0);
        stat_type = args.get('type', 'daily');
        data = statistics.topic(topic_id, stat_type,start_time, end_time);
        return jsonify({'status':True, 'data': data})
    except Exception, e:
        logger.error(e.message)
        return jsonify({'status': False, 'data': {'error_info': e.message}})


@app.route('/server', methods=['GET'])
def server():
    args = request.args
    server_name = args.get('name', None)
    logger = current_app.config['logger']
    try:
        return jsonify({
            'status':True,
            'data':{
                'last_check_time':current_app.config['server_manager'].last_check_time,
                'servers':current_app.config['server_manager'].get_server_info(server_name)
            }
        })
    except Exception as e:
        logger.error(e.message)
        return jsonify({
            'status':False,
            'data': {'error_info':e.message}
        })


@app.route('/site', methods=['GET'])
def site():
    args = request.args
    site = args.get('site', None)
    site_name = args.get('site_name', None)
    page_size = 20 if args.get('page_size', 20) is '' else int(args.get('page_size', 20))
    pageno = 1 if args.get('pageno', 1) is '' else int(args.get('pageno', 1))
    mongo_conn = current_app.config['mongodb']
    logger = current_app.config['logger']
    select = {}
    result = []
    if site is None and site_name is None:
        select = {'metadata.date': datetime.date.today().isoformat()}
    if site:
        select = {'metadata.site': {'$regex': str(site)}, 'metadata.date': datetime.date.today().isoformat()}
    if site_name:
        select = {'metadata.site_name': {'$regex': site_name}, 'metadata.date': datetime.date.today().isoformat()}
    try:
        total_records = mongo_conn.site_task.find(select)
        records = mongo_conn.site_task.find(select).skip((pageno-1)*page_size).limit(page_size)
        result = [{'site': x['metadata']['site'], 'site_name': x['metadata']['site_name'],
                   'request_count': x['request_count']['daily'], 'response_count': x['response_count']['daily'],
                   'success_count': x['success_count']['daily'], 'fail_count': x['fail_count']['daily'],
                   'date': x.get('date', '')}
                  for x in records]
        return jsonify({
            'status': True,
            'data': {'data': result, 'total_count': len(list(total_records))},
            'message': ''
        })
    except Exception, e:
        logger.error(e.message)
        return jsonify({
            'status': False,
            'data': {'data': result, 'total_count': 0},
            'message': e.message
        })


@app.route('/seed_crawl', methods=['GET'])
def seed_crawl():
    args = request.args
    site = args.get('site', None)
    seed_name = args.get('seed_name', None)
    page_size = 20 if args.get('page_size', 20) is '' else int(args.get('page_size', 20))
    pageno = 1 if args.get('pageno', 1) is '' else int(args.get('pageno', 1))
    mongo_conn = current_app.config['mongodb']
    logger = current_app.config['logger']
    select = {}
    result = []
    id_period = {}

    session = current_app.config['Dsession']()
    seeds_info = session.query(Seeds.id, Seeds.config_init_period).all()
    for record in seeds_info:
        is_period = record[1]['is_period']
        if is_period == 'false':
            id_period[int(record[0])] = -1
        else:
            id_period[int(record[0])] = record[1]['period']

    if site is None and seed_name is None:
        select = {'metadata.date': datetime.date.today().isoformat()}
    if site:
        select = {'metadata.site': {'$regex': str(site)}, 'metadata.date': datetime.date.today().isoformat()}
    if seed_name:
        select = {'metadata.seed_name': {'$regex': seed_name}, 'metadata.date': datetime.date.today().isoformat()}
    try:
        total_records = mongo_conn.seed_task.find(select).sort('metadata.seed_id', pymongo.ASCENDING)
        records = mongo_conn.seed_task.find(select).skip((pageno-1)*page_size).limit(page_size)\
            .sort('metadata.seed_id', pymongo.ASCENDING)
        result = [{'site': x['metadata']['site'], 'seed_name': x['metadata']['seed_name'],
                   'seed_id': x['metadata']['seed_id'],
                   'dispatch_frequency': id_period.get(x['metadata']['seed_id'], ''),
                   'download_count': x['download_count']['daily'],
                   'content_page_count': x['content_page_count']['daily'],
                   'download_fail_count': x['download_fail_count']['daily'],
                   'download_success_count': x['download_success_count']['daily'],
                   'download_content_success_count': x['download_content_success_count']['daily'],
                   'last_success_download_time': x['last_success_download_time']
                   } for x in records]
        return jsonify({
            'status': True,
            'data': {'data': result, 'total_count': len(list(total_records))},
            'message': ''
        })
    except Exception, e:
        logger.error(e.message)
        return jsonify({
            'status': False,
            'data': {'data': result, 'total_count': 0},
            'message': e.message
        })


@app.route('/parse_rule', methods=['GET'])
def parse_rule():
    id_name = {}
    name_id = {}
    session = current_app.config['Dsession']()
    parse_rule_data = session.query(ParserConfig.id, ParserConfig.name).all()
    for record in parse_rule_data:
        id_name[record[0]] = record[1]
        name_id[record[1]] = record[0]

    args = request.args
    site = args.get('site', '')
    parser_name = args.get('parser_name', '')
    page_size = 20 if args.get('page_size', 20) is '' else int(args.get('page_size', 20))
    pageno = 1 if args.get('pageno', 1) is '' else int(args.get('pageno', 1))
    mongo_conn = current_app.config['mongodb']
    logger = current_app.config['logger']
    select = {}
    result = []
    if site == '' and parser_name == '':
        select = {'metadata.parser_id': {'$ne': -1}, 'metadata.date': datetime.date.today().isoformat()}

    if site:
        select = {'metadata.site': {'$regex': str(site)}, 'metadata.parser_id': {'$ne': -1},
                  'metadata.date': datetime.date.today().isoformat()}
    if parser_name:
        select = {'metadata.parser_id': name_id.get(parser_name, ''),
                  'metadata.date': datetime.date.today().isoformat()}
    try:
        total_records = mongo_conn.extract_task.find(select).sort('metadata.parser_id', pymongo.ASCENDING)
        records = mongo_conn.extract_task.find(select).skip((pageno-1)*page_size).limit(page_size)\
            .sort('metadata.parser_id', pymongo.ASCENDING)
        result = [{'site': x['metadata']['site'], 'parser_name': id_name.get(x['metadata']['parser_id'], ''),
                   'parser_id': x['metadata']['parser_id'],
                   'in_parse_file_count': x['extract_success']['daily'] + x['download_fail']['daily'] + x['extract_fail']['daily'] + x['extract_skip']['daily'],
                   'success_parse_file_count': x['extract_success']['daily'],
                   'success_download_file_count': x['extract_success']['daily'] + x['extract_skip']['daily'] + x['extract_fail']['daily'],
                   'fail_parse_file_count': x['extract_fail']['daily'], 'date': x.get('date', '')
                   } for x in records]

        return jsonify({
            'status': True,
            'data': {'data': result, 'total_count': len(list(total_records))},
            'message': ''
        })
    except Exception, e:
        logger.error(e.message)
        return jsonify({
            'status': False,
            'data': {'data': result, 'total_count': 0},
            'message': e.message
        })


@app.route('/entity_parse', methods=['GET'])
def entity_parse():
    args = request.args
    topic_name = args.get('topic_name', '')
    page_size = int(args.get('page_size', 20))
    pageno = int(args.get('pageno', 1))
    mongo_conn = current_app.config['mongodb']
    id_name = {}
    name_id = {}
    select = {}
    topic_id = ''
    session = current_app.config['Dsession']()
    topic_data = session.query(Topic.id, Topic.name).all()
    for record in topic_data:
        id_name[record[0]] = record[1]
        name_id[record[1]] = record[0]

    if topic_name == '':
        pass
    else:
        topic_id = name_id.get(topic_name, '')

    result = []
    if topic_id == '':
        select = {'metadata.date': datetime.date.today().isoformat()}
    else:
        select = {'metadata.topic_id': topic_id, 'metadata.date': datetime.date.today().isoformat()}

    try:
        total_records = mongo_conn.entity_task.find(select).sort('metadata.topic_id', pymongo.ASCENDING)
        records = mongo_conn.entity_task.find(select).skip((pageno - 1) * page_size).limit(page_size)\
            .sort('metadata.topic_id', pymongo.ASCENDING)
        result = [{'entity_id': x['metadata']['topic_id'], 'topic_name': id_name[x['metadata']['topic_id']],
                   'in_parse_file_count': x['success']['daily'] + x['failure']['daily'],
                   'success_parse_file_count': x['success']['daily'],
                   'fail_parse_file_count': x['failure']['daily'],
                   'date': x.get('date', '')
                   } for x in records]
    except Exception, e:
        return jsonify({'status': False, 'data': {'data': result, 'total_count': 0}, 'message': e.message})

    return jsonify({'status': True, 'data': {'data': result, 'total_count': len(list(total_records))}, 'message': ''})

