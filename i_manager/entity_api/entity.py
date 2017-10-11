#!/usr/bin/env python
# -*- coding:utf-8 -*-
import json
import multiprocessing
import traceback

from flask import current_app
from flask import jsonify
from flask import request

from i_util.i_crawler_services import ThriftEntityExtractor, ThriftSingleSourceMerge
from . import entity_api as app


def call_other_server_reload_config(server_info, topic_id, class_):
    try:
        current_app.config['logger'].info("start call server %s" % server_info['server'])
        host, port = server_info['server'].split(':')
        class_(host=host, port=int(port)).reload(topic_id)
        current_app.config['logger'].info(
            "call %s server: %s reload topic success." % (class_.name, server_info['server']))
    except Exception as e:
        current_app.config['logger'].info(
            "call %s server: %s reload topic failed." % (class_.name, server_info['server']))
        current_app.config['logger'].error(e.message)


@app.route('/reload_topic', methods=['GET'])
def reload():
    ''' 实体解析重载topic和解析器'''
    current_app.config['logger'].info("start_entity_reload")
    args = request.args
    entity_extractor = current_app.config['entity_extractor']
    try:
        topic_id = int(args.get("topic_id", -1))
        resp = entity_extractor.reload(topic_id)
        current_app.config['logger'].info("finish_entity_reload:" + str(resp))
        _servers = current_app.config['server_manager'].get_server_info('entity_extractor')
        for server_info in _servers["entity_extractor"]:
            try:
                if server_info['status'] != 0:
                    continue
                call_process = multiprocessing.Process(target=call_other_server_reload_config,
                                                       args=(server_info, topic_id, ThriftEntityExtractor))
                call_process.start()

            except Exception as e:
                pass
        _servers = current_app.config['server_manager'].get_server_info('single_src_merge')
        for server_info in _servers["single_src_merge"]:
            try:
                if server_info['status'] != 0:
                    continue
                call_process = multiprocessing.Process(target=call_other_server_reload_config,
                                                       args=(server_info, topic_id, ThriftSingleSourceMerge))
                call_process.start()
            except Exception as e:
                pass
        return jsonify({'status': True, 'msg': resp.msg, 'data': resp.data})
    except Exception, e:
        return jsonify({'status': False, 'data': e.message})


@app.route('/add_extractor', methods=['POST'])
def add_extractor():
    '''实体解析添加解析器'''
    if not request.json:
        return jsonify({'status': 'failed', 'data': 'request error'})
    args = request.json
    current_app.config['logger'].info("start_add_extractor, args:%s" % args)
    entity_extractor = current_app.config['entity_extractor']
    try:
        resp = entity_extractor.add_extractor(json.dumps(args))
        current_app.config['logger'].info("finish_add_extractor:" + str(resp))
        return jsonify({'status': True, 'msg': resp.msg, 'data': resp.data})
    except:
        current_app.config['logger'].error("fail_add_extractor, args:%s\tret:%s" % (args, traceback.format_exc()))
        return jsonify({'status': False, 'data': traceback.format_exc()})