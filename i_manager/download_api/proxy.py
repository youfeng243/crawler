#!/usr/bin/env python
# -*- coding:utf-8 -*-

import traceback

from flask import current_app
from flask import jsonify
from flask import request

from . import download_api as app


# 查询所有的代理
@app.route('/get_proxy', methods=['GET'])
def get_proxy():
    try:
        data = []
        proxy = current_app.config['proxy']
        args = request.args
        site = args['site']
        data = proxy.get_proxy(site)
        return jsonify({'status': True, 'proxy': data, })
    except:
        return jsonify({'status': False, 'error_info': traceback.format_exc()})


# 反馈代理不可用
@app.route('/set_unavailable', methods=['GET'])
def set_unavailable():
    try:
        data = []
        proxy = current_app.config['proxy']
        args = request.args
        site = args['site']
        aim_proxy = args['proxy']
        status = args['status']
        # if status == 8:
        #     proxy.proxy_cannot_ping(aim_proxy)
        # if status == 7:
        #     proxy.site_proxy_mark(site, aim_proxy)
        return jsonify({'status': True, 'proxy': data, })
    except:
        return jsonify({'status': False, 'error_info': traceback.format_exc()})


# 添加代理
@app.route('/add_proxy', methods=['post'])
def add_proxy():
    try:
        data = {}
        proxy = current_app.config['proxy']
        post_data = request.form
        proxylist = post_data.getlist('proxy')
        proxy.add_proxy(proxylist)
        return jsonify({'status': True, 'data': data})
    except:
        return jsonify({'status': False, 'error_info': traceback.format_exc()})


@app.route('/test_proxy', methods=['POST'])
def test_proxy():
    pass

# 删除所有的代理
# @app.route('/removeall_proxy', methods=['GET'])
# def removeall_proxy():
#     try:
#         proxy = current_app.config['proxy']
#         sum_proxy=proxy.get_proxy_llen()
#         proxy.remove_all()
#         return jsonify({'status':True, 'sum':sum_proxy})
#     except:
#         return jsonify({'status':False, 'error_info':traceback.format_exc()})
