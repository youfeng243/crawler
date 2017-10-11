#!/usr/bin/env python
# -*- coding:utf-8 -*-

from flask import current_app
from flask import jsonify
from flask import request
import  traceback
from flask import Flask, flash, redirect, render_template, \
    request, url_for
from . import download_api as app
from i_downloader.util.proxy_new import Proxies
#查询所有的代理
@app.route('/get_proxy', methods=['GET'])
def get_proxy():
    try:
        data = []
        proxy = current_app.config['proxy']
        args=request.args
        site=args['site']
        data = proxy.get_proxy(site)
        return jsonify({'status':True, 'proxy':data,})
    except:
        return jsonify({'status':False, 'error_info':traceback.format_exc()})
#反馈代理不可用
@app.route('/set_unavailable', methods=['GET'])
def set_unavailable():
    try:
        data = []
        proxy = current_app.config['proxy']
        args=request.args
        site=args['site']
        aim_proxy=args['proxy']
        status=args['status']
        if status == 8:
            proxy.proxy_cannot_ping(aim_proxy)
        if status == 7:
            proxy.site_proxy_mark(site, aim_proxy)
        return jsonify({'status': True, 'proxy': data,})
    except:
        return jsonify({'status': False, 'error_info': traceback.format_exc()})
#查询所有的代理
@app.route('/findall_proxy', methods=['GET'])
def findall_proxy():
    try:
        data = []
        proxy = current_app.config['proxy']
        data,fail_num,success_num=proxy.get_proxy_detail()
        sum_proxy=len(data)
        return jsonify({'status':True, 'data':data,'sum':sum_proxy,'fail_num':fail_num,'success_num':success_num})
    except:
        return jsonify({'status':False, 'error_info':traceback.format_exc()})
#添加代理
@app.route('/add_one_proxy', methods=['post'])
def add_one_proxy():
    try:
        data = {}
        proxy = current_app.config['proxy']
        post_data=request.form
        dict={}
        dict['ip']=post_data.get('ip')
        dict['port'] = post_data.get('port')
        dict['user'] = post_data.get('user')
        dict['password'] = post_data.get('password')
        data=proxy.add_one_proxy(dict)
        return jsonify({'status':True, 'data':data})
    except:
        return jsonify({'status':False, 'error_info':traceback.format_exc()})

#添加代理
@app.route('/add_proxy', methods=['post'])
def add_proxy():
    try:
        data = {}
        proxy = current_app.config['proxy']
        post_data=request.form
        proxylist=post_data.getlist('proxy')
        proxy.add_proxy(proxylist)
        return jsonify({'status':True, 'data':data})
    except:
        return jsonify({'status':False, 'error_info':traceback.format_exc()})

# 通过IP查找代理
@app.route('/find_by_ip', methods=['post'])
def find_by_ip():
    try:
        data = {}
        proxy = current_app.config['proxy']
        post_data = request.form
        ip = post_data.get('ip')
        data=proxy.findbyip(ip)
        return jsonify({'status': True, 'data': data})
    except:
        return jsonify({'status': False, 'error_info': traceback.format_exc()})


# 通过IP删除代理
@app.route('/removebyip', methods=['POST'])
def removebyip():
    try:
        data = {}
        proxy = current_app.config['proxy']
        post_data = request.form
        ip = post_data.get('ip')
        data=proxy.removebyip(ip)
        return jsonify({'status': True, 'data': data})
    except:
        return jsonify({'status': False, 'error_info': traceback.format_exc()})


@app.route('/test_proxy', methods=['POST'])
def test_proxy():

    pass

#删除所有的代理
# @app.route('/removeall_proxy', methods=['GET'])
# def removeall_proxy():
#     try:
#         proxy = current_app.config['proxy']
#         sum_proxy=proxy.get_proxy_llen()
#         proxy.remove_all()
#         return jsonify({'status':True, 'sum':sum_proxy})
#     except:
#         return jsonify({'status':False, 'error_info':traceback.format_exc()})
