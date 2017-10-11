#!/usr/bin/env python
#-*- coding:utf-8 -*-
import json
from flask import current_app
from flask import jsonify
from flask import request
from  . import schedule_api as app

@app.route('/find_linkbase', methods=['POST'])
def find_linkbase():
    schedule_debug = current_app.config['schedule_debug']
    logger = current_app.config['logger'];
    post_data=request.form
    try:
        url = post_data.get('url');
        logger.info("find_linkbase\turl:%s" % (url))
        data = schedule_debug.find_linkbase(url);
        return jsonify({'status':True, 'data':data})
    except Exception,e:
        logger.error(e.message)
        return jsonify({'status':False, 'data':{'error_info':e.message}})
@app.route('/find_webpage', methods=['POST'])
def find_webpage():
    schedule_debug = current_app.config['schedule_debug']
    logger = current_app.config['logger'];
    post_data=request.form
    try:
        url = post_data.get('url');
        logger.info("find_linkbase\turl:%s" % (url))
        data = schedule_debug.find_webpage(url);
        return jsonify({'status':True, 'data':data})
    except Exception,e:
        logger.error(e.message)
        return jsonify({'status':False, 'data':{'error_info':e.message}})

@app.route('/company_schedule', methods=['GET'])
def company_schedule():
    schedule_debug = current_app.config['schedule_debug']
    logger = current_app.config['logger'];
    try:
        company = request.args.get('company', '');
        level = request.args.get('level', '6');
        province = request.args.get('province', '');
        data =  schedule_debug.schedule_company(company, level, province)
        return jsonify({'status':True, 'data':data})
    except Exception,e:
        logger.error(e.message)
        return jsonify({'status':False, 'data':{'error_info':e.message}})
@app.route('/companies_schedule', methods=['post'])
def companies_schedule():
    schedule_debug = current_app.config['schedule_debug']
    logger = current_app.config['logger']
    post_data=request.json
    try:
        companies = post_data.get('companies', '')
        data = schedule_debug.schedule_companies(companies)
        return jsonify({'status':True, 'data':data})
    except Exception,e:
        return jsonify({'status':False, 'data':{'error_info':e.message}})



@app.route('/import_schedule', methods=['GET'])
def import_schedule():
    schedule_debug = current_app.config['schedule_debug']
    logger = current_app.config['logger'];
    try:
        company = request.args.get('company', '');
        province = request.args.get('province', '');
        level = request.args.get('level', 2);
        user = request.args.get('user', '');
        data =  schedule_debug.import_schedule(company,province, level, user)
        return jsonify({'status':True, 'data':data})
    except Exception,e:
        return jsonify({'status':False, 'data':{'error_info':e.message}})

@app.route('/get_schedule_list', methods=['GET'])
def get_schedule_list():
    schedule_debug = current_app.config['schedule_debug']
    logger = current_app.config['logger'];
    try:
        user = request.args.get('user', '');
        start = request.args.get('start', '0');
        limit = request.args.get('limit', '10');
        data =  schedule_debug.get_schedule_list(user,start, limit)
        return jsonify({'status':True, 'data':data})
    except Exception,e:
        return jsonify({'status':False, 'data':{'error_info':e.message}})

@app.route('/import_companies', methods=['POST'])
def import_companies():
    schedule_debug = current_app.config['schedule_debug']
    logger = current_app.config['logger'];
    #post_data=request.form
    post_data = request.json
    try:
        companies = post_data.get('companies', '');
        user = post_data.get('user', '');
        data =  schedule_debug.import_companies(companies, user)
        return jsonify({'status':True, 'data':data})
    except Exception,e:
        return jsonify({'status':False, 'data':{'error_info':e.message}})
