#!/usr/bin/env python
# -*- coding:utf-8 -*-
import json

from flask import current_app
from flask import jsonify
from flask import request
from i_manager.background.models_sqlalchemy import Site, Topic
from . import site_api as app
import re


@app.route('/add', methods=['POST'])
def add():
    post_data = request.form
    session = current_app.config['Dsession']()
    try:
        regex = re.compile('^([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z0-9]{2,6}$')
        site = post_data.get('site', '')
        ret = regex.match(site)
        if not ret:
            return jsonify({'status': False, 'count': 0, 'data': 'site format invalid'})
        name = post_data.get('name', '')
        avg_interval = post_data.get('avg_interval', '')
        encoding = post_data.get('encoding', 'utf-8')
        label = post_data.get('label', '')
        site = Site(site=site, name=name, avg_interval=avg_interval, encoding=encoding, label=label)
        session.add(site)
        session.commit()
    except Exception, e:
        return jsonify({'status': False, 'count': 0, 'data': e.message})
    return jsonify({'status': True, 'count': 1, 'data': ''})


@app.route('/edit', methods=['POST'])
def edit():
    post_data = request.form
    session = current_app.config['Dsession']()
    try:
        regex = re.compile('^([a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z0-9]{2,6}$')
        id = post_data.get('id', '')
        if not id:
            return jsonify({'status': False, 'count': 0, 'data': 'id must input'})

        site = post_data.get('site', '')
        ret = regex.match(site)
        if not ret:
            return jsonify({'status': False, 'count': 0, 'data': 'site format invalid'})
        name = post_data.get('name','')
        avg_interval = post_data.get('avg_interval', '')
        encoding = post_data.get('encoding', '')
        label = post_data.get('label', '')
        up_f = {'site': site, 'name': name, 'avg_interval': avg_interval, 'encoding': encoding, 'label': label}
        query = session.query(Site)
        query.filter(Site.id == id).update(up_f)
        session.flush()
    except Exception, e:
        return jsonify({'status': False, 'count': 0, 'data': e.message})

    return jsonify({'status': True, 'count': 1, 'data': ''})


@app.route('/entry', methods=['GET'])
def entry():
    site = request.args.get('site', '')
    name = request.args.get('name', '')
    pageno = int(request.args.get('pageno', 1))
    label = request.args.get('label', '')
    page_size = int(request.args.get('page_size', 20))
    session = current_app.config['Dsession']()
    redis_conn = current_app.config['redis_conn']
    try:
        id_name = {}
        name_id = {}
        topic_data = session.query(Topic.id, Topic.name).all()
        for record in topic_data:
            id_name[record[0]] = record[1]
            name_id[record[1]] = record[0]

        if site == '' and name == '':
            query = session.query(Site)
            datas = query.filter().offset((pageno-1)*page_size).limit(page_size).all()

        if site:
            query = session.query(Site)
            datas = query.filter(Site.site == site).offset((pageno - 1) * page_size).limit(page_size).all()

        if name:
            query = session.query(Site)
            datas = query.filter(Site.name == name).offset((pageno - 1) * page_size).limit(page_size).all()

        if label:
            label = name_id.get(label, '')
            query = session.query(Site)
            datas = query.filter(Site.label.like('%' + label + '%')).offset((pageno - 1) * page_size).\
                limit(page_size).all()

        result = [{'site': data.site, 'site_id': data.site_id, 'topic_id': data.topic_id, 'name': data.name,
                   'mongo_table_name': data.mongo_table_name, 'list_display_fields': data.list_display_fields,
                   'label': data.label, 'encoding': data.encoding, 'ctime': data.ctime, 'id': data.id,
                   'avg_interval': data.avg_interval,
                   'schedule_info': redis_conn.hgetall("site:%s" % data.site)} for data in datas]

    except Exception, e:
        return jsonify({'status': False, 'count': 0, 'data': e.message})

    return jsonify({'status': True, 'count': 1, 'data': result})


@app.route('/start_crawl', methods=['GET'])
def start_crawl():
    sites = request.args.get('site', '')
    sites = sites.split(',')
    try:
        if sites:
            scheduler = current_app.config['scheduler']
            if isinstance(sites, list):
                for site in sites:
                    scheduler.start_one_site_tasks(site)
        else:
            raise Exception('no argument id')
    except Exception, e:
        return jsonify({'code': 500, 'status': False, 'data': e.message, 'count': 0})
    return jsonify({'code': 0, 'status': True, 'count': len(sites), 'data': ''})


@app.route('/stop_crawl', methods=['GET'])
def stop_crawl():
    sites = request.args.get('site', '')
    sites = sites.split(',')
    try:
        if sites:
            scheduler = current_app.config['scheduler']
            for site in sites:
                scheduler.stop_one_site_tasks(site)
        else:
            raise Exception('no argument id')
    except Exception, e:
        return jsonify({'code': 500, 'status': False, 'data': e.message, 'count': 0})
    return jsonify({'code': 0, 'status': True, 'count': len(sites), 'data': ''})


@app.route('/delete_site', methods=['GET'])
def delete_site():
    args = request.args
    session = current_app.config['Dsession']()
    redis_conn = current_app.config['redis_conn']
    try:
        if not session.query(Site.site).filter_by(id=int(args['id'])).count():
            raise Exception('no this site')

        site = session.query(Site.site).filter_by(id=int(args['id'])).first().site
        status = redis_conn.hget("site:%s" % site, 'status')
        if status == 'running':
            return jsonify({'status': False, 'data': 'site is running, stop site first.'})
        session.query(Site).filter_by(id=int(args['id'])).delete()
        session.commit()
        return jsonify({'status': True, 'count': 0, 'data': {'id': args['id']}})
    except Exception, e:
        return jsonify({'status': False, 'data': e.message})
    finally:
        if session:
            session.close()


@app.route('/delete_cache', methods=['GET'])
def delete_cache():
    site = request.args.get('site', '')
    try:
        if site:
            scheduler = current_app.config['scheduler']
            scheduler.clear_one_site_cache(site)
        else:
            raise Exception('no argument id')
    except Exception, e:
        return jsonify({'code': 500, 'status': False, 'data': e.message, 'count': 0})
    return jsonify({'code': 0, 'status': True, 'count': 1, 'data': ''})
