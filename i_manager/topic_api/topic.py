#!/usr/bin/env python
# -*- coding:utf-8 -*-
import json

import multiprocessing
from flask import current_app
from flask import jsonify
from flask import request
from i_manager.background.models_sqlalchemy import Topic, DataQuery
from . import topic_api as app
from sqlalchemy.sql import func
from topic_mongo import TopicMongo
from i_util.i_crawler_services import ThriftSingleSourceMerge

@app.route('/delete_topic', methods=['GET'])
def delete_topic():
    args = request.args
    session = current_app.config['Dsession']()
    try:
        if not session.query(Topic).filter_by(id=int(args['id'])).count():
            raise Exception('no this topic')
        session.query(Topic).filter_by(id=int(args['id'])).delete()
        session.commit()
        return jsonify({'status': True, 'count': 0, 'data': {'id': args['id']}})
    except Exception, e:
        return jsonify({'status': False, 'data': e.message})
    finally:
        if session:
            session.close()

@app.route('/save_query',methods=['POST'])
def save_query():
    args = request.json
    session = current_app.config['Dsession']()
    try:
        data_query = DataQuery()
        data_query.user_id=args['user_id']
        data_query.topic_id=args['topic_id']
        data_query.query=args['query']
        data_query.utime=func.now()
        if args.has_key('id'):
            data_query.id = args['id']
            session.merge(data_query)
        else:
            session.add(data_query)
        session.commit()
        session.close()
        return jsonify({'status': True, 'count': 0, 'data': {'id': args['id']}})
    except Exception, e:
        return jsonify({'status': False, 'data': e.message})


@app.route('/query',methods=['POST'])
def query():
    args = request.json
    session = current_app.config['Dsession']()
    try:
        topic = session.query(Topic).filter_by(id=args['id']).first()
        data_query = session.query(DataQuery).filter_by(topic_id=args['id'], user_id=args['user_id']).first()
        if not topic:
            raise Exception('no this topic')
        if not data_query:
            raise Exception('no this data_query')
        topic_mongo = TopicMongo()
        page_no = args['page_no']
        page_size = args['page_size']
        find_filter = json.loads(data_query.query)#{'_id':0}
        find_filter['_id'] = 0
        data_list = list(topic_mongo.mongo_db[topic.table_name].find({},find_filter).sort('in_time').skip(page_no).limit(page_size))
        return jsonify({'status': True, 'count': 0, 'data': {'id': data_list}})
    except Exception, e:
        return jsonify({'status': False, 'data': e.message})
