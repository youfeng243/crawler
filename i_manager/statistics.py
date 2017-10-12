# coding=utf-8
import json
import os
import time
import traceback

import pymongo
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from  background.models_sqlalchemy import Settings, Topic


class Statistics:

    def __init__(self, logger, mysql_conf):
        self.logger = logger
        self.mysql_conf = mysql_conf;
        self.mongodb_conf = {};
        self.table_name = '__STATISTICS__'
        try:

            engine = create_engine(self.mysql_conf, pool_recycle=3600)
            Settings.metadata.create_all(engine)
            self.Dsession = sessionmaker(bind=engine)
            session = self.Dsession()
            query = session.query(Settings)
            records = query.filter(Settings.item == 'mongodb').all()
            for record in records:
                self.mongodb_conf['host'] = '172.17.1.119'
                self.mongodb_conf['port'] = record.value['port']
                self.mongodb_conf['database'] = record.value['database']
		if record.value['database']=='final_data':                
		    self.mongodb_conf['database'] = 'app_data'
		self.mongodb_conf['user'] = record.value['user']
                self.mongodb_conf['password'] = record.value['password']
                break;
            session.close();
        except Exception as e:
            self.logger.error(traceback.format_exc())
            os._exit(1) 
        #初始化mongodb
        self.mongo_conn = pymongo.MongoClient(host=self.mongodb_conf['host'],
                                              port=int(self.mongodb_conf['port']), connect=False);
        self.mongo_db = self.mongo_conn[self.mongodb_conf['database']];
        if self.mongodb_conf['user'] and self.mongodb_conf['password']:
            self.mongo_db.authenticate(self.mongodb_conf['user'], self.mongodb_conf['password']);

    def create_topic_stat(self, yes_date, now_date):
        topic_stat = {
            'topic_id': 0,
            'name' : "",
            'table_name':"",
            'yes_date' : yes_date,
            'yes_sum'  : 0,
            'yes_insert'  : 0,
            'yes_update'  : 0,
            'now_date' : now_date,
            'now_sum'  : 0,
            'now_insert'  : 0,
            'now_update'  : 0,
        }
        return topic_stat;

    def create_topic_detail(self, start_date, end_date):
        topic_stat = {
            'topic_id': 0,
            'name' : "",
            'table_name':"",
            'start_date' : start_date,
            'end_date' : end_date,
            'sum':0,
            'points':{
                'times':[],
                'insert':[],
                'update':[],
                'failure':[],
            },
            'type':"daily"
        }
        return topic_stat

    def find_topic_info(self, pageno, count, id, name, table_name, yes_date, now_date):
        # mysql数据访问
        select = ''
        daily_topic_stats = {}
        session = self.Dsession()
        query = session.query(Topic)
        if None in [pageno, count]:
            pageno = 1
            count = 20

        skip = (pageno-1) * count

        if id != '' and id is not None:
            totalcount = len(query.filter(Topic.id.like('%' + id + '%')).all())
            records = query.filter(Topic.id.like('%' + id + '%')).offset(skip).limit(count).all()
            ret_count = len(records)

        if name != '' and name is not None:
            totalcount = len(query.filter(Topic.name.like('%' + name + '%')).all())
            records = query.filter(Topic.name.like('%' + name + '%')).offset(skip).limit(count).all()
            ret_count = len(records)

        if table_name != '' and table_name is not None:
            totalcount = len(query.filter(Topic.table_name.like('%' + table_name + '%')).all())
            records = query.filter(Topic.table_name.like('%' + table_name + '%')).offset(skip).limit(count).all()
            ret_count = len(records)

        if name in ['', None] and table_name in ['', None] and id in ['', None]:
            select = ''
            totalcount = len(query.filter(select).all())
            records = query.filter(select).offset(skip).limit(count).all()
            ret_count = len(records)

        for record in records:
            daily_topic_stat = self.create_topic_stat(yes_date, now_date)
            daily_topic_stat['topic_id'] = record.id
            daily_topic_stat['name'] = record.name
            daily_topic_stat['table_name'] = record.table_name
            daily_topic_stat['yes_sum'] = daily_topic_stat['now_sum']
            daily_topic_stat['size'] = 0.00
            daily_topic_stats[record.id] = daily_topic_stat
        if session:
            session.close()

        ext = {'pageSize': count, 'totalCounts': totalcount}
        return daily_topic_stats, ext, ret_count

    def find_crawl_info(self, yes_date, now_date, daily_topic_stats):
        for id in daily_topic_stats.keys():
            try:
                daily_topic_stats[id]['now_sum'] = self.mongo_db.command('collstats', daily_topic_stats[id]['table_name'])['count']
                daily_topic_stats[id]['size'] = '%.3f' % (self.mongo_db.command('collstats', daily_topic_stats[id]['table_name'])['size']/1024/1024.0)
            except Exception as e:
                daily_topic_stats[id]['now_sum'] = 0

        # 对monggo进行访问
        mongo_datas = self.mongo_db[self.table_name].find({"metadata.date":{'$in': [yes_date, now_date]}})
        for stat_topic in mongo_datas:
            if stat_topic.has_key('metadata') and stat_topic['metadata'].has_key('topic_id'):
                topic_id = stat_topic['metadata']['topic_id']
                topic_date = stat_topic['metadata']['date']
                daily_topic_stat = daily_topic_stats.get(topic_id, None)
                if daily_topic_stat and daily_topic_stat['now_date'] == topic_date:
                    daily_topic_stat['now_insert'] = stat_topic.get('success_insert', {}).get('daily',0);
                    daily_topic_stat['now_update'] = stat_topic.get('success_update', {}).get('daily',0)
                    daily_topic_stat['yes_sum'] = daily_topic_stat['now_sum'] - stat_topic.get('success_insert', {})\
                        .get('daily', 0)
                elif daily_topic_stat and daily_topic_stat['yes_date'] == topic_date:
                    daily_topic_stat['yes_insert'] = stat_topic.get('success_insert', {}).get('daily', 0)
                    daily_topic_stat['yes_update'] = stat_topic.get('success_update', {}).get('daily', 0)
        return [item[1] for item in sorted(daily_topic_stats.items(), key=lambda d: d[0], reverse=True)]

    def find_topic_crawl_info(self, count, pageno, id, name, table_name):
        yes_date = time.strftime('%Y-%m-%d', time.localtime(time.time() - 86400));
        now_date = time.strftime('%Y-%m-%d', time.localtime(time.time()));
        daily_topic_stats, ext, ret_count = self.find_topic_info(pageno, count, id, name, table_name, yes_date, now_date)
        return self.find_crawl_info(yes_date, now_date, daily_topic_stats), ext, ret_count
        #修改延迟查询mongo,防止mysql connection has gone away

    def topic(self, topic_id = 0, type="daily", start_time = None, end_time = None):
        if (start_time is None) or (end_time is None):
            return None;
        if start_time >= end_time:
            return None;
        try:
            start_time = int(start_time);
            end_time = int(end_time)
        except Exception as e:
            self.logger.error(traceback.format_exc())
            return None
            
        start_date = time.strftime('%Y-%m-%d',time.localtime(start_time));
        end_date = time.strftime('%Y-%m-%d',time.localtime(end_time));
        session = self.Dsession()
        query = session.query(Topic);
        records = query.filter(Topic.id==topic_id).all()
        topic_detail = self.create_topic_detail(start_date, end_date);
        for record in records:
            topic_detail['topic_id'] = record.id
            topic_detail['name'] = record.name;
            topic_detail['table_name'] = record.table_name;
        session.close();
        try:
            topic_detail['sum'] = self.mongo_db.command('collstats', topic_detail['table_name'])['count']
        except Exception as e:
            topic_detail['sum'] = 0
            self.logger.warning(e.message)
        if topic_detail['topic_id'] == 0:
            return None;
        # 对monggo进行访问
        mongo_datas = self.mongo_db[self.table_name].find({"metadata.topic_id": topic_detail['topic_id'], "metadata.date":{'$gte': start_date, '$lte': end_date}})
        daily_dict = {};
        hourly_dict = {};
        for stat_topic in mongo_datas:
            date_str = stat_topic["metadata"]["date"];
            daily_dict[date_str] = (stat_topic['success_insert'].get('daily', 0), stat_topic['success_update'].get('daily', 0), stat_topic['failure'].get('daily', 0))
            for key, value in stat_topic['success_insert']['hourly'].items():
                try:
                    update = int(stat_topic['success_update']['hourly'][key])
                    failure = int(stat_topic['failure']['hourly'][key])
                    key = "%s-%02d" % (date_str, int(key));
                    hourly_dict[key] = (int(value), update,failure);
                except:
                    self.logger.error("cast_error\tdate_str:%s\ttopic_id:%s" % (date_str, topic_detail['topic_id']));
        if type == 'daily':
            topic_detail['type'] = 'daily'
            while start_time <= end_time:
                date_str = time.strftime('%Y-%m-%d', time.localtime(start_time));
                start_time += 86400;
                if daily_dict.has_key(date_str):
                    topic_detail['points']['times'].append(date_str);
                    topic_detail['points']['insert'].append(daily_dict[date_str][0]);
                    topic_detail['points']['update'].append(daily_dict[date_str][1]);
                    topic_detail['points']['failure'].append(daily_dict[date_str][2]);
                else:
                    topic_detail['points']['times'].append(date_str);
                    topic_detail['points']['insert'].append(0);
                    topic_detail['points']['update'].append(0);
                    topic_detail['points']['failure'].append(0);
        elif type == 'hourly':
            topic_detail['type'] = 'hourly'
            while start_time < end_time+86400 and start_time <= time.time():
                hour_str = time.strftime('%Y-%m-%d-%H', time.localtime(start_time));
                start_time += 3600;
                if hourly_dict.has_key(hour_str):
                    topic_detail['points']['times'].append(hour_str);
                    topic_detail['points']['insert'].append(hourly_dict[hour_str][0]);
                    topic_detail['points']['update'].append(hourly_dict[hour_str][1]);
                    topic_detail['points']['failure'].append(hourly_dict[hour_str][2]);
                else:
                    topic_detail['points']['times'].append(hour_str);
                    topic_detail['points']['insert'].append(0);
                    topic_detail['points']['update'].append(0);
                    topic_detail['points']['failure'].append(0);
        return topic_detail

def main(conf):
    stat = Statistics(conf.log, conf.MYSQL)
    print json.dumps(stat.all_topic());
    #print json.dumps(stat.topic(71, 'daily', 1476259259-86400, 1476259259));


if __name__ == '__main__':
    import conf
    main(conf);
