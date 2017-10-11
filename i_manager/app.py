#!/usr/bin/env python
#-*- coding:utf-8 -*-
import sys

from flask import Flask
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.append('..')
from i_util.i_crawler_services import ThriftExtractor, ThriftDownloader, ThriftScheduler, ThriftEntityExtractor, ThriftCrawlerMerge, \
    ThriftDataSaver
from conf import THRIFT_DOWNLOADER_CONFIG,THRIFT_EXTRACTOR_CONFIG, THRIFT_SCHEDULER_CONFIG, \
    THRIFT_ENTITY_EXTRACTOR_CONFIG, MYSQL, log as i_manage_log, THRIFT_DATA_SAVER_CONFIG, \
    THRIFT_CRAWLERMERGE_CONFIG, REDIS, BEANSTALKD_CONFIG, MONGODB
import conf
import pymongo
from redis import StrictRedis
from i_downloader.util.proxy_new import Proxies
from put_beanstald_server import PutBeanstaldServer
from statistics import Statistics
from servermanager import ServerManager
from schedule_debug import ScheduleDebug

class QuitableFlask(Flask):

    def run(self, host=None, port=None, debug=None, **options):
        from tornado.wsgi import WSGIContainer
        from tornado.httpserver import HTTPServer
        from tornado.ioloop import IOLoop
        import tornado

        if host is None:
            host = "127.0.0.1"
        if port is None:
            port = 5000
        if debug is not None:
            self.debug = bool(debug)

        hostname = host
        port = port
        application = self
        use_reloader = self.debug
        use_debugger = self.debug

        if use_debugger:
            from werkzeug.debug import DebuggedApplication
            application = DebuggedApplication(application, True)
        container = WSGIContainer(application)
        self.http_server = HTTPServer(container)
        sockets = tornado.netutil.bind_sockets(port, hostname)
        tornado.process.fork_processes(0)
        # self.http_server.listen(port, hostname)
        self.http_server.add_sockets(sockets)
        if use_reloader:
            from tornado import autoreload
            autoreload.start()
        self.ioloop = IOLoop.current()
        self.ioloop.start()


def create_app():
    app = Flask(__name__)
    #app = QuitableFlask(__name__)
    #init ThriftServer
    app.config['extractor'] = ThriftExtractor(host=THRIFT_EXTRACTOR_CONFIG['host'],port=THRIFT_EXTRACTOR_CONFIG['port'])
    app.config['downloader'] = ThriftDownloader(host=THRIFT_DOWNLOADER_CONFIG['host'], port=THRIFT_DOWNLOADER_CONFIG['port'])
    app.config['scheduler'] = ThriftScheduler(host=THRIFT_SCHEDULER_CONFIG['host'], port=THRIFT_SCHEDULER_CONFIG['port'])
    app.config['entity_extractor'] = ThriftEntityExtractor(host=THRIFT_ENTITY_EXTRACTOR_CONFIG['host'], port=THRIFT_ENTITY_EXTRACTOR_CONFIG['port'])
    app.config['crawler_merge'] = ThriftCrawlerMerge(**THRIFT_CRAWLERMERGE_CONFIG)
    app.config['data_saver'] = ThriftDataSaver(**THRIFT_DATA_SAVER_CONFIG)
    # 统计器
    app.config['statistics'] = Statistics(logger=i_manage_log, mysql_conf=MYSQL)
    realtime_crawl = {}
    if hasattr(conf, "realtime_crawl"):
        realtime_crawl = conf.realtime_crawl
    app.config['schedule_debug'] = ScheduleDebug(logger=i_manage_log, mysql_conf=MYSQL, crawl_conf = realtime_crawl)
    # 服务监控器
    app.config['server_manager'] = ServerManager(logger= i_manage_log, redis_conf=REDIS)
    # 消息队列服务
    app.config['put_beanstald_server'] = PutBeanstaldServer(beanstalk_conf=BEANSTALKD_CONFIG, log=i_manage_log)
    app.config['put_beanstald_server'].start()
    # 代理设置
    app.config['proxy'] = Proxies(log= i_manage_log,config=REDIS)
    #init MysqlEngine
    engine = create_engine(MYSQL)
    Dsession = sessionmaker(bind=engine)
    app.config['Dsession'] = Dsession
    # mongodb
    mongo_conn = pymongo.MongoClient(host=MONGODB['host'], port=MONGODB['port'])
    if MONGODB['username'] != '':
        app.config['mongodb'] = mongo_conn[MONGODB['database']]
        app.config['mongodb'].authenticate(MONGODB['username'], MONGODB['password'])
    else:
        app.config['mongodb'] = mongo_conn[MONGODB['database']]

    # redis
    app.config['redis'] = REDIS
    app.config['redis_conn'] = StrictRedis(host=REDIS['host'], port=REDIS['port'], password=REDIS['password'])
    # regiest log
    app.config['logger'] = i_manage_log
    #register blueprint
    from parser_api import parser_api
    app.register_blueprint(parser_api, url_prefix='/api/parser')
    from stat_api import stat_api
    app.register_blueprint(stat_api, url_prefix='/api/statistics')
    from site_api import site_api
    app.register_blueprint(site_api, url_prefix='/api/site')
    from seed_api import seed_api
    app.register_blueprint(seed_api, url_prefix='/api/seed')
    from topic_api import topic_api
    app.register_blueprint(topic_api, url_prefix='/api/topic')
    from schedule_api import schedule_api
    app.register_blueprint(schedule_api, url_prefix='/api/schedule')
    from download_api import download_api
    app.register_blueprint(download_api, url_prefix='/api/download')
    from datasave_api import datasave_api
    app.register_blueprint(datasave_api, url_prefix='/api/datasave')
    from entity_api import entity_api
    app.register_blueprint(entity_api, url_prefix='/api/entity')

    @app.route('/test_parser_config')
    def test_parser_config():
        return app.send_static_file('test_parser_config.html')

    return app
