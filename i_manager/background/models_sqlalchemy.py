# -*- coding: utf-8 -*-
import json
import traceback

from sqlalchemy import Column, String, Float, Text, DateTime, Integer, BLOB, types, text, Enum, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class JsonType(types.TypeDecorator):
    impl = types.TEXT

    def process_bind_param(self, value, engine):
        return value

    def process_result_value(self, value, engine):
        ret = None
        if value:
            try:
                ret = json.loads(value.strip())
            except Exception, e:
                print traceback.format_exc()
        return ret


class EvalString(types.TypeDecorator):
    impl = types.Unicode

    def process_bind_param(self, value, engine):
        return value

    def process_result_value(self, value, engine):
        ret = None
        if value:
            try:
                ret = value.strip('\'').strip('\"')
            except Exception, e:
                print traceback.format_exc()
        return ret


class Settings(Base):
    __tablename__ = "settings"
    __table_args__ = {
        'mysql_engine': 'innodb',
        'mysql_charset': 'utf8'
    }
    id = Column(Integer(), primary_key=True, autoincrement=True)
    item = Column(String(128))
    value = Column(JsonType, nullable=True, default=None)


class ParserConfig(Base):
    __tablename__ = "parser_config"
    __table_args__ = {
        'mysql_engine': 'innodb',
        'mysql_charset': 'utf8'
    }

    id = Column(Integer(), primary_key=True, autoincrement=True)
    name = Column(String(40), nullable=False, unique=True)
    topic_id = Column(Integer())
    url_format = Column(EvalString(2048), nullable=False)
    datas_type = Column(EvalString(10))
    datas_rule = Column(JsonType, nullable=True, default=None)
    urls_rule = Column(JsonType, default=None)
    next_page_type = Column(EvalString(20), default=None)
    next_page_rule = Column(JsonType, default=None)
    weight = Column(Integer, default=10)
    post_params = Column(JsonType, default='{}')
    http_method = Column(String(10), default='get')
    create_time = Column(DateTime, server_default=func.now(), nullable=False)
    update_time = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), nullable=False)
    creator_id = Column(Integer, default=-1)
    plugin = Column(BLOB)
    label = Column(String(40), nullable=True, unique=False)

    def copy(self, parse_config):
        if not parse_config:
            return False
        self.name = parse_config.name
        self.topic_id = parse_config.topic_id
        self.url_format = parse_config.url_format
        self.datas_type = parse_config.datas_type
        self.datas_rule = json.dumps(parse_config.datas_rule)
        self.urls_rule = json.dumps(parse_config.urls_rule)
        self.next_page_type = parse_config.next_page_type
        self.next_page_rule = json.dumps(parse_config.next_page_rule)
        self.weight = parse_config.weight
        self.post_params = json.dumps(parse_config.post_params)
        self.http_method = parse_config.http_method
        self.create_time = parse_config.create_time
        self.update_time = parse_config.update_time
        self.creator_id = parse_config.creator_id
        self.plugin = parse_config.plugin
        return True


class Topic(Base):
    __tablename__ = "topic"
    __table_args__ = {
        'mysql_engine': 'innodb',
        'mysql_charset': 'utf8'
    }

    id = Column(Integer(), primary_key=True, autoincrement=True)
    name = Column(String(64), nullable=False, unique=True)
    table_name = Column(String(64), nullable=False, unique=True)
    description = Column(String(256), nullable=False, unique=True)
    schema = Column(JsonType)
    ctime = Column(DateTime, server_default=func.now(), nullable=False)
    utime = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), nullable=False)


class Site(Base):
    __tablename__ = "site"
    __table_args__ = {
        'mysql_engine': 'innodb',
        'mysql_charset': 'utf8'
    }

    id = Column(Integer(), primary_key=True, autoincrement=True)
    name = Column(String(40), nullable=False, unique=True)
    avg_interval = Column(Float(), nullable=False)
    list_display_fields = Column(String(1024))
    mongo_table_name = Column(String(30))
    topic_id = Column(Integer())
    ctime = Column(DateTime, server_default=func.now(), nullable=False)
    site = Column(String(128), nullable=False)
    site_id = Column(Integer())
    label = Column(JsonType)
    encoding = Column(String(30))

    def copy(self, site):
        import time
        if not site:
            return False
        self.name = site.name
        self.avg_interval = site.avg_interval
        self.list_display_fields = site.list_display_fields
        self.mongo_table_name = site.mongo_table_name
        self.topic_id = site.topic_id
        self.ctime = site.ctime
        self.site = site.site + '_copy_' + str(int(time.time()))
        self.site_id = site.site_id
        return True


class Seeds(Base):
    __tablename__ = 'seeds'
    __table_args__ = {
        'mysql_engine': 'innodb',
        'mysql_charset': 'utf8'
    }

    id = Column(Integer(), primary_key=True, autoincrement=True)
    site_id = Column(Integer(), nullable=False)
    name = Column(String(64))
    url = Column(EvalString(1000), nullable=False)
    mode = Column(String(10), nullable=False)
    download_type = Column(String(10), nullable=False)
    priority = Column(String(10), nullable=False, default=0)
    doc_type = Column(String(10), nullable=False, default=0)
    page_num = Column(String(10), nullable=False, default=0)
    check_body = Column(String(100))
    check_body_not = Column(String(100))
    check_size = Column(String(10))
    timeout = Column(String(10))
    max_tries = Column(String(10))
    proxy_time = Column(String(10))
    crawl_item = Column(String(10))
    item_check_body = Column(String(100))
    item_check_body_not = Column(String(100))
    item_download_type = Column(String(10))
    cat = Column(String(100))
    is_once = Column(String(10))
    data = Column(JsonType(1024), default='{}')
    method = Column(String(10), default='get')
    max_dup_count_exit = Column(Integer())
    variable_params = Column(JsonType(1024), default='{}')
    site = Column(String(128), nullable=False)
    proxy = Column(Text, nullable=False)
    session_commit = Column(Text, nullable=False)
    config_init_period = Column(JsonType, nullable=False)

    def copy(self, seeds):
        if not seeds:
            return False
        self.site_id = seeds.site_id
        self.name = seeds.name
        self.url = seeds.url
        self.mode = seeds.mode
        self.download_type = seeds.download_type
        self.priority = seeds.priority
        self.doc_type = seeds.doc_type
        self.page_num = seeds.page_num
        self.check_body = seeds.check_body
        self.check_body_not = seeds.check_body_not
        self.check_size = seeds.check_size
        self.timeout = seeds.timeout
        self.max_tries = seeds.max_tries
        self.proxy_time = seeds.proxy_time
        self.crawl_item = seeds.crawl_item
        self.item_check_body = seeds.item_check_body
        self.item_check_body_not = seeds.item_check_body_not
        self.item_download_type = seeds.item_download_type
        self.cat = seeds.cat
        self.is_once = seeds.is_once
        self.data = json.dumps(seeds.data)
        self.method = seeds.method
        self.max_dup_count_exit = seeds.max_dup_count_exit
        self.variable_params = json.dumps(seeds.variable_params)
        self.site = seeds.site
        self.proxy = seeds.proxy
        self.session_commit = seeds.session_commit
        return True


class DataQuery(Base):
    __tablename__ = "data_query"
    __table_args__ = {
        'mysql_engine': 'innodb',
        'mysql_charset': 'utf8'
    }

    id = Column(Integer(), primary_key=True, autoincrement=True)
    user_id = Column(Integer(), nullable=False, unique=True)
    topic_id = Column(Integer(), nullable=False, unique=True)
    query = Column(Text, nullable=False)
    ctime = Column(DateTime, server_default=func.now(), nullable=False)
    utime = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), nullable=False)