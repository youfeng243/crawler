#!/usr/bin/env python
# -*- coding:utf-8 -*-
import json

from sqlalchemy import Column, String, Integer, MetaData, BLOB,DateTime,types
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()
class JsonType(types.TypeDecorator):
    impl = types.TEXT
    def process_bind_param(self, value, engine):
        if isinstance(value, basestring):
            return value
        else:
            return json.dumps(value)

    def process_result_value(self, value, engine):
        ret = None
        if value:
            try:
                #print value
                ret = json.loads(value.strip())
            except Exception,e:
                pass
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
            except Exception,e:
                pass
        return ret

class HistoryExtractorConfig(Base):
    __tablename__ = "history_parser_config"
    __table_args__ = {
        'mysql_engine': 'innodb',
        'mysql_charset': 'utf8'
    }
    metadata = MetaData()
    id = Column(Integer, nullable=False, autoincrement=True, primary_key=True, index=True)
    rel_id = Column(Integer, nullable=False)
    name = Column(String(40), nullable=False)
    topic_id = Column(Integer, nullable=True)
    url_format = Column(EvalString(2048), nullable=False)
    datas_type = Column(EvalString(10), nullable=True, default=None)
    datas_rule = Column(JsonType, nullable=True, default=None)
    urls_rule = Column(JsonType, default=None)
    next_page_type = Column(EvalString(20), default=None)
    next_page_rule = Column(JsonType, default=None)
    weight = Column(Integer, default=10)
    post_params = Column(JsonType, default={})
    http_method = Column(String(10), default='get')
    create_time = Column(DateTime, server_default =func.now(), nullable=False)
    creator_id =Column(Integer, default=-1)
    plugin = Column(BLOB)


class ExtractorConfig(Base):
    __tablename__ = "parser_config"
    __table_args__ = {
        'mysql_engine': 'innodb',
        'mysql_charset': 'utf8'
    }
    metadata = MetaData()
    id = Column(Integer, nullable=False, autoincrement=True, primary_key=True, index=True)
    name = Column(String(40), nullable=False)
    topic_id = Column(Integer, nullable=True)
    url_format = Column(EvalString(512), nullable=False)
    datas_type = Column(EvalString(10), nullable=True, default=None)
    datas_rule = Column(JsonType, nullable=True, default=None)
    urls_rule = Column(JsonType, default=None)
    next_page_type = Column(EvalString(20), default=None)
    next_page_rule = Column(JsonType, default=None)
    weight = Column(Integer, default=10)
    post_params = Column(JsonType, default={})
    http_method = Column(String(10), default='get')
    create_time = Column(DateTime, server_default =func.now(), nullable=False)
    update_time = Column(DateTime, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), nullable=False)
    creator_id =Column(Integer, default=-1)
    plugin = Column(BLOB)
    label = Column(String(64), nullable=True)



if __name__ == "__main__":
    pass
    #print ParseConfigModel.__table__
