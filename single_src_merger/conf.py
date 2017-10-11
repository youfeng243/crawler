# coding=utf-8
import sys
sys.path.append('..')


class conf(object):
    def __init__(self):
        pass
        self.server = {
            "host" : '127.0.0.1',
            "port" : 12500,
            "server_thread_num"  : 120,
            "server_process_num" : 1,
            "process_thread_num" : 20
        }
        self.extract_state = {
            "Extract_NotExtract" : 1,
            "Extract_Success" : 2,
            "Extract_Fail" : 3
        }
        self.beanstalk_conf  = {
            "host" : '101.201.100.58',  # 线上beanstalk内网IP
            "port" : 11300,  # 线上beanstalk内网port
            "input_tube" : 'extract_info_test',
            "output_tube" : 'entity_info'
        }
        self.MYSQL =  {
            "host" : '101.201.102.37',
            "port" : 3306,
            "username" : 'root',
            "password" : 'haizhi@)',
            "database" : 'cmb_crawl',
        }


        self.logname = 'entity_extractor'
        self.topic_map = {
            "32" : {
                "module_path" : "extractors/judge_wenshu/judge_wenshu_extractor.py",
                "module_name" : "JudgeWenshuExtractor"}
        }


config = conf()


def get_config(dict):
    config.server = dict.get('server')
    config.extract_state = dict.get('extract_state')
    config.beanstalk_conf = dict.get('beanstalk_conf')
    config.MYSQL = dict.get('MYSQL')
    config.logname = dict.get('logname')
    config.topic_map = dict.get('topic_map', {})
    config.hbase = dict.get('HBASE')
    config.backend = dict.get('backend')
    return config
