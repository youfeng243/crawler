#coding=utf8
import copy
import time
from db_process import Dbprocess
import traceback
from multiprocessing import Pool, Process, Queue
import re
import multiprocessing


class BaseEtl:
    def __init__(self,conf):
        self.log              = conf.get('log_etl')
        self.db_obj           = Dbprocess(conf)
        self.conf             = conf
        self.max_insert_data  = 50
        self.max_process_data = 50
        self.manager          = multiprocessing.Manager()
        self.q_data           = self.manager.Queue()
        self.q_lock           = self.manager.Lock()


    def process_data(self,item):
        etl_data = copy.deepcopy(item)

        return etl_data

    def etl(self):
        '''清洗总入口'''

        return True



