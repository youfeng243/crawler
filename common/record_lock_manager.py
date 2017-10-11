import redis
import datetime
import uuid
from redlock import Redlock
import pickle
import random
import os
import re
from threading import Lock


class Task:
    def __init__(self, name, prefix, data, is_backlog=False, postfix=None):
        self.name = name
        self.lock_name = prefix + self.name
        self.pickle_name = self.lock_name + "-backlog."
        if postfix is None:
            self.pickle_name += str(uuid.uuid4())
        else:
            self.pickle_name += postfix
        self.data = data
        self.lock = None
        self.is_backlog = is_backlog
        self.saved_context = None


class RecordLockManager(object):
    def __init__(self, conf, prefix=None, ttl_sec=60, backlog_path=None):
        self.backlog_lock = Lock()
        self.conf = conf
        self.redlock = Redlock([{"host": conf['backend']['host'], "port": int(conf['backend']['port']), "db": 13, "password": conf['backend']['password']}, ],
                               retry_count=100000, retry_delay=0.01)
        if prefix is not None:
            self.prefix = prefix + "-"
        self.ttl = ttl_sec * 1000
        self.backlog_filename_pat = re.compile(self.prefix + '([\w\d\.\-\\/]+?)\-backlog\.([\w\d\-]+)')
        self.backlog = {}
        self.backlog_path = backlog_path # Should be abs path to the dir containing backlog pickles
        self.is_dumping = False
        if not os.path.exists(self.backlog_path):
            os.makedirs(self.backlog_path)
        self.reload()


    def parse_backlog_filename(self, name):
        match = self.backlog_filename_pat.search(name)
        if match is not None:
            return match.group(1), match.group(2)
        else:
            return None, None

    def reload(self):
        for i in os.listdir(self.backlog_path):
            if os.path.isfile(os.path.join(self.backlog_path, i)):
                name, postfix = self.parse_backlog_filename(i)
                if name is not None:
                    with open(self.make_real_path(i), 'rb') as ifile:
                        a = pickle.load(ifile)
                        (data, saved_context) = a
                        task = self.make_task(name, data, postfix=postfix)
                        task.saved_context = saved_context
                        task.is_backlog = True
                        self.backlog[task.pickle_name] = task

    def make_real_path(self, pickle_name):
        if self.backlog_path is not None:
            return os.path.join(self.backlog_path, pickle_name)
        else:
            return pickle_name

    # TODO: implement exponential back off
    def choose_backlog_and_lock_it(self, ttl_sec=None, retry=5):
        with self.backlog_lock:
            count = 0
            while count < retry:
                if len(self.backlog) < 1:
                    return None
                backlog_task = self.backlog[random.choice(self.backlog.keys())]
                if backlog_task.lock is not None:
                    # this task is already locked, maybe by other thread
                    continue
                count+=1
                self.try_lock(backlog_task, ttl_sec)
                if not backlog_task.lock:
                    return None
                else:
                    return backlog_task
            return None


    def try_lock(self, task, ttl_sec=None, block=False):
        if ttl_sec is not None:
            ttl = ttl_sec * 1000
        else:
            ttl = self.ttl
        lock = self.redlock.lock(task.lock_name, ttl)
        while block and not lock:
            lock = self.redlock.lock(task.lock_name, ttl)
        if not lock:
            task.lock = None
        else:
            task.lock = lock

    def store_backlog(self, task, saved_context):
        print "Store backlog " + task.lock_name
        self.is_dumping = True
        task.saved_context = saved_context
        with open(self.make_real_path(task.pickle_name), "wb") as ofile:
            pickle.dump((task.data, saved_context), ofile)
        self.is_dumping = False
        with self.backlog_lock:
            # Must ensure task holds no lock before store it as backlog
            # otherwise, it may never scheduled again
            if task.lock is not None:
                self.unlock(task.lock)
                task.lock = None
            task.is_backlog = True
            self.backlog[task.pickle_name] = task

    def make_task(self, name, data, postfix=None):
        return Task(name, self.prefix, data, is_backlog=False, postfix=postfix)

    def unlock(self, lock):
        self.redlock.unlock(lock)

    def commit_task(self, task):
        if task.is_backlog:
            with self.backlog_lock:
                if task.pickle_name in self.backlog:
                    del self.backlog[task.pickle_name]
                real_path = self.make_real_path(task.pickle_name)
                if os.path.exists(real_path):
                    os.remove(real_path)
        if task.lock is not None:
            self.unlock(task.lock)
            task.lock = None
