# coding=utf-8
from Queue import PriorityQueue
import threading, time, redis, traceback, json, sys
import pytoml
import getopt

from config_loader import SchedulerConfigLoader 
sys.path.append('..')
from i_util.tools import str_obj
from i_util.logs import LogHandler


class SeedSelector(threading.Thread):

    def __init__(self, scheduler, selector_conf, log):
        threading.Thread.__init__(self)
        self.daemon = True
        self.running = False
        self.scheduler = scheduler
        self.log = log
        self.selector_conf = selector_conf
        
    def _select_seed(self):
        try:
            self._load_fail_task()
        except:
            self.log.error('select_seed\terror:' + traceback.format_exc())

    def run(self):
        self.running = True
        while self.running:
            try:
                self.scheduler.select_seed()
                self.scheduler.get_schedule_seeds()
                time.sleep(self.selector_conf['select_seed_sleep_time'])
            except Exception, e:
                self.log.error('SeedSelector\terror:' + traceback.format_exc())

    def stop(self):
        self.running = False


class IndexSelector(object):

    def __init__(self, scheduler, conf):
        threading.Thread.__init__(self)
        self.daemon = True
        self.running = False
        self.scheduler, self.conf = scheduler, conf
        pass

    def run(self):
        while True:
            time.sleep(10)
            pass


class ItemSelector(object):

    def __init__(self, scheduler, conf):
        threading.Thread.__init__(self)
        self.daemon = True
        self.running = False
        self.scheduler, self.conf = scheduler, conf
        pass

    def run(self):
        while True:
            time.sleep(10)
            pass


class Selector(object):

    def __init__(self, scheduler, conf):
        self.scheduler = scheduler
        self.conf = conf
        self.seed_selector = SeedSelector(self.scheduler, self.conf['selector_conf'], self.conf['log'])
        #self.index_selector = IndexSelector(self.scheduler, self.conf)
        #self.item_selector = ItemSelector(self.scheduler, self.conf)

    def start(self):
        self.seed_selector.start()
        #self.index_selector.start()
        #self.item_selector.start()

    def stop(self):
        self.seed_selector.stop()
        #self.index_selector.stop()
        #self.item_selector.stop()


def usage():
    pass

if __name__ == '__main__':
    try:
        file_path = './scheduler.toml'
        opt, args = getopt.getopt(sys.argv[1:], 'f:', ['help'])
        for name, value in opt:
            if name == "-f":
                file_path = value
            elif name in ("-h", "--help"):
                usage()
                sys.exit()
            else:
                assert False, "unhandled option"

        with open(file_path, 'rb') as config:
            conf = pytoml.load(config)
            conf['log']=LogHandler(conf['server']['name']+str(conf['server']['port']))
        
        selector = Selector(None, conf)
        selector.start()
        time.sleep(1000)

    except getopt.GetoptError:
        sys.exit()
