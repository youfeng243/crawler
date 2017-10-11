#!/home/work/env/python-2.7/bin/python
# coding=utf-8
import signal, sys, os
import traceback
sys.path.append('..')
from i_util.thread_pool import ThreadPool
from i_util.i_crawler_services import ThriftScheduler


def main():
    # if len(sys.argv) <= 2:
    #     print "error input"
    #     return 1;
    # type = sys.argv[1]
    # site = sys.argv[2]
    type = 'start'
    site = 'wenshu.court.gov.cn'
    scheduler_client = ThriftScheduler('127.0.0.1', 12100);
    if type == "start":
        print type, site, scheduler_client.start_one_site_tasks(site);
    elif type == "stop":
        print type, site, scheduler_client.stop_one_site_tasks(site);
    #print scheduler_client.stop_one_site_tasks('xyjg.egs.gov.cn');
    #print scheduler_client.start_one_site_tasks('movie.douban.com');

if __name__ == '__main__':
    main()
