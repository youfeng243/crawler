# -*- coding:utf-8 -*-

from requests import Session
import traceback
from multiprocessing.dummy import Pool as ThreadPool
import time
from functools import partial
proxies_file = '../util/proxies_400.txt'
def read_proxies(proxies_file):
    with open(proxies_file) as fr:
        proxies = fr.read().splitlines()
        return proxies


# 默认工商系统测试
def test_proxy(proxy_dict, test_site='https://movie.douban.com/tag/', timeout=60):
    s = Session()
    s.proxies = proxy_dict
    try:
        start = time.time()
        resp = s.get(test_site, timeout=timeout)
        end = time.time()
        if resp.status_code == 200:
            print '{0} succeed'.format(proxy_dict['https'])
            return '%0.2f' % (end - start)
        else:
            print '{0} fail status_code:{1}'.format(proxy_dict['https'], resp.status_code)
            return -1
    except:
        print 'proxy:{0}\nexcept:{1}'.format(proxy_dict['https'], traceback.format_exc())
        return -2


def process_single(s_proxy, timeout=10, test_site='https://movie.douban.com/tag/'):
    proxy_dict = {'https': 'http://{0}'.format(s_proxy)}
    succeed_time = 0
    fail_time = 0
    total_time = 0
    for i in xrange(3):
        time.sleep(1)
        result = test_proxy(proxy_dict, test_site=test_site, timeout=timeout)
        if result > 0:
            succeed_time += 1
            total_time += float(result)
        else:
            fail_time += 1
    avg_time = 0
    if succeed_time > 0:
        avg_time = total_time / float(succeed_time)
    result_dict = {
        'proxy': s_proxy,
        'succeed_time': succeed_time,
        'fail_time': fail_time,
        'avg_times': avg_time,
    }
    return result_dict


def start_test_simple(timeout=10, thread_num=40, proxy_file_path='./proxies_400.txt'):
    print 'test start...'
    pool = ThreadPool(thread_num)
    proxies_txt = read_proxies(proxy_file_path)
    # 过滤代理为None或者为''
    to_test_proxies = [elem for elem in proxies_txt if elem is not None and elem is not '']
    # 定制测试函数(函数柯里化)
    process_single_timeout_s = partial(process_single, timeout=timeout)
    result = pool.map(process_single_timeout_s, to_test_proxies)

    fail_proxies = [elem for elem in result if elem['succeed_time'] == 0]
    succeed_proxies = [elem for elem in result if elem['succeed_time'] > 0]
    print 'test end-----------------'
    print 'timeout = {0}:\n\tsucceed:{1}\n\tfailed:{2}'.format(timeout, len(succeed_proxies), len(fail_proxies))
    print '-------------------------'
    print 'succeed:'
    all_succeed = succeed_proxies
    for single_succeed in all_succeed:
        print single_succeed
    print '------------------------'
    print 'fail:'
    #prox=Proxies(proxies_file)

    for single_fail in fail_proxies:
        #result = prox.sub_grade_with_proxy('https://movie.douban.com/tag/', single_fail['proxy'])

        print single_fail['proxy']


if __name__ == '__main__':
    start_test_simple(timeout=5, thread_num=50, proxy_file_path='./proxies_400.txt')
