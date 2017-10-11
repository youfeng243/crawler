# coding=utf-8

import sys
sys.path.append('..')

host = '127.0.0.1'
port = 12400
server_thread_num = 1
server_process_num = 1

process_thread_num = 1

# beanstalk_conf = {
#     'host': '101.201.102.37',  # 线上beanstalk内网IP
#     'port': 11300,  # 线上beanstalk内网port
#     'input_tube': 'extract_to_crawl',
#     'output_tube': 'scheduler_info',
# }
beanstalk_conf = {
    'host': '127.0.0.1',  # 线上beanstalk内网IP
    'port': 11300,  # 线上beanstalk内网port
    'input_tube': 'extract_to_crawl',
    'output_tube': 'scheduler_info',
}

mysql_host = '101.201.102.37'
mysql_user = 'root'
mysql_passwd = 'haizhi@)'
mysql_db = 'cmb_crawl'

download_output_tube = 'czj_download_rsp'

from i_util.logs import LogHandler

log = LogHandler("crawler_merge")
