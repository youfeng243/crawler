from thrift.protocol.TBinaryProtocol import TBinaryProtocol as TBinaryServerProtocol
from thrift.transport.TTransport import TMemoryBuffer
from i_util.pybeanstalk import PyBeanstalk
from bdp.i_crawler.i_downloader.ttypes import DownLoadRsp
from bdp.i_crawler.i_downloader.ttypes import CrawlStatus
from i_util.logs import LogHandler
def to_string(log,page_info):
    str_page_info = None
    try:
        tMemory_b = TMemoryBuffer()
        tBinaryProtocol_b = TBinaryServerProtocol(tMemory_b)
        page_info.write(tBinaryProtocol_b)
        str_page_info = tMemory_b.getvalue()
    except EOFError, e:
        log.warning("cann't write DownLoadRsp to string")
    return str_page_info


def put_beanstalked(beanstalk_conf,log,rsp):
    beanstalk = PyBeanstalk(beanstalk_conf['host'], beanstalk_conf['port'])
    tube = beanstalk_conf['input_tube']
    str_page_info = to_string(log,rsp)
    try:
        beanstalk.put(tube, str_page_info)
        log.info('beanstalk\turl:%s\ttube:%s' % (rsp.url, tube))
    except Exception as e:
        log.info(
            'beanstalk put error url:' + rsp.url + '\ttube:' + tube)
if __name__=="__main__":
    url='dsfsdf'
    redirect_url='sdfdfsfsfs'
    status=1
    http_code=200
    rsp=DownLoadRsp(url=url,redirect_url=redirect_url,status=status,http_code=http_code)
    beanstalk_conf={}
    beanstalk_conf['host']='101.201.102.37'
    beanstalk_conf['port'] = 11300
    beanstalk_conf['input_tube'] = 'download_rsp_test'
    log=LogHandler('download_test')
    put_beanstalked(beanstalk_conf,log,rsp=rsp)
