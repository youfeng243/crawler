# -*- coding: utf-8 -*-
import os
import sys
SCRIPT_PATH = os.path.dirname(__file__)
sys.path.append(os.path.join(SCRIPT_PATH, '..'))
sys.path.append(os.path.join(SCRIPT_PATH, '../..'))
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol

from bdp.i_crawler.i_data_saver import DataSaverService
from bdp.i_crawler.i_data_saver.ttypes import *

from bdp.i_crawler.i_entity_extractor.ttypes import *

if __name__ == '__main__':

    if len(sys.argv) <= 1 or sys.argv[1] == '--help':
        print ''
        print 'Usage: ' + sys.argv[0] + ' function [arg1 [arg2...]]'
        print ''
        print 'Functions:'
        print '  DataSaverRsp check_data(EntityExtractorInfo entity)'
        print '  DataSaverRsp get_schema(i32 topic_id)'
        print '  DataSaverRsp reload()'
        print ''
        sys.exit(0)

    import conf
    socket = TSocket.TSocket('127.0.0.1', conf.port)
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = DataSaverService.Client(protocol)
    transport.open()

    import pprint
    pp = pprint.PrettyPrinter(indent = 2)
    
    cmd = sys.argv[1]
    args = sys.argv[2:]
    if cmd == 'check_data':
        if len(args) != 1:
            print 'check_data requires 1 args'
            sys.exit(1)
        pp.pprint(client.check_data(eval(args[0]),))
    elif cmd == 'get_schema':
        if len(args) != 1:
            print 'get_schema requires 1 args'
            sys.exit(1)
        pp.pprint(client.get_schema(eval(args[0]),))
    elif cmd == 'reload':
        if len(args) != 0:
            print 'reload requires 0 args'
            sys.exit(1)
        pp.pprint(client.reload())
    else:
        print 'Unrecognized method %s' % cmd
        sys.exit(1)

    transport.close()
