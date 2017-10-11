#!/usr/bin/env python
#-*- coding:utf-8 -*-
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from thrift.transport.TTransport import TMemoryBuffer


class UnicodeTBinaryProtocol(TBinaryProtocol):
    def writeString(self, _str):
        if isinstance(_str, unicode):
            _str = _str.encode('utf-8')
        self.writeI32(len(_str))
        self.trans.write(_str)

def slug_unicode_handler(obj):
    tMemory_b = TMemoryBuffer()
    tBinaryProtocol_b = UnicodeTBinaryProtocol(tMemory_b)
    obj.write(tBinaryProtocol_b)
    str_parse = tMemory_b.getvalue()
    tMemory_o = TMemoryBuffer(str_parse)
    tBinaryProtocol_o = TBinaryProtocol(tMemory_o)
    obj.read(tBinaryProtocol_o)
    return obj



if __name__ == "__main__":
    pass
    #print get_url_info("http://www.baidu.com/index")