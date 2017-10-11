#! /usr/bin/env python
# coding=utf-8

import sys
from hbase.ttypes import *
# from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol, TCompactProtocol
from hbase import THBaseService
import json
import requests

# Thrift1 interface demo, not used
# from hbase1.ttypes import *
#
# def test1():
#     from thrift import Thrift
#     from thrift.transport import TSocket
#     from thrift.transport import TTransport
#     from thrift.protocol import TBinaryProtocol
#     from hbase1 import Hbase
#
#     transport = TSocket.TSocket('54.223.119.104', 9091)
#     transport = TTransport.TBufferedTransport(transport)
#     protocol = TBinaryProtocol.TBinaryProtocol(transport)
#     client = Hbase.Client(protocol)
#     transport.open()
#     print(client.getTableNames())



class HBaseThrift2Connection:

    DEFAULT_CFS = ['docs']

    def __init__(self, ip="localhost", port=9090, rest_port=8080, conf=None):
        if conf is None:
            self.port = port
            self.ip = ip
            self.rest_port = rest_port
        else:
            self.port = conf["port"]
            self.ip = conf["host"]
            self.rest_port = conf["rest_port"]
        self.transport = TSocket.TSocket(self.ip, self.port)
        self.transport = TTransport.TBufferedTransport(self.transport)
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = THBaseService.Client(self.protocol)
        self.transport.open()
        self.base_url = "http://%s:%d" % (self.ip, self.rest_port)

    def reopen(self):
        self.transport.close()
        self.transport.open()

    def close(self):
        self.transport.close()

    def get(self, table_name, rowkey):
        res = {}
        get = TGet()
        get.row = rowkey
        try:
            result = self.client.get(table_name, get)
        except Exception, e:
            self.reopen()
            result = self.client.get(table_name, get)
        for column in result.columnValues:
            res[unicode(column.qualifier)] = unicode(column.value)
        return res

    def scan(self, table_name, rc=100, cols=None):
        scannerId = self.client.openScanner(table_name, TScan())
        res = self.client.getScannerRows(scannerId, rc)
        rows = []
        while res:
            for item in res:
                row = {}
                for col in item.columnValues:
                    if cols is None or col.qualifier in cols:
                        row[col.qualifier] = json.loads(col.value, strict=False)
                rows.append(row)
            res = self.client.getScannerRows(scannerId, rc)
        self.client.closeScanner(scannerId)
        return rows


    def put(self, table_name, rowkey, col_family, col, value):
        colVal = TColumnValue(col_family, col, value)
        put = TPut(rowkey, [colVal])
        try:
            self.client.put(table_name, put)
        except Exception, e:
            self.reopen()
            self.client.put(table_name, put)


    def put_multi(self, table_name, rowkey, col_family, col_defs):
        puts = []
        for k in col_defs:
            colVal = TColumnValue(col_family, k, col_defs[k])
            put = TPut(rowkey, [colVal])
            puts.append(put)
        try:
            self.client.putMultiple(table_name, puts)
        except Exception, e:
            self.reopen()
            self.client.putMultiple(table_name, puts)

    def delete_row(self, table_name, rowkey):
        delete = TDelete(rowkey)
        try:
            self.client.deleteSingle(table_name, delete)
        except Exception, e:
            self.reopen()
            self.client.deleteSingle(table_name, delete)

    def delete_col(self, table_name, rowkey, col):
        col = TColumn(family='docs', qualifier=col)
        delete = TDelete(row=rowkey, columns=[col])
        try:
            self.client.deleteSingle(table_name, delete)
        except Exception, e:
            self.reopen()
            self.client.deleteSingle(table_name, delete)

    def delete_cols(self, table_name, rowkey, cols):
        cols = [TColumn(family='docs', qualifier=col) for col in cols]
        delete = TDelete(row=rowkey, columns=cols)
        try:
            self.client.deleteSingle(table_name, delete)
        except Exception, e:
            self.reopen()
            self.client.deleteSingle(table_name, delete)

    def list_table(self):
        response = requests.get(self.base_url + '/', headers={"Accept": "application/json"})
        return [item['name'] for item in response.json()['table']]

    def create_table(self, table_name, cfs=DEFAULT_CFS):
        if len(cfs) < 1:
            raise Exception("need at least one column family")
        cf_descs = "".join(['<ColumnSchema name="%s"/>' % cf for cf in cfs])
        table_desc = '<?xml version="1.0" encoding="UTF-8"?><TableSchema name="%s">%s</TableSchema>' % (table_name, cf_descs)
        response = requests.post(self.base_url + '/%s/schema' % table_name, data=table_desc, headers={'content-type': 'text/xml'})
        response.raise_for_status()

    def drop_table(self, table_name):
        response = requests.delete(self.base_url + '/%s/schema' % table_name)
        response.raise_for_status()

    def truncate_table(self, table_name, cfs=DEFAULT_CFS):
        tables = self.list_table()
        if table_name not in tables:
            raise Exception("No such hbase table as " + table_name)
        self.drop_table(table_name)
        self.create_table(table_name, cfs)

if __name__ == "__main__":
    conn = HBaseThrift2Connection("localhost", 9090, rest_port=8080)
    # conn.delete_row("judgement_wenshu", "AAA")
    conn.create_table("aaa")
    conn.truncate_table('aaa')

    conn.put('aaa', 'bbb', 'docs', 'col1', 'v1')
    conn.put('aaa', 'bbb', 'docs', 'col2', 'v2')
    conn.delete_col('aaa', 'bbb', 'col1')

    print conn.get('aaa', 'bbb')