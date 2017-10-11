#!/usr/bin/env
# -*- coding:utf-8 -*-
import pymongo
import sys

# get mongo table
def getdb(kw):
    dbhost = kw['host']
    dbport = kw['port']
    dbname = kw['db']
    dbuser = kw['username']
    dbpass = kw['password']
    try:
        conn = pymongo.MongoClient(host=dbhost, port=int(dbport), connectTimeoutMS=30 * 60 * 1000,
                                   serverSelectionTimeoutMS=30 * 60 * 1000)
        db = conn[dbname]
        if dbuser and dbpass:
            connected = db.authenticate(dbuser, dbpass)
        else:
            connected = True
    except Exception as e:
        print e
        print dbhost, dbport, dbname, dbuser, dbpass
        print 'Connect Statics Database Fail.'
        sys.exit(1)
    return db


if __name__ == "__main__":
    tables=["acquirer_event","ppp_project","land_auction",
            "land_project_selling","ershoufang_lianjia","xiaoqu_lianjia",
            "loupan_lianjia","investment_funds","listing_events",
            "inventment_events","financing_events","exit_event",
            "investment_institutions","net_loan_blacklist"]
    kw =  {
        "host": "172.16.215.36",
        "port": '40042',
        "db": "final_data",
        "username": "work",
        "password": "haizhi",
    }
    final_data=getdb(kw)
    for item in tables:
        table=final_data[item]
        count=table.find().count()
        print item+"\tcount:"+str(count)
        all_data=[]
        for record in table.find():
            record_id = record['_record_id']
            if record_id in all_data:
                print record_id
                try:
                    #result=table.remove({'_record_id':record_id})
                    pass
                except Exception as e:
                    pass
            else:
                all_data.append(record_id)




