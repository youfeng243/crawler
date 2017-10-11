#!/usr/bin/env thrift --gen py -r -o ../thrift
include "../../i_extractor/thrift/i_extractor.thrift"
namespace py bdp.i_crawler.single_source_merge


struct ResultResp
{
   1:optional i32          code,               # 成功为1， 不成功为-1
   2:optional string       msg,                # 结果的描述
   3:optional string       data,               # 返回所需数据，为json格式
}


service SingleSourceMergeService
{
    string do_merge(1:i_extractor.PageParseInfo req),
    ResultResp reload(1:i32 topic_id),
}

