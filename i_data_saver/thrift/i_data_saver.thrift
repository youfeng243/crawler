#!/usr/local/bin/thrift --gen py -r -o ../thrift
include "../../i_entity_extractor/thrift/i_entity_extractor.thrift"

namespace py bdp.i_crawler.i_data_saver

struct DataSaverRsp
{
    1:optional i32          status,             # 成功校验为0， 不成功为1
    2:optional string       message,            # 校验结果的描述
    3:optional string       data,               # 返回所需数据，为json格式
}

service DataSaverService
{
    DataSaverRsp check_data(1:string data),
    DataSaverRsp get_schema(1:i32 topic_id),
    DataSaverRsp reload(1:i32 topic_id);
}
