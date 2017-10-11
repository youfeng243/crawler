#!/usr/bin/env thrift --gen py -r -o ../thrift
include "../../i_extractor/thrift/i_extractor.thrift"
namespace py bdp.i_crawler.i_entity_extractor

struct EntitySource
{
    1:optional string       url,            # 来源url
    2:optional i64          url_id,         # 来源站点的唯一id
    3:optional string       site,           # 来源站点
    4:optional i64          site_id,        # 来源站点的唯一id
    5:optional string       domain,         # 可选字段，子域的id
    6:optional i64          domain_id,      # 可选字段，主域的id
    7:optional string       src_type,       # 可选字段，数据来源
    8:optional  i32         download_time,  # 网页下载时间
}

struct EntityExtractorInfo
{
    1:optional  EntitySource     entity_source,          # 实体抽取的来源
    2:optional  i32              update_time             # 实体抽取时间
    3:optional  i32              topic_id,               # topic_id
    4:optional  string           entity_data,            # json格式，实体解析出来的最终数据
}

struct ResultResp
{
   1:optional i32          code,               # 成功为1， 不成功为-1
   2:optional string       msg,                # 结果的描述
   3:optional string       data,               # 返回所需数据，为json格式
}

struct EntityResp
{
    1:optional i32                        code,
    2:optional string                     msg,
    3:optional list<EntityExtractorInfo>  entity_data_list,
}

service EntityExtractorService
{
    EntityResp entity_extract(1:i_extractor.PageParseInfo req),
    ResultResp reload(1:i32 topic_id),
    ResultResp add_extractor(1:string extractor_info),
    ResultResp add_topic(1:string topic_info),
}

