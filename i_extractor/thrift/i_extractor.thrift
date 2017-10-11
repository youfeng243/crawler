#!/usr/bin/env thrift --gen py -r -o ../thrift
include "../../i_downloader/thrift/i_downloader.thrift" 

namespace py bdp.i_crawler.i_extractor
enum ExStatus
{
    kEsNotExtract = 1, #未发生抽取
    kEsSuccess    = 2, #抽取成功
    kEsFail       = 3, #抽取失败
}
enum ExFailErrorCode # 志强定义好解析失败的错误码 
{
    kExFailParseRuleLimit       = 1, #无规则解析
    KExFailPageTranscoding      = 2, #页面转码失败
    kExFailPageParse            = 3, #规则解析错误
    kExFailParseNothing         = 4, #无解析结果
    kExFailNoContent            = 5, #网页内容为空
    KExFailRequireError         = 6, #必须字段为空
}


struct BaseInfo
{
    1:required string      url,             #必须字段, 页面原始url,先不区分原始和规范化的 *
    3:optional i64         url_id,          #可选字段，对于url求得唯一url;
    4:optional string      site,            #可选字段，url的站点 *
    5:optional i64         site_id,         #可选字段，子域的id 
    6:optional string      domain,          #可选字段，主域
    7:optional i64         domain_id,       #可选字段，主域的id
    8:optional i64         segment_id,      #可选字段，url片段id
    9:optional string      src_type,        #可选字段，数据来源
}
struct CrawlInfo
{
    1:optional i32         status_code,     #可选字段，下载错误码
    2:optional i32         http_code,       #可选字段，下载http_code
    3:optional i32         download_time,   #可选字段，下载时间
    4:optional string      redirect_url,    #可选字段，跳转的url
    5:optional i32         elapsed,         #可选字段，下载耗时
    6:optional string      content_type,    #可选字段，下载的类型
    7:optional string      content,         #可选字段，网页的内容
    8:optional i32         page_size,       #可选字段，网页大小
}
enum LinkType
{
    kUnknownLink       = 0x00,  # 未知连接类型
    kNavLink           = 0x01,  #
    kSecNavLink        = 0x02,  #
    kTitleLink         = 0x03,  #
    kSecondTitleLink   = 0x04,  #
    kSectionTitleLink  = 0x05,  # 分标题，即段落标题
    kContentLink       = 0x06,  # 内容页url
    kNextPageLink      = 0x07,  # 下一页url
    kTurnPageLink      = 0x08,  # 翻页url
    kDownLoadLink      = 0x09,  # 附件下载连接
}
enum StructType
{
    unknown_type       = 0,
    fail_type          = 1,
    index_type         = 2,
    content_type       = 4,
}
struct Link
{
    1:required string           url,            #必须字段，*
    2:optional i64              url_id,
    3:optional string           site,
    4:optional i64              site_id,
    5:optional string           domain,
    6:optional i64              domain_id,
    7:optional i64              segment_id,
    8:optional string           anchor,         #可选字段，瞄
    9:optional LinkType         type,           #连接类型,
    10:optional string          parse_extends,  #解析器可指定，解析的规则
    11:optional bool            is_new,         #是否为新连接
}
struct ExtractInfo
{
    1:required ExStatus             ex_status,       # 必须字段，抽取状态 *
    2:optional ExFailErrorCode      extract_error,   # 可选字段，抽取错误码 *
    3:optional string               redirect_url,    # 可选字段，跳转后提供最终内容的url *
    4:optional bool                 next_page_type,  # 是否是翻页的页面类型
    5:optional i32                  struct_type,     # 判断是内容页还是索引页
    6:optional i32                  compose_type,    # 页面板式分类
    7:optional i32                  content_type,    # 页面类型分类
    8:optional i32                  topic_id,            # 内容子分类 schema *
    9:optional i32                  extracted_body_time, # 页面内部抽取的时间
    10:optional i32                 content_time,        # 正文时间
    11:optional string              html_tag_title,      # 页面标题 *<title>
    12:optional string              analyse_title,       # 分析标题
    13:optional string              zone,                # 页面所在地区
    14:optional string              page_text,           # 正文内容 *
    15:optional string              content_language,    # 正文语言*
    16:optional string              second_navigate,     # 二级导航
    17:optional string              valid_pic_url,       # 有效图片url
    18:optional string              digest,              # 摘要 *
    19:optional string              finger_feature,      # 指纹特征
    20:optional i64                 content_finger,      # 内容指纹 *
    21:optional i64                 simhash_finger       # simhash指纹
    22:optional i64                 link_finger          # 链接指纹, 一般采用内链url *

    23:optional list<Link>          links,               # 提取的链接 *
    24:optional string              extract_data,        # 抽取出来的数据信息，json结构的字符串 *
}

struct PageParseInfo
{
    1:optional BaseInfo         base_info,
    2:optional CrawlInfo        crawl_info,
    3:optional ExtractInfo      extract_info,
    4:optional string           scheduler,
    5:optional string           parse_extends,
    6:optional string           data_extends,
}

struct ExtractRsp
{
    1:string status = '',
    2:string message = '',
}


service ExtractorService
{
    PageParseInfo extract(1:i_downloader.DownLoadRsp req),
    ExtractRsp   save_parser_config(1:string config_json),
    ExtractRsp   reload_parser_config(1:string parser_id)
}
