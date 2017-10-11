#!/usr/bin/env thrift --gen py -r -o ../thrift
include "../../i_extractor/thrift/i_extractor.thrift"
include "../../i_downloader/thrift/i_downloader.thrift"
namespace py bdp.i_crawler.i_crawler_merge

#抓取相关
struct CrawlInfo{
        1:optional    i32               status_code,      # 抓取情况  0:下载成功
        2:optional    i32               http_code,        # 抓取返回码
        3:optional    i32               schedule_time,    # 调度时间
        4:optional    i32               download_time,    # 抓取时间
        5:optional    string            redirect_url,     # 可选字段,跳转的url
        6:optional    i32               elapsed,          # 可选字段,下载耗时
        7:optional    string            content_type,     # 可选字段,下载的类型
        8:optional    i32               page_size,        # 可选字段,网页大小
        9:optional    i32               seed_id,          # 种子id
}

#网页内容相关
struct PageInfo{
    1:optional    i64       content_finger,                 # 内容指纹
    2:optional    i64       link_finger,                    # 链接指纹
    3:optional    i64       inner_link_finger,              # 内链指纹
    4:optional    i32       page_size,                      # 页面大小
    5:optional    i32       page_type,                      # 网页类型
    6:optional    i32       page_struct,                    # 网页结构索引页还是内容页
    9:optional    i32       inner_links_num,                # 内链个数
    10:optional   i32       outer_links_num,                # 外链个数
    11:optional   i32       new_links_num_for_self,         # 本身的新连接数
    12:optional   i32       good_links_num_for_for_self,    # 本身的好新连接数
    13:optional   i32       new_links_num_for_all,          # 全局的新连接数
    14:optional   i32       good_links_num_for_all,         # 全局的好新连接数
    15:optional   i32       dead_page_type,                 # 死链类型,1：内容死链,2：协议死链,0：非死链
    16:optional   i32       dead_page_time,                 # 非0为死链时间
    17:optional   string    real_title,                     # 真实title
    18:optional   string    analyse_title,                  # 分析标题
    19:optional   i32       content_time,                   # 正文时间
}
#父链接属性
struct ParentInfo{
    1:optional    i32       depth,                      # 父连接深度
    2:optional    string    parent_page,                # 父链接,多父链选择一个最优质的
    3:optional    i32       parent_page_type,           # 父链接类型
    4:optional    i32       parent_download_time,       # 父链接下载时间
    5:optional    string    anchor,                     # 超链对url的描述
    6:optional    string    analyse_title,              # 父链的分析标题
}

#抓取历史
struct CrawlHistory {
    1:optional    i32   download_time,                  # 下载时间
    2:optional    i32   status_code,                    # 下载情况
    3:optional    i32   http_code,                      # 下载返回码
    4:optional    i64   content_finger,                 # 内容指纹
    5:optional    i64   link_finger,                    # 链接指纹
    6:optional    i64   inner_link_finger,              # 内链指纹
    7:optional    i32   page_size,                      # 页面大小
    8:optional    i32   page_type,                      # 网页类型
    9:optional    i32   inner_links_num,                # 内链个数
    10:optional   i32   outer_links_num,                # 外链个数
    11:optional   i32   new_links_num_for_self,         # 本身的新连接数
    12:optional   i32   good_links_num_for_for_self,    # 本身的好新连接数
    13:optional   i32   new_links_num_for_all,          # 全局的新连接数
    14:optional   i32   good_links_num_for_all,         # 全局的好新连接数
    15:optional   i32   dead_page_type,                 # 死链类型,1：内容死链,2：协议死链,0：非死链
    16:optional   i32   dead_page_time,                 # 非0为死链时间
    17:optional   string    real_title,                 # 真实title
    18:optional   string    analyse_title,              # 分析标题
    19:optional   i32   content_time,                   # 正文时间
}

# 抽取出来的信息
struct ExtractMessage {
    1:required i_extractor.ExStatus             ex_status,      # 必须字段,抽取状态
    2:optional i32                              topic_id,       # 可选字段,内容子分类 schema
    3:optional string                           extract_data,   # 抽取出来的数据信息，json结构的字符串
}

struct LinkAttr {
    1:required    string                url,                  # url
    2:required    i64                   url_id,               # url_id
    3:optional    string                src_type,             # url来源,人工回灌,或大库回灌等
    4:optional    i32                   found_time,           # 链接发现时间
    5:optional    i32                   depth,                # 链接深度,作为url与首页的差距
    6:optional    CrawlInfo             crawl_info,           # 抓取情况
    7:optional    PageInfo              page_info,            # 页面信息
    8:optional    ParentInfo            parent_info,          # 父链接信息
    9:optional    list<CrawlHistory>    normal_crawl_his,     # 临时抓取状况
    10:optional   string                data_extends,         # 扩展字段,用以后续扩展数据使用
    11:optional   string                del_reason,           # 删除原因
    12:optional   i32                   seed_id,              # 种子页
    13:optional   list<i_extractor.Link>    sub_links,        # 子链接列表
    14:optional   ExtractMessage            extract_message,  # 抽取出来的信息
}


service CrawlerMergeService
{
    LinkAttr merge(1:i_extractor.PageParseInfo page_parseinfo),
    i_downloader.RetStatus select(1:string site, 2:string url_format, 3:i32 limit, 4:i32 start, 5:string extra_filter 6:string tube_name),
    i_downloader.DownLoadRsp select_one(1:string url),
}
