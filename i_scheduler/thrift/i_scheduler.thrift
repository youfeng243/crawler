#!/usr/local/bin/thrift --gen py -r -o ../thrift
include "../../i_crawler_merge/thrift/i_crawler_merge.thrift"

namespace py bdp.i_crawler.i_scheduler


service SchedulerService
{
    bool start_one_site_tasks(1:string site),
    bool stop_one_site_tasks(1:string site),
    bool clear_one_site_cache(1:string site),
    bool restart_seed(1:string seed_id, 2:string site)
}
