#!/bin/bash
BIN_PATH=`dirname $0`
cd ${BIN_PATH}
sh env.sh

for d in `echo i_downloader   i_extractor   i_scheduler single_src_merger  i_data_saver i_entity_extractor i_manager i_crawler_merge`
do
    cd $d
        sh run.sh start
    cd ..
done 

