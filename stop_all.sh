#!/bin/bash
BIN_PATH=`dirname $0`
cd ${BIN_PATH}
sh env.sh

for d in `echo i_scheduler i_downloader   i_extractor i_entity_extractor i_data_saver i_crawler_merge i_manager`
do
    cd $d
        sh run.sh stop
    cd ..
done 

