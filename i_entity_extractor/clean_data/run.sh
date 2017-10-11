#!/usr/bin/env sh
filename=$1
source ../../env.sh
nohup python acquirer_event/clean_acquirer_event.py &>acquirer_event.log  &
nohup python acquirer_event/remove_acquirer_event.py &>remove_acquirer_event.log  &
nohup python exit_event/clean_exit_event.py &>exit_event.log  &
nohup python financing_events/clean_financing_events.py &>financing_events.log  &
nohup python fygg/clean_fygg.py &>fygg.log  &
nohup python fygg/remove_data.py &>remove_data.log  &
nohup python investment_institutions/clean_investment_institutions.py &>investment_institutions.log  &
nohup python judge_process/clean_judge_process.py &>judge_process.log  &
nohup python ktgg/clean_ktgg.py &>ktgg.log  &
nohup python land_auction/clean_land_auction.py &>land_auction.log  &
nohup python land_project_selling/clean_land_project_selling.py &>land_project_selling.log  &
nohup python land_selling_auction/clean_land_selling_auction.py &>land_selling_auction.log  &
nohup python news/clean_baidunews.py &>baidunews.log  &
nohup python owx/clean_owx.py &>owx.log  &
nohup python ppp_project/clean_ppp_project.py &>ppp_project.log  &
nohup python zhixing/clean_zhixing.py &>zhixing.log  &
nohup python wenshu/clean_wenshu.py &>wenshu.log  &
echo start_success