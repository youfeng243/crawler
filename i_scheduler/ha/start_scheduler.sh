#!/usr/bin/env bash

set -e

cd /home/litao/monit-5.21.0/bin
./monit
./monit start scheduler
