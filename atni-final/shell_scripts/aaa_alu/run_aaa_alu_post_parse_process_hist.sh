#!/usr/bin/env bash

fileCount=`sudo -u gtt hadoop fs -count /etl/gtt/aaa_alu/hist/staging/ | awk '{print $2}'`

if [ $fileCount -eq 0 ]; then
        echo 'No historic aaa_alu files to move...'
        exit 0
fi

sudo -u gtt hadoop fs -cp -f /etl/gtt/aaa_alu/hist/staging/* /etl/gtt/aaa_alu/complete/
sudo -u gtt hadoop fs -rm -r /etl/gtt/aaa_alu/hist/staging/*
