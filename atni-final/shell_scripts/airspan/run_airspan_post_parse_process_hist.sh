#!/usr/bin/env bash

fileCount=`sudo -u gtt hadoop fs -count /etl/gtt/airspan/hist/staging/ | awk '{print $2}'`

if [ $fileCount -eq 0 ]; then
        echo 'No historic airspan files to move...'
        exit 0
fi

sudo -u gtt hadoop fs -cp -f /etl/gtt/airspan/hist/staging/* /etl/gtt/airspan/complete/
sudo -u gtt hadoop fs -rm -r /etl/gtt/airspan/hist/staging/*


