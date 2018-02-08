#!/usr/bin/env bash

fileCount=`sudo -u gtt hadoop fs -count /tmp/airspan_new/staging/ | awk '{print $2}'`

if [ $fileCount -eq 0 ]; then
        echo 'No new airspan files to move...'
        exit 0
fi

sudo -u gtt hadoop fs -cp -f /tmp/airspan_new/staging/* /etl/gtt/airspan/complete/
sudo -u gtt hadoop fs -rm -r /tmp/airspan_new/staging/*


