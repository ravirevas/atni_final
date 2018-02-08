#!/usr/bin/env bash

fileCount=`sudo -u gtt hadoop fs -count /tmp/zteumts_new/staging/ | awk '{print $2}'`

if [ $fileCount -eq 0 ]; then
        echo 'No new zteumts files to move...'
        exit 0
fi

sudo -u gtt hadoop fs -cp -f /tmp/zteumts_new/staging/* /tmp/zteumts_new/complete/
sudo -u gtt hadoop fs -rm -r /tmp/zteumts_new/staging/*



