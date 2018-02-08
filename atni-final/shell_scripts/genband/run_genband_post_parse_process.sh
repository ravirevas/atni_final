#!/usr/bin/env bash

fileCount=`sudo -u gtt hadoop fs -count /tmp/genband_new/staging/ | awk '{print $2}'`

if [ $fileCount -eq 0 ]; then
        echo 'No new genband files to move...'
        exit 0
fi

sudo -u gtt hadoop fs -cp -f /tmp/genband_new/staging/* /tmp/genband_new/complete/
sudo -u gtt hadoop fs -rm -r /tmp/genband_new/staging/*


