#!/usr/bin/env bash

fileCount=`sudo -u gtt hadoop fs -count /tmp/ericsson_new/staging/ | awk '{print $2}'`

if [ $fileCount -eq 0 ]; then
        echo 'No new airspan files to move...'
        exit 0
fi

sudo -u gtt hadoop fs -cp -f /tmp/ericsson_new/staging/* /tmp/ericsson_new/complete/
sudo -u gtt hadoop fs -rm -r /tmp/ericsson_new/staging/*






