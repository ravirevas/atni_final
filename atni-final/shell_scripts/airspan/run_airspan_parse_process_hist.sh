#!/usr/bin/env bash

fileCount=`hadoop fs -count /etl/gtt/airspan/hist/staging/ | awk '{print $2}'`

if [ $fileCount -eq 0 ]; then
        echo 'No new airspan files to process...'
        exit 0
fi

cd $ATNI_REPO/shell_scripts/airspan/
./run_airspan_parser.sh /etl/gtt/airspan/hist/staging/

cd $ATNI_REPO/shell_scripts/airspan/
./refresh_airspan_tables.sh
