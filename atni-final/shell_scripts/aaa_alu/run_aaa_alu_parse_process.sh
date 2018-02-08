#!/usr/bin/env bash

fileCount=`hadoop fs -count /etl/gtt/aaa_alu/staging/ | awk '{print $2}'`

if [ $fileCount -eq 0 ]; then
        echo 'No new aaa_alu files to process...'
        exit 0
fi

cd /home/cloudera/Desktop/atni/atni/parsers
./run_aaa_alu_parser.sh /etl/aaa_alu/staging/

cd $ATNI_REPO/shell_scripts/aaa_alu/

./refresh_aaa_alu_tables.sh
