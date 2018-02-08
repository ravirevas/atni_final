#!/usr/bin/env bash

IN_PATH=$1

fileCount=`sudo -u gtt hadoop fs -count $IN_PATH | awk '{print $2}'`

if [ $fileCount -eq 0 ]; then
        echo 'No new ericsson_sgsn files to parse...'
        exit 0
fi

path="ericsson_main_bin_sc.py $IN_PATH"
process_id=`ps -ef | grep "$path" | grep -v grep | awk '{print $2}'`

#echo "Here is the process_id $process_id"

##   If there are any processes matching the above condition, then the processID of the first process would be captured in the array and the length would be more than 1.
##   If there are no processes, the first element in the array would be blank and the length of the same would be 0. So we can proceed safely.

p_count=${#process_id[0]}

if [ ${p_count} -lt 1 ];
then
        echo "No previous process Running. Hence processing the current request...."
else
        echo "Looks like previous processing is still running. Hence exiting.."
        exit 1
fi


cd $ATNI_REPO/common
sudo -u gtt python check_duplicate_file.py ericsson_sgsn

cd $ATNI_REPO/ericsson

sudo -u gtt \
spark-submit --master local \
--py-files \
$ATNI_REPO/bin/ericsson_sgsn.zip,$ATNI_REPO/bin/common.zip,\
./ericsson_main_bin_sc.py $IN_PATH

