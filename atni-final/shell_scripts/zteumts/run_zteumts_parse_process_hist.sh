#!/usr/bin/env bash

cd $ATNI_REPO/shell_scripts/zteumts
./run_zteumts_parser.sh /etl/commnet/zteumts/hist/staging/temp/


cd $ATNI_REPO/shell_scripts/zteumts/
./refresh_zteumts_tables.sh