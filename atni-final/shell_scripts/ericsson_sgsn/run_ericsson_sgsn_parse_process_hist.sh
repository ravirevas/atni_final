#!/usr/bin/env bash

cd $ATNI_REPO/parsers
./run_ericsson_sgsn_parser.sh /etl/gtt/ericsson_sgsn/hist/staging/temp/

cd $ATNI_REPO/shell_scripts/ericsson_sgsn/

./refresh_ericsson_tables.sh