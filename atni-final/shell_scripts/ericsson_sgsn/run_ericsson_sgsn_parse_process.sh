#!/usr/bin/env bash

cd $ATNI_REPO/common
python scripts.py ericsson

cd $ATNI_REPO/shell_scripts/ericsson_sgsn/
./run_ericsson_sgsn_parser.sh /tmp/ericsson_new/staging


cd $ATNI_REPO/shell_scripts/ericsson_sgsn/
./refresh_ericsson_tables.sh
