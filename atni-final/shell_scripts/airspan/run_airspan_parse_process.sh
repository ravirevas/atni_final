#!/usr/bin/env bash

cd $ATNI_REPO/common
python scripts.py airspan

cd $ATNI_REPO/shell_scripts/airspan
./run_airspan_parser.sh /tmp/airspan_new/staging

cd $ATNI_REPO/shell_scripts/airspan/
./refresh_airspan_tables.sh




