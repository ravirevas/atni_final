#!/usr/bin/env bash

cd $ATNI_REPO/shell_scripts/genband
./run_genband_parser.sh /etl/gtt/genband/hist/staging/temp/

cd $ATNI_REPO/shell_scripts/genband/
./refresh_genband_tables.sh