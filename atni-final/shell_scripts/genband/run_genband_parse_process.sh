#!/usr/bin/env bash

cd $ATNI_REPO/common
python scripts.py genband

cd $ATNI_REPO/shell_scripts/genband
./run_genband_parser.sh /tmp/genband_new/staging

cd $ATNI_REPO/shell_scripts/genband/

./refresh_genband_tables.sh
