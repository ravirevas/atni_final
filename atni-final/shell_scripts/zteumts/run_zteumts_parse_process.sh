#!/usr/bin/env bash

cd $ATNI_REPO/common
python scripts.py zteumts


cd $ATNI_REPO/shell_scripts/zteumts
./run_zteumts_parser.sh /tmp/zteumts_new/staging


cd $ATNI_REPO/shell_scripts/zteumts/
./refresh_zteumts_tables.sh
