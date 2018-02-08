#!/usr/bin/env bash

impala-shell -i 172.19.100.103:21000 -f $ATNI_REPO/sql_scripts/refresh_aaa_alu_tables.hql
