#!/usr/bin/env bash

sudo -u commnet hadoop fs -rm -r /etl/commnet/zteumts/hist/staging/temp
sudo -u commnet hadoop fs -cp -f /etl/commnet/zteumts/hist/staging/* /etl/commnet/zteumts/complete/
sudo -u commnet hadoop fs -rm -r /etl/commnet/zteumts/hist/staging/*
sudo -u commnet hadoop fs -mkdir -p /etl/commnet/zteumts/hist/staging/temp
