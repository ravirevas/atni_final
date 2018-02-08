#!/usr/bin/env bash

sudo -u gtt hadoop fs -rm -r /etl/gtt/ericsson_sgsn/hist/staging/temp
sudo -u gtt hadoop fs -cp -f /etl/gtt/ericsson_sgsn/hist/staging/* /etl/gtt/ericsson_sgsn/complete/
sudo -u gtt hadoop fs -rm -r /etl/gtt/ericsson_sgsn/hist/staging/*
sudo -u gtt hadoop fs -mkdir -p /etl/gtt/ericsson_sgsn/hist/staging/temp
