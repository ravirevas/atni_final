#!/usr/bin/env bash

sudo -u gtt hadoop fs -rm -r /etl/gtt/genband/hist/staging/temp
sudo -u gtt hadoop fs -cp -f /etl/gtt/genband/hist/staging/* /etl/gtt/genband/complete/
sudo -u gtt hadoop fs -rm -r /etl/gtt/genband/hist/staging/*
sudo -u gtt hadoop fs -mkdir -p /etl/gtt/genband/hist/staging/temp

