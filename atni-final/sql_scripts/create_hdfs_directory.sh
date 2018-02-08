#!/usr/bin/env bash

# Top Level
#sudo -u hdfs hadoop fs -mkdir /etl

# GCC process
#sudo -u hdfs hadoop fs -mkdir /etl/gtt
#sudo -u hdfs hadoop fs -mkdir /etl/gtt/airspan
sudo -u hdfs hadoop fs -mkdir /etl/gtt/airspan/staging
sudo -u hdfs hadoop fs -mkdir /etl/gtt/airspan/complete
sudo -u hdfs hadoop fs -mkdir /etl/gtt/airspan/failed

#sudo -u hdfs hadoop fs -mkdir /etl/gtt/genband
sudo -u hdfs hadoop fs -mkdir /etl/gtt/genband/staging
sudo -u hdfs hadoop fs -mkdir /etl/gtt/genband/complete
sudo -u hdfs hadoop fs -mkdir /etl/gtt/genband/failed

#sudo -u hdfs hadoop fs -mkdir /etl/gtt/ericsson_sgsn
sudo -u hdfs hadoop fs -mkdir /etl/gtt/ericsson_sgsn/staging
sudo -u hdfs hadoop fs -mkdir /etl/gtt/ericsson_sgsn/complete
sudo -u hdfs hadoop fs -mkdir /etl/gtt/ericsson_sgsn/failed

# COMMNET process
#sudo -u hdfs hadoop fs -mkdir /etl/commnet
#sudo -u hdfs hadoop fs -mkdir /etl/commnet/zteumts
sudo -u hdfs hadoop fs -mkdir /etl/commnet/zteumts/staging
sudo -u hdfs hadoop fs -mkdir /etl/commnet/zteumts/complete
sudo -u hdfs hadoop fs -mkdir /etl/commnet/zteumts/failed
