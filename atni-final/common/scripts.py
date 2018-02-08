import sys,os
import re
from datetime import datetime,timedelta
import logging
from subprocess import call,PIPE
import ConfigParser
import subprocess
from os import system
from constants import *
import gzip

dataset_stats_dict = {

    AIRSPAN: (AIRSPAN_HDFS_SOURCE,hdfs_path_airspan),
    GENBAND: (GENBAND_HDFS_SOURCE,hdfs_path_genband),
    ERICSSON_SGSN: (ERICSSON_HDFS_SOURCE,hdfs_path_ericsson),
    ZTEUMTS: (ZTEUMTS_HDFS_SOURCE,hdfs_path_zteumts)

}

def main(argv=None):

  switch_type = sys.argv[1]
  source_path,dest_hdfs_path=dataset_stats_dict[switch_type]
  FilePullDate=(datetime.today()-timedelta(days=1)).strftime("%Y%m%d")
  if sys.argv[1]=='zteumts':
    for path in source_path:
      base_path=path+"/"+FilePullDate+"/"+"*"
      base_path1=path+"/"+FilePullDate+"/"
      count_cmd=["hadoop","fs","-count",base_path1] 
      stat_proc = subprocess.Popen(' '.join(count_cmd), shell=True, stdout=subprocess.PIPE)
      s_output,s_err = stat_proc.communicate()
      print s_output
      data = re.sub(' +', '#', s_output).split('#')
      count=int(data[2])
      if count == 0:
        flag=1
        print "error-flag="+str(flag)
      else:
       call(["hadoop","fs","-cp",base_path,dest_hdfs_path])
       hdfsFilePath=dest_hdfs_path+"/"+"*"
       localFilePath="/home/cloudera/temp1/"
       call(["hadoop", "fs", "-copyToLocal",hdfsFilePath,localFilePath])
       call(["hadoop","fs", "-mkdir", "-p" ,dest_hdfs_path+'/temp/'])
       for pathname in os.listdir(localFilePath):
        temp=localFilePath+'/'+pathname
        name = os.path.splitext(os.path.basename(pathname))[0]
        call(["gzip","-d",temp])
        call(["hadoop", "fs", "-put",localFilePath+name,dest_hdfs_path+'/temp/'])
  elif sys.argv[1]=='ericsson_sgsn':

     base_path=source_path+"/"+FilePullDate+"/"+"*"
     base_path1=source_path+"/"+FilePullDate+"/"

     count_cmd=["hadoop","fs","-count",base_path1] 
     stat_proc = subprocess.Popen(' '.join(count_cmd), shell=True, stdout=subprocess.PIPE)
     s_output,s_err = stat_proc.communicate()
     print s_output
     data = re.sub(' +', '#', s_output).split('#')
     count=int(data[2])
     if count == 0:
       flag=1
       print "error-flag="+str(flag)
     else:
       call(["hadoop","fs","-cp",base_path,dest_hdfs_path])
       hdfsFilePath=dest_hdfs_path+"/"+"*"
       localFilePath="/home/cloudera/temp1/"
       call(["hadoop", "fs", "-copyToLocal",hdfsFilePath,localFilePath])
       call(["hadoop","fs", "-mkdir", "-p" ,dest_hdfs_path+'/temp/'])
       for pathname in os.listdir(localFilePath):
        temp=localFilePath+pathname
        name = os.path.splitext(os.path.basename(pathname))[0]
        call(["gzip","-d",temp])
        call(["hadoop", "fs", "-put",localFilePath+name,dest_hdfs_path+'/temp/'])
        call(["rm",localFilePath+name])
  else:
   
     base_path=source_path+"/"+FilePullDate+"/"+"*"
     base_path1=source_path+"/"+FilePullDate+"/"

     count_cmd=["hadoop","fs","-count",base_path1] 
     stat_proc = subprocess.Popen(' '.join(count_cmd), shell=True, stdout=subprocess.PIPE)
     s_output,s_err = stat_proc.communicate()
     print s_output
     data = re.sub(' +', '#', s_output).split('#')
     count=int(data[2])
     if count == 0:
       flag=1
       print "error-flag="+str(flag)
     else:
       call(["hadoop","fs","-cp",base_path,dest_hdfs_path])

if __name__ == "__main__":
    sys.exit(main())
