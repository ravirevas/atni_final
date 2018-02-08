import sys
from pyspark import SparkConf, SparkContext, HiveContext
from pyspark.sql.types import *
from atni.parsers.constants import *
from atni.parsers.utilities import *
from aaa_alu_schema import *
import subprocess
import logging
import uuid
import datetime
import time

#print logging.INFO

def main(argv=None):
   

    # logging.basicConfig(filename=get_logging_file_name("parse", AIRSPAN_LOG, AIRSPAN), level=logging.INFO)

#    input_path = aaa_alu_IN_PATH
    input_path = sys.argv[1]

    sc = get_spark_context("aaa_alu_spark_job")
    sqlContext = get_sql_context(sc)

    # Custom schema to read input

    hdfs_aaa_alu_paths = get_file_paths_from_hfds(input_path)


    iteration_id = get_current_time_and_day()
    aaa_alu_stats = []

    for path in hdfs_aaa_alu_paths:

        # Read the input file,split with a delimiter and extract input fields
        total_count = 0
        success_count = 0
        fail_count = 0
        calls = sc.textFile(path)
        file_name = get_file_name(path)

        print "############file_path is:" + path
        print "############file_name is:" + file_name
        
        
        check_sum = get_file_check_sum(str(path))
        #check_sum = 123445678
        calls_parts = calls.map(lambda l: l.encode("ascii", "ignore").split(","))

        total_count = calls_parts.count()
     
         
        calls_parsed = calls_parts.map(lambda p : ((file_name,(p[0].replace("/","-")),p[1],p[2],p[3],p[4],p[5],p[6],p[7],p[8],p[9],p[10],p[11],p[12],p[13],p[14],p[15],p[16],p[17],p[18],p[19],p[20],p[21],p[22],p[23],p[24],p[25],p[26],p[27],p[28],p[29],p[30],p[31],p[32],p[33],p[34],p[35],p[36],p[37],p[38],p[39],p[40],p[41],(p[42].replace("/","-")),p[43],p[44],p[45],p[46],p[47],p[48],p[49],p[50],convert_long(p[51]),p[52],p[53],p[54],long(datetime.datetime.strptime(p[0],'%Y/%m/%d  %H:%M:%S').strftime('%Y%m%d')),long(datetime.datetime.strptime(p[0],'%Y/%m/%d  %H:%M:%S').strftime('%H')))))
        
                
        #print calls_parsed.take(5)
        #print total_count 


        success_count = calls_parsed.count()
        fail_count = total_count - success_count
        print "##################$$$$$$$$ Parsing complete. Fetching stats now ##########$$$$$$$$$$$$:" + str(success_count)
        stats = [[str(iteration_id) ,str(uuid.uuid4()), file_name, check_sum , total_count, success_count,
        fail_count, get_current_time_day(), get_current_time()]]
        
	
        #print stats

  

        print "########## Aaa parsing done. Saving to Database ##########"
       # calls_parsed.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/sample113") 
        # aaa_alu_stats.append(stats)
        # Create Dataframe and execute Hive commands to save the table

        ## Below code is for Hive storing
        insert_records(sqlContext,calls_parsed,get_aaa_schema(),AAA_TABLE_NAME)
        print "#############DATA SAVED IN TABLE##################################"
       
        insert_records(sqlContext,sc.parallelize(stats),get_aaa_stats_schema(),AAA_STATS_TABLE_NAME)
        
        print"############Stats Table Updated###########"




if __name__ == "__main__":
    sys.exit(main())
