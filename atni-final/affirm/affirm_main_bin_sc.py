#!/usr/bin/env python
import sys
import time


from affirm_process_records import *

from affirm_schema import *
from common.constants import *
from common.utilities import *
from pyspark import StorageLevel


def save_affirm_data(sc, sqlContext, cdr_pdp_pgw_rdd,cdr_pdp_lsd_rdd,cdr_pdp_sgw_rdd,cdr_sgw_rec_extension_rdd):
   if cdr_pdp_pgw_rdd.count() > 0:
       print "crd_pdp data is here"

       insert_records(sqlContext, cdr_pdp_pgw_rdd, get_pgw_schema(), AFFIRMED_LTE_CDR_PGW_TABLE_NAME)
       print "############### Saved AFFIRMED_LTE_CDR_TABLE_NAME ######################"
       if cdr_pdp_lsd_rdd.count()>0:
           insert_records(sqlContext,cdr_pdp_lsd_rdd,get_cdr_pdp_lsd_schema(), AFFIRMED_LTE_CDR_LSD_TABLE_NAME)
           print "############### Saved AFFIRMED_LTE_CDR_LSD_TABLE_NAME ####################"
           #TODO
   # if cdr_pdp_sgw_rdd.count() > 0:
   #     insert_records(sqlContext,cdr_pdp_sgw_rdd, get_sgw_schema(), AFFIRMED_LTE_CDR_SGW_TABLE_NAME )
   #     print "############### Saved AFFIRMED_LTE_CDR_TABLE_NAME ######################"
   #     # if cdr_sgw_rec_extension_rdd.count() > 0:
       #     insert_sub_records(sqlContext, cdr_sgw_rec_extension_rdd, get_record_extension_schema(),AFFIRMED_LTE_CDR_RECORD_EXTENSIONS_TABLE)
       #     print "############### Saved AFFIRMED_LTE_CDR_RECORD_EXTENSIONS_TABLE ######################"

def save_affirm_stats(sc, sqlContext,cdr_iteration_stats_rdd):
    print "Entered stats function #########"
    if(cdr_iteration_stats_rdd.count() > 0):
        print "After if condition ############################"
        insert_records(sqlContext, cdr_iteration_stats_rdd, get_affirm_stats_schema(), AFFIRMED_STATS_TABLE_NAME)
        print "############### Saved AFFIRMED_LTE_CDR_STATS_TABLE_NAME ######################"



def get_contentz_chunk_array(contentz, chunk_size):
    for i in range(0, len(contentz), chunk_size):
        yield contentz[i:i + chunk_size]

def main(argv=None):

    try:

        #logging.basicConfig(filename=get_logging_file_name("parsing", AFFIRMED_LTE_CDR), level=logging.INFO)

        iteration_id = str(get_current_time_and_day())
        start_time_for_iteration = time.time()

        sc = get_spark_context("affirm_process")

        #cdr_whole = sc.binaryFiles("hdfs://quickstart.cloudera:8020/user/cloudera/" )
        #input_path = sys.argv[1]
        input_path = "hdfs://quickstart.cloudera:8020/etl/gtt/affirmed_lte/CDR_20161007050016_229.asn1"


        cdr_whole = sc.binaryFiles(input_path)
        #hdfs_affirm_paths = get_file_paths_from_hfds(input_path)

        print "##### Parsing ASN format records #####"
        parsed_records = cdr_whole.flatMap(lambda (x,y): parse_affirm_file(x,y,iteration_id))
        parsed_records.persist(storageLevel=StorageLevel.MEMORY_ONLY_SER)
        #logging.debug("###### ASN1 parsing done #####")

        cdr_pdp_pgw_rdd = parsed_records.flatMap(lambda (x): x.pgw_record)
        cdr_pdp_lsd_rdd = parsed_records.flatMap(lambda (x): x.pgw_list_of_service_data)
        cdr_pdp_sgw_rdd = parsed_records.flatMap(lambda (x): x.sgw_record)
        cdr_sgw_rec_extension_rdd = parsed_records.flatMap(lambda (x): x.sgw_rec_ext)
        print parsed_records.count()

        print "pgw counts:" + str(cdr_pdp_pgw_rdd.count())
        print "list of service data counts:" +  str(cdr_pdp_lsd_rdd.count())
        print "sgw counts:" + str(cdr_pdp_sgw_rdd.count())
        print "sgw rec extension counts:" + str(cdr_sgw_rec_extension_rdd.count())

        cdr_iteration_stats_rdd = parsed_records.flatMap(lambda (x): [
            [x.iter_stats.iteration_id, x.iter_stats.file_id, x.iter_stats.file_name, x.iter_stats.checksum,
             x.iter_stats.total_count, x.iter_stats.success_count, x.iter_stats.fail_count,
             x.iter_stats.pgw_total_count, x.iter_stats.pgw_success_count, x.iter_stats.pgw_failed_count,x.iter_stats.pgw_lsd_count,
             x.iter_stats.sgw_rec_ext_count,x.iter_stats.sgw_total_count, x.iter_stats.sgw_success_count,
             x.iter_stats.sgw_failed_count,x.iter_stats.file_created_timestamp, x.iter_stats.year, x.iter_stats.month,x.iter_stats.day]])

        print "stats counts:" + str(cdr_iteration_stats_rdd.count())
        print "Time to iterate entire contents:" + str(time.time() - start_time_for_iteration)

        save_affirm_data(sc, get_sql_context(sc),cdr_pdp_pgw_rdd,cdr_pdp_lsd_rdd,cdr_pdp_sgw_rdd,cdr_sgw_rec_extension_rdd)

        save_affirm_stats(sc, get_sql_context(sc), cdr_iteration_stats_rdd)

        print "Complete Affirm iteration"

    except Exception as ep:
        print ep.message



if __name__ == "__main__":
    sys.exit(main())
