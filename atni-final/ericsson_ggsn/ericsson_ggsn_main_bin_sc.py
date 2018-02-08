import sys

from atni.parsers.ericsson_ggsn.ericsson_ggsn_process_records import *
import logging, sys, os
import time
from datetime import datetime
from atni.parsers.constants import *
from atni.parsers.utilities import *
from pyspark import StorageLevel
import atni.parsers.ericsson_ggsn.ericsson_ggsn_schema


def save_ericsson_data(sc, sqlContext, cdr_pdp_ggsn_rdd,cdr_pdp_egsn_rdd,ggsn_cdr_pdp_traffic_volume_rdd,
                       ggsn_cdr_pdp_rec_extension_rdd,egsn_cdr_pdp_traffic_volume_rdd,egsn_cdr_pdp_service_data_rdd,egsn_cdr_pdp_rec_extension_rdd):

   if cdr_pdp_ggsn_rdd.count() > 0:

       insert_records_with_timestamp1(sqlContext, cdr_pdp_ggsn_rdd, get_ggsn_schema(), ERICSSONGGSN_PDP_OUT_PATH)
       print "############### Saved ggsn_pdp_CDR_TABLE_NAME ######################"

       if ggsn_cdr_pdp_traffic_volume_rdd.count() > 0:

           insert_records_with_timestamp1(sqlContext, ggsn_cdr_pdp_traffic_volume_rdd,get_traffic_volume_schema_ggsn(),ERICSSONGGSN_PDP_TRAFFIC_VOLUME_OUT_PATH)
           print "############### Saved GGSNERICSSON_PDP_TRAFFIC_VOLUME_TABLE ######################"

       if ggsn_cdr_pdp_rec_extension_rdd.count() > 0:

           insert_records_with_timestamp1(sqlContext, ggsn_cdr_pdp_rec_extension_rdd, get_record_extension_schema_ggsn(),
                                          ERICSSONGGSN_PDP_RECORD_EXTENSIONS_OUT_PATH)
           print "############### Saved GGSNERICSSON_PDP_RECORD_EXTENSIONS_TABLE ######################"

   if cdr_pdp_egsn_rdd.count() > 0:

       insert_records_with_timestamp1(sqlContext, cdr_pdp_egsn_rdd, get_egsn_schema(), ERICSSON_EGSN_OUT_PATH)
       print "############### Saved ERICSSON_EGSN_TABLE_NAME ######################"
       if egsn_cdr_pdp_traffic_volume_rdd.count() > 0:

           insert_records_with_timestamp1(sqlContext, egsn_cdr_pdp_traffic_volume_rdd,get_traffic_volume_schema_egsn(),ERICSSONEGSN_PDP_TRAFFIC_VOLUME_OUT_PATH)
           print "############### Saved EGSNERICSSON_PDP_TRAFFIC_VOLUME_TABLE ######################"
       if egsn_cdr_pdp_service_data_rdd.count() > 0:
           insert_records_with_timestamp1(sqlContext, egsn_cdr_pdp_service_data_rdd,get_service_data_schema_egsn(),ERICSSONEGSN_PDP_SERVICE_DATA_OUT_PATH)
           print "############### Saved EGSNERICSSON_PDP_SERVICE_DATA_TABLE ######################"

       if egsn_cdr_pdp_rec_extension_rdd.count() > 0:
           insert_records_with_timestamp1(sqlContext, egsn_cdr_pdp_rec_extension_rdd, get_record_extension_schema_egsn(),
                                          ERICSSONEGSN_PDP_RECORDEXTENSION_OUT_PATH)
           print "############### Saved EGSNERICSSON_PDP_RECORD_EXTENSIONS_TABLE ######################"

def save_ericsson_stats(sc, sqlContext,cdr_iteration_stats_rdd):

    if(cdr_iteration_stats_rdd.count() > 0):

        insert_records_with_timestamp1(sqlContext, cdr_iteration_stats_rdd, get_ericsson_stats_schema(),ERICSSONGGSN_STATS_OUT_PATH)
        print "############### Saved ERICSSON_SGSN_STATS_TABLE_NAME ######################"

def get_contentz_chunk_array(contentz, chunk_size):
    for i in range(0, len(contentz), chunk_size):
        yield contentz[i:i + chunk_size]

def main(argv=None):

    try:
        print "#### Logging directory is: " + ERICSSON_SGSN_LOG
        #logging.basicConfig(filename=get_logging_file_name("parsing", ERICSSON_GGSN_LOG, ERICSSON_GGSN), level=logging.INFO)

        iteration_id = str(get_current_time_and_day())
        start_time_for_iteration = time.time()

        sc = get_spark_context("ericsson_process")
        #    cdr_whole = sc.binaryFiles(ERICSSON_IN_PATH)
        cdr_whole = sc.binaryFiles('/user/cloudera/ingg/')


        print "##### Parsing ASN format records #####"
        parsed_records = cdr_whole.flatMap(lambda (x,y): parse_ericsson_file(x,y,iteration_id))

        #print parsed_records.take(10)

        #logging.debug("###### ASN1 parsing done #####")

        parsed_records.persist(storageLevel = StorageLevel.MEMORY_ONLY_SER)


        cdr_pdp_ggsn_rdd = parsed_records.flatMap(lambda (x): x.ggsnrecord)
        ggsn_cdr_pdp_traffic_volume_rdd = parsed_records.flatMap(lambda (x): x.ggsn_pdp_traffic_volume_array)
        ggsn_cdr_pdp_rec_extension_rdd = parsed_records.flatMap(lambda (x): x.ggsn_pdp_rec_ext)
        cdr_pdp_egsn_rdd = parsed_records.flatMap(lambda (x): x.egsn_base)
        egsn_cdr_pdp_traffic_volume_rdd = parsed_records.flatMap(lambda (x): x.egsn_pdp_traffic_volume_array)
        egsn_cdr_pdp_service_data_rdd = parsed_records.flatMap(lambda (x): x.egsn_pdp_service_data_array)
        egsn_cdr_pdp_rec_extension_rdd = parsed_records.flatMap(lambda (x): x.egsn_pdp_rec_ext)



        print "ggsn counts:" + str(cdr_pdp_ggsn_rdd.count())
        print "ggsn_pdp traffic counts:" + str(ggsn_cdr_pdp_traffic_volume_rdd.count())
        print "ggsnpdp rec extension counts:" + str(ggsn_cdr_pdp_rec_extension_rdd.count())
        print "egsn base counts:" + str(cdr_pdp_egsn_rdd.count())
        print "egsn_pdp traffic counts:" + str(egsn_cdr_pdp_traffic_volume_rdd.count())
        print "egsn_pdp service data counts:" + str(egsn_cdr_pdp_service_data_rdd.count())
        print "egsnpdp rec extension counts:" + str(egsn_cdr_pdp_rec_extension_rdd.count())

        cdr_iteration_stats_rdd = parsed_records.flatMap(lambda(x): [
            [x.iter_stats.iteration_id, x.iter_stats.file_id, x.iter_stats.file_name, x.iter_stats.checksum ,
             x.iter_stats.total_records,x.iter_stats.success_count,x.iter_stats.fail_count,
             x.iter_stats.ggsnrecord_total_count, x.iter_stats.ggsnrecord_success_count,
             x.iter_stats.ggsnrecord_failed_count,
             x.iter_stats.ggsn_pdp_traffic_count,
             x.iter_stats.ggsn_pdp_rec_ext_count,
             x.iter_stats.egsn_base_total_count, x.iter_stats.egsn_base_success_count, x.iter_stats.egsn_base_failed_count,
             x.iter_stats.egsn_pdp_traffic_count,
             x.iter_stats.egsn_pdp_servicedata_count,
             x.iter_stats.egsn_pdp_rec_ext_count,
             get_current_time_day(),get_year(), get_month(), get_day()
             ]])

        print "stats counts:" + str(cdr_iteration_stats_rdd.count())
        print "Time to iterate entire contents:" + str(time.time() - start_time_for_iteration)
        print "#################### Saving to db. Done parsing"
        #save_ericsson_data(sc, get_sql_context(sc), cdr_pdp_ggsn_rdd,ggsn_cdr_pdp_traffic_volume_rdd,cdr_pdp_egsn_rdd,ggsn_cdr_pdp_rec_extension_rdd)
        save_ericsson_data(sc, get_sql_context(sc), cdr_pdp_ggsn_rdd,cdr_pdp_egsn_rdd, ggsn_cdr_pdp_traffic_volume_rdd,ggsn_cdr_pdp_rec_extension_rdd,
                           egsn_cdr_pdp_traffic_volume_rdd,egsn_cdr_pdp_service_data_rdd, egsn_cdr_pdp_rec_extension_rdd)

        save_ericsson_stats(sc, get_sql_context(sc),cdr_iteration_stats_rdd)
        print "Complete ericsson iteration"

    except Exception as ep:
        print ep.message


if __name__ == "__main__":
    sys.exit(main())
