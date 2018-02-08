import sys

from common.constants import *
from pyspark import StorageLevel

from common.utilities import *
from ericsson_process_records import *
from ericsson_schema import *

def save_ericsson_data(sc, sqlContext,cdr_pdp_rdd,cdr_smo_rdd,cdr_smt_rdd,
                       cdr_pdp_traffic_volume_rdd,cdr_pdp_camel_pdp_rdd,cdr_pdp_rec_extension_rdd,
                       cdr_smo_camel_sms_rdd):
    if cdr_pdp_rdd.count() > 0:
        insert_records(sqlContext, cdr_pdp_rdd, get_pdp_schema(), ERICSSON_PDP_TABLE_NAME)
        print "############### Saved ERICSSON_PDP_TABLE_NAME ######################"
        if cdr_pdp_traffic_volume_rdd.count() > 0:
            insert_sub_records(sqlContext, cdr_pdp_traffic_volume_rdd, get_traffic_volume_schema(), ERICSSON_PDP_TRAFFIC_VOLUME_TABLE)
            print "############### Saved ERICSSON_PDP_TRAFFIC_VOLUME_TABLE ######################"
        if cdr_pdp_camel_pdp_rdd.count() > 0:
            insert_sub_records(sqlContext, cdr_pdp_camel_pdp_rdd, get_camel_info_pdp_schema(), ERICSSON_PDP_CAMEL_INFO_PDP_TABLE)
            print "############### Saved ERICSSON_PDP_CAMEL_INFO_PDP_TABLE ######################"
        if cdr_pdp_rec_extension_rdd.count() > 0:
            insert_sub_records(sqlContext, cdr_pdp_rec_extension_rdd, get_record_extension_schema(), ERICSSON_PDP_RECORD_EXTENSIONS_TABLE)
            print "############### Saved ERICSSON_PDP_RECORD_EXTENSIONS_TABLE ######################"
    if cdr_smo_rdd.count() > 0:
        insert_records(sqlContext, cdr_smo_rdd, get_smo_schema(), ERICSSON_SMO_TABLE_NAME)
        print "############### Saved ERICSSON_SMO_TABLE_NAME ######################"
    if cdr_smt_rdd.count() > 0:
        insert_records(sqlContext, cdr_smt_rdd, get_smt_schema(), ERICSSON_SMT_TABLE_NAME)
        print "############### Saved ERICSSON_SMT_TABLE_NAME ######################"



def save_ericsson_stats(sc, sqlContext,cdr_iteration_stats_rdd):

    if(cdr_iteration_stats_rdd.count() > 0):
        insert_records(sqlContext, cdr_iteration_stats_rdd, get_ericsson_stats_schema(), ERICSSON_STATS_TABLE_NAME)
        print "############### Saved ERICSSON_SGSN_STATS_TABLE_NAME ######################"


def get_contentz_chunk_array(contentz, chunk_size):
    for i in range(0, len(contentz), chunk_size):
        yield contentz[i:i + chunk_size]

def main(argv=None):

    try:
        print "#### Logging directory is: " + ERICSSON_SGSN_LOG
        #logging.basicConfig(filename=get_logging_file_name("parsing", ERICSSON_SGSN_LOG, ERICSSON_SGSN), level=logging.INFO)

        iteration_id = str(get_current_time_and_day())
        start_time_for_iteration = time.time()

        sc = get_spark_context("ericsson_process")
        #    cdr_whole = sc.binaryFiles(ERICSSON_IN_PATH)
        cdr_whole = sc.binaryFiles(sys.argv[1])

        print "##### Parsing ASN format records #####"
        parsed_records = cdr_whole.flatMap(lambda (x,y): parse_ericsson_file(x,y,iteration_id))

        #logging.debug("###### ASN1 parsing done #####")

        parsed_records.persist(storageLevel = StorageLevel.MEMORY_ONLY_SER)

        cdr_pdp_rdd = parsed_records.flatMap(lambda(x): x.pdp_base)
        cdr_pdp_traffic_volume_rdd = parsed_records.flatMap(lambda(x): x.pdp_traffic_volume_array)
        cdr_pdp_camel_pdp_rdd = parsed_records.flatMap(lambda(x): x.pdp_camel_pdp)
        cdr_pdp_rec_extension_rdd = parsed_records.flatMap(lambda(x): x.pdp_rec_ext)
        cdr_smo_rdd = parsed_records.flatMap(lambda(x): x.smo_base)
        cdr_smo_camel_sms_rdd = parsed_records.flatMap(lambda(x): x.smo_camel)
        cdr_smt_rdd = parsed_records.flatMap(lambda(x): x.smt_base)



        print "pdp base counts:" + str(cdr_pdp_rdd.count())
        print "pdp traffic counts:" + str(cdr_pdp_traffic_volume_rdd.count())
        print "pdp camel counts:" + str(cdr_pdp_camel_pdp_rdd.count())
        print "pdp rec extension counts:" + str(cdr_pdp_rec_extension_rdd.count())
        print "smo base counts:" + str(cdr_smo_rdd.count())
        print "smo camel counts:" + str(cdr_smo_camel_sms_rdd.count())
        print "smt base counts:" + str(cdr_smt_rdd.count())

        cdr_iteration_stats_rdd = parsed_records.flatMap(lambda(x): [
            [x.iter_stats.iteration_id, x.iter_stats.file_id, x.iter_stats.file_name, x.iter_stats.checksum ,
             x.iter_stats.total_records,x.iter_stats.success_count,x.iter_stats.fail_count,
             x.iter_stats.pdp_base_total_count, x.iter_stats.pdp_base_success_count,x.iter_stats.pdp_base_failed_count,
             x.iter_stats.pdp_traffic_count,
             x.iter_stats.pdp_camel_count,
             x.iter_stats.pdp_rec_ext_count,
             x.iter_stats.smo_base_total_count, x.iter_stats.smo_base_success_count,x.iter_stats.smo_base_failed_count ,
             x.iter_stats.smo_camel_count,
             x.iter_stats.smt_base_total_count, x.iter_stats.smt_base_success_count,x.iter_stats.smt_base_failed_count,
             get_current_time_day(), get_current_time()]])

        print "stats counts:" + str(cdr_iteration_stats_rdd.count())
        print "Time to iterate entire contents:" + str(time.time() - start_time_for_iteration)
        print "#################### Saving to db. Done parsing"
        save_ericsson_data(sc, get_sql_context(sc), cdr_pdp_rdd,cdr_smo_rdd,cdr_smt_rdd, cdr_pdp_traffic_volume_rdd,
                           cdr_pdp_camel_pdp_rdd, cdr_pdp_rec_extension_rdd, cdr_smo_camel_sms_rdd)

        save_ericsson_stats(sc, get_sql_context(sc),cdr_iteration_stats_rdd)
        print "Complete ericsson iteration"
        
    except Exception as ep:
        print ep.message


if __name__ == "__main__":
    sys.exit(main())
