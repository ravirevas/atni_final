import StringIO
import logging
import sys
import uuid
#sys.path.append("/tmp/parsers/atni/parsers/")
from pyasn1 import error
from pyasn1.codec.ber import decoder
from zteumts_asn1_schema import *
from zteumts_parsed import *
from zteumts_asn_gen import CallEventRecord
from pyspark import StorageLevel
from common.utilities import *
from zteumts_stats import ZTEUMTSStats
from atni.zteumts_asn1.process_records.zteumts_process_commonequiprecord import ProcessZTEumtsCommonEquipRecord
from atni.zteumts_asn1.process_records.zteumts_process_incgatewayrecord import ProcessZTEumtsIncGatewayRecord
from atni.zteumts_asn1.process_records.zteumts_process_mcfcallrecord import ProcessZTEumtsMCFCallRecord
from atni.zteumts_asn1.process_records.zteumts_process_mocallrecord import ProcessZTEumtsMOCallRecord
from atni.zteumts_asn1.process_records.zteumts_process_mosmsrecord import ProcessZTEumtsMOSMSRecord
from atni.zteumts_asn1.process_records.zteumts_process_mtsmsrecord import ProcessZTEumtsMTSMSRecord
from atni.zteumts_asn1.process_records.zteumts_process_mtcallrecord import ProcessZTEumtsMTCallRecord
from atni.zteumts_asn1.process_records.zteumts_process_mtlcsrecord import ProcessZTEumtsmtLCSRecordRecord
from atni.zteumts_asn1.process_records.zteumts_process_outgatewayrecord import ProcessZTEumtsOutGatewayRecord
from atni.zteumts_asn1.process_records.zteumts_process_ssactionrecord import ProcessZTEumtsSSActionRecord
from atni.zteumts_asn1.process_records.zteumts_process_ussdrecord import ProcessZTEumtsUSSDRecord
from atni.zteumts_asn1.process_records.zteumts_process_gsicphrecord import ProcessZTEumtsGSICPHRecord
from atni.zteumts_asn1.process_records.zteumts_process_hlrintrecord import ProcessZTEumtsHLRIntRecord
from atni.zteumts_asn1.process_records.zteumts_process_molcsrecord import ProcessZTEumtsmoLCSRecordRecord
from atni.zteumts_asn1.process_records.zteumts_process_mtrfrecord import ProcessZTEumtsMTRFRecord
from atni.zteumts_asn1.process_records.zteumts_process_ncscphrecord import ProcessZTEumtsNCSCPHRecord
from atni.zteumts_asn1.process_records.zteumts_process_nilcsrecord import ProcessZTEumtsNILCSRecord
from atni.zteumts_asn1.process_records.zteumts_process_vigrecord import ProcessZTEumtsVIGRecord
from atni.zteumts_asn1.process_records.zteumts_process_mcfcphrecord import ProcessZTEumtsMCFCPHRecord
from atni.zteumts_asn1.process_records.zteumts_process_tcicphrecord import ProcessZTEumtsTCICPHRecord
from atni.zteumts_asn1.process_records.zteumts_process_mscsrvccrecord import ProcessZTEumtsMSCSRVCCRecord
from atni.zteumts_asn1.process_records.zteumts_process_roamrecord import ProcessZTEumtsROAMRecord
from atni.zteumts_asn1.process_records.zteumts_process_mocphrecord import ProcessZTEumtsMOCPHRecord
from atni.zteumts_asn1.process_records.zteumts_process_moecallrecord import ProcessZTEumtsMOECallRecord
from atni.zteumts_asn1.process_records.zteumts_process_termcamelintrecord import \
    ProcessZTEumtsTermCAMELIntRecord




def save_zteumts_data(sc, sqlContext, cdr_commonequip_rdd, cdr_incgateway_rdd, cdr_mcfcall_rdd,
                      cdr_mocall_rdd, cdr_mosms_rdd, cdr_mtcall_rdd,
                      cdr_mtlcs_rdd, cdr_mtsms_rdd, cdr_outgateway_rdd, cdr_ssaction_rdd, cdr_ussd_rdd, cdr_gsicph_rdd,
                      cdr_hlrint_rdd, cdr_molcs_rdd, cdr_mtrf_rdd, cdr_ncscph_rdd, cdr_nilcs_rdd, cdr_vig_rdd,
                      cdr_mcfcph_rdd, cdr_tcicph_rdd, cdr_mscsrvcc_rdd, cdr_roam_rdd, cdr_mocph_rdd, cdr_moecall_rdd,
                      cdr_termcamelint_rdd):
    if cdr_commonequip_rdd.count() > 0:
        # x = cdr_commonequip_rdd.first()
        insert_records(sqlContext, cdr_commonequip_rdd, get_commonequip_schema(),
                       ZTEUMTS_COMMONEQUIP_TABLE_NAME)
        print "############### Saved ZTEUMTS_COMMONEQUIP_TABLE_NAME ######################"
    if cdr_incgateway_rdd.count() > 0:
        insert_records(sqlContext, cdr_incgateway_rdd, get_incgateway_schema(),
                       ZTEUMTS_INCGATEWAY_TABLE_NAME)
        print "############### Saved ZTEUMTS_INCGATEWAY_TABLE_NAME ######################"
    if cdr_mcfcall_rdd.count() > 0:
        insert_records(sqlContext, cdr_mcfcall_rdd, get_mcfcallrecord_schema(),
                       ZTEUMTS_MCFCALL_TABLE_NAME)
        print "############### Saved ZTEUMTS_MCFCALL_TABLE_NAME ######################"
    if cdr_mocall_rdd.count() > 0:
        insert_records(sqlContext, cdr_mocall_rdd, get_mocallrecord_schema(),
                       ZTEUMTS_MOCALL_TABLE_NAME)
        print "############### Saved ZTEUMTS_MOCALL_TABLE_NAME ######################"
    if cdr_mosms_rdd.count() > 0:
        insert_records(sqlContext, cdr_mosms_rdd, get_mosms_schema(), ZTEUMTS_MOSMS_TABLE_NAME)
        print "############### Saved ZTEUMTS_MOSMS_TABLE_NAME ######################"
    if cdr_mtcall_rdd.count() > 0:
        insert_records(sqlContext, cdr_mtcall_rdd, get_mtcallrecord_schema(),
                       ZTEUMTS_MTCALL_TABLE_NAME)
        print "############### Saved ZTEUMTS_MTCALL_TABLE_NAME ######################"
    if cdr_mtlcs_rdd.count() > 0:
        insert_records(sqlContext, cdr_mtlcs_rdd, get_mtlcs_schema(), ZTEUMTS_MTLCS_TABLE_NAME)
        print "############### Saved ZTEUMTS_MTLCS_TABLE_NAME ######################"
    if cdr_mtsms_rdd.count() > 0:
        insert_records(sqlContext, cdr_mtsms_rdd, get_mtsms_schema(), ZTEUMTS_MTSMS_TABLE_NAME)
        print "############### Saved ZTEUMTS_MTSMS_TABLE_NAME ######################"
    if cdr_outgateway_rdd.count() > 0:
        insert_records(sqlContext, cdr_outgateway_rdd, get_outgateway_schema(),
                       ZTEUMTS_OUTGATEWAY_TABLE_NAME)
        print "############### Saved ZTEUMTS_OUTGATEWAY_TABLE_NAME ######################"
    if cdr_ssaction_rdd.count() > 0:
        insert_records(sqlContext, cdr_ssaction_rdd, get_ssaction_schema(),
                       ZTEUMTS_SSACTION_TABLE_NAME)
        print "############### Saved ZTEUMTS_SSACTION_TABLE_NAME ######################"
    if cdr_ussd_rdd.count() > 0:
        insert_records(sqlContext, cdr_ussd_rdd, get_ussd_schema(), ZTEUMTS_USSD_TABLE_NAME)
        print "############### Saved ZTEUMTS_USSD_TABLE_NAME ######################"
    if cdr_gsicph_rdd.count() > 0:
        insert_records_without_timestamp(sqlContext, cdr_gsicph_rdd, get_gsicphrecord_schema(), ZTEUMTS_GSICPH_TABLE_NAME)
        print "############### Saved ZTEUMTS_GSICPH_TABLE_NAME ######################"
    if cdr_hlrint_rdd.count() > 0:
        insert_records_without_timestamp(sqlContext, cdr_hlrint_rdd, get_hlrintrecord_schema(), ZTEUMTS_HLRINT_TABLE_NAME)
        print "############### Saved ZTEUMTS_HLRINT_TABLE_NAME ######################"
    if cdr_molcs_rdd.count() > 0:
        insert_records_without_timestamp(sqlContext, cdr_molcs_rdd, get_molcs_schema(), ZTEUMTS_MOLCS_TABLE_NAME)
        print "############### Saved ZTEUMTS_MOLCS_TABLE_NAME ######################"
    if cdr_mtrf_rdd.count() > 0:
        insert_records_without_timestamp(sqlContext, cdr_mtrf_rdd, get_mtrf_schema(), ZTEUMTS_MTRF_TABLE_NAME)
        print "############### Saved ZTEUMTS_MTRF_TABLE_NAME ######################"
    if cdr_ncscph_rdd.count() > 0:
        insert_records_without_timestamp(sqlContext, cdr_ncscph_rdd, get_ncscph_schema(), ZTEUMTS_NCSCPH_TABLE_NAME)
        print "############### Saved ZTEUMTS_NCSCPH_TABLE_NAME ######################"
    if cdr_nilcs_rdd.count() > 0:
        insert_records_without_timestamp(sqlContext, cdr_nilcs_rdd, get_nilcs_schema(), ZTEUMTS_NILCS_TABLE_NAME)
        print "############### Saved ZTEUMTS_NILCS_TABLE_NAME ######################"
    if cdr_vig_rdd.count() > 0:
        insert_records_without_timestamp(sqlContext, cdr_vig_rdd, get_vig_schema(), ZTEUMTS_VIG_TABLE_NAME)
        print "############### Saved ZTEUMTS_VIG_TABLE_NAME ######################"
    if cdr_mcfcph_rdd.count() > 0:
        insert_records_without_timestamp(sqlContext, cdr_mcfcph_rdd, get_mcfcph_schema(), ZTEUMTS_MCFCPH_TABLE_NAME)
        print "############### Saved ZTEUMTS_MCFCPH_TABLE_NAME ######################"
    if cdr_tcicph_rdd.count() > 0:
        insert_records_without_timestamp(sqlContext, cdr_tcicph_rdd, get_tcicph_schema(), ZTEUMTS_TCICPH_TABLE_NAME)
        print "############### Saved ZTEUMTS_TCICPH_TABLE_NAME ######################"
    if cdr_mscsrvcc_rdd.count() > 0:
        insert_records_without_timestamp(sqlContext, cdr_mscsrvcc_rdd, get_mscsrvcc_schema(), ZTEUMTS_MSCSRVCC_TABLE_NAME)
        print "############### Saved ZTEUMTS_MSCSRVCC_TABLE_NAME ######################"
    if cdr_roam_rdd.count() > 0:
        insert_records_without_timestamp(sqlContext, cdr_roam_rdd, get_roam_schema(), ZTEUMTS_ROAM_TABLE_NAME)
        print "############### Saved ZTEUMTS_ROAM_TABLE_NAME ######################"
    if cdr_mocph_rdd.count() > 0:
        insert_records_without_timestamp(sqlContext, cdr_mocph_rdd, get_mocph_schema(), ZTEUMTS_MOCPH_TABLE_NAME)
        print "############### Saved ZTEUMTS_MOCPH_TABLE_NAME ######################"
    if cdr_moecall_rdd.count() > 0:
        insert_records_without_timestamp(sqlContext, cdr_moecall_rdd, get_moecall_schema(), ZTEUMTS_MOECALL_TABLE_NAME)
        print "############### Saved ZTEUMTS_MOECALL_TABLE_NAME ######################"
    if cdr_termcamelint_rdd.count() > 0:
        insert_records_without_timestamp(sqlContext, cdr_termcamelint_rdd, get_termcamelint_schema(), ZTEUMTS_TERMCAMELINT_TABLE_NAME)
        print "############### Saved ZTEUMTS_TERMCAMELINT_TABLE_NAME ######################"


def save_zteumts_stats(sc, sqlContext, cdr_iteration_stats_rdd):
    if cdr_iteration_stats_rdd.count() > 0:
        insert_records(sqlContext, cdr_iteration_stats_rdd, get_zteumts_stats_schema(),
                       ZTEUMTS_STATS_TABLE_NAME)
        print "############### Saved ZTEUMTS_STATS_TABLE_NAME ######################"


def get_zteumts_stats_rdd(parsed_records):
    return parsed_records.flatMap(lambda (x): [
        [x.iter_stats_zteumts.file_id,
         x.iter_stats_zteumts.file_name, x.iter_stats_zteumts.iteration_id, x.iter_stats_zteumts.checksum,
         x.iter_stats_zteumts.total_records, x.iter_stats_zteumts.success_count, x.iter_stats_zteumts.fail_count,
         x.iter_stats_zteumts.commonequip_base_total_count, x.iter_stats_zteumts.commonequip_base_success_count,
         x.iter_stats_zteumts.commonequip_base_failed_count,
         x.iter_stats_zteumts.incgateway_base_total_count, x.iter_stats_zteumts.incgateway_base_success_count,
         x.iter_stats_zteumts.incgateway_base_failed_count,
         x.iter_stats_zteumts.mcfcall_base_total_count, x.iter_stats_zteumts.mcfcall_base_success_count,
         x.iter_stats_zteumts.mcfcall_base_failed_count,
         x.iter_stats_zteumts.mocall_base_total_count, x.iter_stats_zteumts.mocall_base_success_count,
         x.iter_stats_zteumts.mocall_base_failed_count,
         x.iter_stats_zteumts.mosms_base_total_count, x.iter_stats_zteumts.mosms_base_success_count,
         x.iter_stats_zteumts.mosms_base_failed_count,
         x.iter_stats_zteumts.mtcall_base_total_count, x.iter_stats_zteumts.mtcall_base_success_count,
         x.iter_stats_zteumts.mtcall_base_failed_count,
         x.iter_stats_zteumts.mtlcs_base_total_count, x.iter_stats_zteumts.mtlcs_base_success_count,
         x.iter_stats_zteumts.mtlcs_base_failed_count,
         x.iter_stats_zteumts.mtsms_base_total_count, x.iter_stats_zteumts.mtsms_base_success_count,
         x.iter_stats_zteumts.mtsms_base_failed_count,
         x.iter_stats_zteumts.outgateway_base_total_count, x.iter_stats_zteumts.outgateway_base_success_count,
         x.iter_stats_zteumts.outgateway_base_failed_count,
         x.iter_stats_zteumts.ssaction_base_total_count, x.iter_stats_zteumts.ssaction_base_success_count,
         x.iter_stats_zteumts.ssaction_base_failed_count,
         x.iter_stats_zteumts.ussd_base_total_count, x.iter_stats_zteumts.ussd_base_success_count,
         x.iter_stats_zteumts.ussd_base_failed_count,
         x.iter_stats_zteumts.gsicph_base_total_count, x.iter_stats_zteumts.gsicph_base_success_count,
         x.iter_stats_zteumts.gsicph_base_failed_count,
         x.iter_stats_zteumts.hlrint_base_total_count, x.iter_stats_zteumts.hlrint_base_success_count,
         x.iter_stats_zteumts.hlrint_base_failed_count,
         x.iter_stats_zteumts.molcs_base_total_count, x.iter_stats_zteumts.molcs_base_success_count,
         x.iter_stats_zteumts.molcs_base_failed_count,
         x.iter_stats_zteumts.mtrf_base_total_count, x.iter_stats_zteumts.mtrf_base_success_count,
         x.iter_stats_zteumts.mtrf_base_failed_count,
         x.iter_stats_zteumts.ncscph_base_total_count, x.iter_stats_zteumts.ncscph_base_success_count,
         x.iter_stats_zteumts.ncscph_base_failed_count,
         x.iter_stats_zteumts.nilcs_base_total_count, x.iter_stats_zteumts.nilcs_base_success_count,
         x.iter_stats_zteumts.nilcs_base_failed_count,
         x.iter_stats_zteumts.vig_base_total_count, x.iter_stats_zteumts.vig_base_success_count,
         x.iter_stats_zteumts.vig_base_failed_count,
         x.iter_stats_zteumts.mcfcph_base_total_count, x.iter_stats_zteumts.mcfcph_base_success_count,
         x.iter_stats_zteumts.mcfcph_base_failed_count,
         x.iter_stats_zteumts.tcicph_base_total_count, x.iter_stats_zteumts.tcicph_base_success_count,
         x.iter_stats_zteumts.tcicph_base_failed_count,
         x.iter_stats_zteumts.mscsrvcc_base_total_count, x.iter_stats_zteumts.mscsrvcc_base_success_count,
         x.iter_stats_zteumts.mscsrvcc_base_failed_count,
         x.iter_stats_zteumts.roam_base_total_count, x.iter_stats_zteumts.roam_base_success_count,
         x.iter_stats_zteumts.roam_base_failed_count,
         x.iter_stats_zteumts.mocph_base_total_count, x.iter_stats_zteumts.mocph_base_success_count,
         x.iter_stats_zteumts.mocph_base_failed_count,
         x.iter_stats_zteumts.moecall_base_total_count, x.iter_stats_zteumts.moecall_base_success_count,
         x.iter_stats_zteumts.moecall_base_failed_count,
         x.iter_stats_zteumts.termcamelint_base_total_count, x.iter_stats_zteumts.termcamelint_base_success_count,
         x.iter_stats_zteumts.termcamelint_base_failed_count,
         get_current_time_day(), get_current_time()]])


def main():
    # logging.basicConfig(filename=get_logging_file_name("parsing", ZTEUMTS_LOG, ZTEUMTS), level=logging.INFO)

    iteration_id = str(get_current_time_and_day())
    start_time_for_iteration = time.time()
    ZTEUMTS_IN_PATH = sys.argv[1]
    sc = get_spark_context("zteumts_process")
    cdr_whole = sc.binaryFiles(ZTEUMTS_IN_PATH)

    print "##### Parsing ASN format records #####"
    # x = cdr_whole.first()
    # parsed_records = parse_zteumts_file(x[0], x[1], iteration_id)

    parsed_records = cdr_whole.flatMap(lambda (x, y): parse_zteumts_file(x, y, iteration_id))

    logging.debug("###### ZTEUMTS ASN1 parsing done #####")

    parsed_records.persist(storageLevel=StorageLevel.MEMORY_ONLY_SER)

    cdr_commonequip_rdd = parsed_records.flatMap(lambda (x): x.commonequip_array)
    cdr_incgateway_rdd = parsed_records.flatMap(lambda (x): x.incgateway_array)
    cdr_mcfcall_rdd = parsed_records.flatMap(lambda (x): x.mcfcall_array)
    cdr_mocall_rdd = parsed_records.flatMap(lambda (x): x.mocall_array)
    cdr_mosms_rdd = parsed_records.flatMap(lambda (x): x.mosms_array)
    cdr_mtcall_rdd = parsed_records.flatMap(lambda (x): x.mtcall_array)
    cdr_mtlcs_rdd = parsed_records.flatMap(lambda (x): x.mtlcs_array)
    cdr_mtsms_rdd = parsed_records.flatMap(lambda (x): x.mtsms_array)
    cdr_outgateway_rdd = parsed_records.flatMap(lambda (x): x.outgateway_array)
    cdr_ssaction_rdd = parsed_records.flatMap(lambda (x): x.ssaction_array)
    cdr_ussd_rdd = parsed_records.flatMap(lambda (x): x.ussd_array)
    cdr_gsicph_rdd = parsed_records.flatMap(lambda (x): x.gsicph_array)
    cdr_hlrint_rdd = parsed_records.flatMap(lambda (x): x.hlrint_array)
    cdr_molcs_rdd = parsed_records.flatMap(lambda (x): x.molcs_array)
    cdr_mtrf_rdd = parsed_records.flatMap(lambda (x): x.mtrf_array)
    cdr_ncscph_rdd = parsed_records.flatMap(lambda (x): x.ncscph_array)
    cdr_nilcs_rdd = parsed_records.flatMap(lambda (x): x.nilcs_array)
    cdr_vig_rdd = parsed_records.flatMap(lambda (x): x.vig_array)
    cdr_mcfcph_rdd = parsed_records.flatMap(lambda (x): x.mcfcph_array)
    cdr_tcicph_rdd = parsed_records.flatMap(lambda (x): x.tcicph_array)
    cdr_mscsrvcc_rdd = parsed_records.flatMap(lambda (x): x.mscsrvcc_array)
    cdr_roam_rdd = parsed_records.flatMap(lambda (x): x.roam_array)
    cdr_mocph_rdd = parsed_records.flatMap(lambda (x): x.mocph_array)
    cdr_moecall_rdd = parsed_records.flatMap(lambda (x): x.moecall_array)
    cdr_termcamelint_rdd = parsed_records.flatMap(lambda (x): x.termcamelint_array)
    cdr_iteration_stats_rdd = get_zteumts_stats_rdd(parsed_records)

    print "commonequipment counts:" + str(cdr_commonequip_rdd.count())
    print "incgateway counts:" + str(cdr_incgateway_rdd.count())
    print "mcfcall counts:" + str(cdr_mcfcall_rdd.count())
    print "mocall base counts:" + str(cdr_mocall_rdd.count())
    print "mosms base counts:" + str(cdr_mosms_rdd.count())
    print "mtcall base counts:" + str(cdr_mtcall_rdd.count())
    print "mtlcs base counts:" + str(cdr_mtlcs_rdd.count())
    print "mtsms base counts:" + str(cdr_mtsms_rdd.count())
    print "outgateway base counts:" + str(cdr_outgateway_rdd.count())
    print "ssaction base counts:" + str(cdr_ssaction_rdd.count())
    print "ussd counts:" + str(cdr_ussd_rdd.count())
    print "gsicph counts:" + str(cdr_gsicph_rdd.count())
    print "hlrint counts:" + str(cdr_hlrint_rdd.count())
    print "molcs counts:" + str(cdr_molcs_rdd.count())
    print "mtrf counts:" + str(cdr_mtrf_rdd.count())
    print "ncscph counts:" + str(cdr_ncscph_rdd.count())
    print "nilcs counts:" + str(cdr_nilcs_rdd.count())
    print "vig counts:" + str(cdr_vig_rdd.count())
    print "mcfcph counts:" + str(cdr_mcfcph_rdd.count())
    print "tcicph counts:" + str(cdr_tcicph_rdd.count())
    print "mscsrvcc counts:" + str(cdr_mscsrvcc_rdd.count())
    print "roam counts:" + str(cdr_roam_rdd.count())
    print "mocph counts:" + str(cdr_mocph_rdd.count())
    print "moecall counts:" + str(cdr_moecall_rdd.count())
    print "termcamelint counts:" + str(cdr_termcamelint_rdd.count())

    print "Time to iterate entire contents:" + str(time.time() - start_time_for_iteration)
    print "#################### Saving to impala. Done parsing"

    save_zteumts_data(sc, get_sql_context(sc), cdr_commonequip_rdd, cdr_incgateway_rdd, cdr_mcfcall_rdd,
                      cdr_mocall_rdd, cdr_mosms_rdd, cdr_mtcall_rdd, cdr_mtlcs_rdd, cdr_mtsms_rdd,
                      cdr_outgateway_rdd, cdr_ssaction_rdd, cdr_ussd_rdd, cdr_gsicph_rdd, cdr_hlrint_rdd,
                      cdr_molcs_rdd, cdr_mtrf_rdd, cdr_ncscph_rdd, cdr_nilcs_rdd, cdr_vig_rdd, cdr_mcfcph_rdd,
                      cdr_tcicph_rdd, cdr_mscsrvcc_rdd, cdr_roam_rdd, cdr_mocph_rdd, cdr_moecall_rdd,
                      cdr_termcamelint_rdd)

    save_zteumts_stats(sc, get_sql_context(sc), cdr_iteration_stats_rdd)
    print "Complete zte_umts iteration"


def parse_zteumts_file(hdfs_file_name, content, iteration_id):
    print "$$$$$$$$$$$$$$$$$$$$$$$$$$ Inside parsing $$$$$$$$$$$$$$$$$$$$$$$$$$"
    zteumts_file = StringIO.StringIO()
    zteumts_file.write(content)

    zteumts_file.seek(0)

    _absoluteFileName = hdfs_file_name.rpartition(':')[2]
    file_name = _absoluteFileName.rpartition('/')[2]
    filePath = _absoluteFileName.rpartition('/')[0]

    check_sum = get_file_check_sum(str(hdfs_file_name))
    #check_sum = 1234789
    file_id = str(uuid.uuid4())
    chunk_size = ZTEUMTS_FILE_CHUNK_SIZE

    bytes_already_read = 0
    k = 0
    i = 0

    commonequip_base_total_count = 0
    commonequip_base_failed_count = 0

    incgateway_base_total_count = 0
    incgateway_base_failed_count = 0

    mcfcall_base_total_count = 0
    mcfcall_base_failed_count = 0

    mocall_base_total_count = 0
    mocall_base_failed_count = 0

    mosms_base_total_count = 0
    mosms_base_failed_count = 0

    mtcall_base_total_count = 0
    mtcall_base_failed_count = 0

    mtlcs_base_total_count = 0
    mtlcs_base_failed_count = 0

    mtsms_base_total_count = 0
    mtsms_base_failed_count = 0

    outgateway_base_total_count = 0
    outgateway_base_failed_count = 0

    ssaction_base_total_count = 0
    ssaction_base_failed_count = 0

    ussd_base_total_count = 0
    ussd_base_failed_count = 0

    gsicph_base_total_count = 0
    gsicph_base_failed_count = 0

    hlrint_base_total_count = 0
    hlrint_base_failed_count = 0

    molcs_base_total_count = 0
    molcs_base_failed_count = 0

    mtrf_base_total_count = 0
    mtrf_base_failed_count = 0

    ncscph_base_total_count = 0
    ncscph_base_failed_count = 0

    nilcs_base_total_count = 0
    nilcs_base_failed_count = 0

    vig_base_total_count = 0
    vig_base_failed_count = 0

    mcfcph_base_total_count = 0
    mcfcph_base_failed_count = 0

    tcicph_base_total_count = 0
    tcicph_base_failed_count = 0

    mscsrvcc_base_total_count = 0
    mscsrvcc_base_failed_count = 0

    roam_base_total_count = 0
    roam_base_failed_count = 0

    mocph_base_total_count = 0
    mocph_base_failed_count = 0

    moecall_base_total_count = 0
    moecall_base_failed_count = 0

    termcamelint_base_total_count = 0
    termcamelint_base_failed_count = 0

    sys.tracebacklimit = 0

    file_size = len(content)
    bytes_to_read = chunk_size
    print "File size:" + str(file_size)
    cdr_parsed = []
    mocall_array = []
    mtcall_array = []
    incgateway_array = []
    outgateway_array = []
    mosms_array = []
    mtsms_array = []
    ssaction_array = []
    mcfcall_array = []
    commonequip_array = []
    mtlcs_array = []
    ussd_array = []
    gsicph_array = []
    hlrint_array = []
    molcs_array = []
    mtrf_array = []
    ncscph_array = []
    nilcs_array = []
    vig_array = []
    mcfcph_array = []
    tcicph_array = []
    mscsrvcc_array = []
    roam_array = []
    mocph_array = []
    moecall_array = []
    termcamelint_array = []

    while bytes_already_read < file_size:

        data = zteumts_file.read(bytes_to_read)
        is_record_ended_good = True
        while data != "":

            try:
                ab, data = decoder.decode(data, asn1Spec=CallEventRecord())

                mocall_record = ab.getComponentByName("moCallRecord")
                mtcall_record = ab.getComponentByName("mtCallRecord")
                incgateway_record = ab.getComponentByName("incGatewayRecord")
                outgateway_record = ab.getComponentByName("outGatewayRecord")
                mosms_record = ab.getComponentByName("moSMSRecord")
                mtsms_record = ab.getComponentByName("mtSMSRecord")
                ssaction_record = ab.getComponentByName("ssActionRecord")
                mcfcall_record = ab.getComponentByName("mcfCallRecord")
                commonequip_record = ab.getComponentByName("commonEquipRecord")
                mtlcs_record = ab.getComponentByName("mtLCSRecord")
                ussd_record = ab.getComponentByName("uSSDRecord")
                gsicph_record = ab.getComponentByName("gsiCPHRecord")
                hlrint_record = ab.getComponentByName("hlrIntRecord")
                molcs_record = ab.getComponentByName("moLCSRecord")
                mtrf_record = ab.getComponentByName("mTRFRecord")
                ncscph_record = ab.getComponentByName("ncsCPHRecord")
                nilcs_record = ab.getComponentByName("niLCSRecord")
                vig_record = ab.getComponentByName("vIGRecord")
                mcfcph_record = ab.getComponentByName("mcfCPHRecord")
                tcicph_record = ab.getComponentByName("tciCPHRecord")
                mscsrvcc_record = ab.getComponentByName("mSCSRVCCRecord")
                roam_record = ab.getComponentByName("roamRecord")
                mocph_record = ab.getComponentByName("moCPHRecord")
                moecall_record = ab.getComponentByName("moeCallRecord")
                termcamelint_record = ab.getComponentByName("termCAMELIntRecord")

                if mocall_record is not None:
                    mocall_base_total_count += 1
                    mocall_array.append(
                            ProcessZTEumtsMOCallRecord.process_mocall_records(mocall_record, file_id, file_name))
                elif mtcall_record is not None:
                    mtcall_base_total_count += 1
                    mtcall_array.append(
                            ProcessZTEumtsMTCallRecord.process_mtcall_records(mtcall_record, file_id, file_name))
                elif incgateway_record is not None:
                    incgateway_base_total_count += 1
                    incgateway_array.append(
                            ProcessZTEumtsIncGatewayRecord.process_incgateway_records(incgateway_record, file_id,
                                                                                      file_name))
                elif outgateway_record is not None:
                    outgateway_base_total_count += 1
                    outgateway_array.append(
                            ProcessZTEumtsOutGatewayRecord.process_outgateway_records(outgateway_record, file_id,
                                                                                      file_name))
                elif mosms_record is not None:
                    mosms_base_total_count += 1
                    mosms_array.append(
                        ProcessZTEumtsMOSMSRecord.process_mosms_records(mosms_record, file_id, file_name))
                elif mtsms_record is not None:
                    mtsms_base_total_count += 1
                    mtsms_array.append(
                        ProcessZTEumtsMTSMSRecord.process_mtsms_records(mtsms_record, file_id, file_name))
                elif ssaction_record is not None:
                    ssaction_base_total_count += 1
                    ssaction_array.append(
                            ProcessZTEumtsSSActionRecord.process_ssaction_records(ssaction_record, file_id, file_name))
                elif mcfcall_record is not None:
                    mcfcall_base_total_count += 1
                    mcfcall_array.append(
                        ProcessZTEumtsMCFCallRecord.process_mcfcall_records(mcfcall_record, file_id, file_name))
                elif commonequip_record is not None:
                    commonequip_base_total_count += 1
                    commonequip_array.append(
                            ProcessZTEumtsCommonEquipRecord.process_commonequip_records(commonequip_record, file_id,
                                                                                        file_name))
                elif mtlcs_record is not None:
                    mtlcs_base_total_count += 1
                    mtlcs_array.append(
                        ProcessZTEumtsmtLCSRecordRecord.process_mtlcs_records(mtlcs_record, file_id, file_name))
                elif ussd_record is not None:
                    ussd_base_total_count += 1
                    ussd_array.append(ProcessZTEumtsUSSDRecord.process_ussd_records(ussd_record, file_id, file_name))
                elif gsicph_record is not None:
                    gsicph_base_total_count += 1
                    # gsicph_array.append(ProcessZTEumtsGSICPHRecord.process_gsicph_records(gsicph_record, file_name))
                elif hlrint_record is not None:
                    hlrint_base_total_count += 1
                    # hlrint_array.append(ProcessZTEumtsHLRIntRecord.process_hlrint_records(hlrint_record, file_name))
                elif molcs_record is not None:
                    molcs_base_total_count += 1
                    # molcs_array.append(ProcessZTEumtsmoLCSRecordRecord.process_molcs_records(molcs_record, file_name))
                elif mtrf_record is not None:
                    mtrf_base_total_count += 1
                    # mtrf_array.append(ProcessZTEumtsMTRFRecord.process_mtrf_records(mtrf_record, file_name))
                elif ncscph_record is not None:
                    ncscph_base_total_count += 1
                    # ncscph_array.append(ProcessZTEumtsNCSCPHRecord.process_ncscph_records(ncscph_record, file_name))
                elif nilcs_record is not None:
                    nilcs_base_total_count += 1
                    # nilcs_array.append(ProcessZTEumtsNILCSRecord.process_nilcs_records(nilcs_record, file_name))
                elif vig_record is not None:
                    vig_base_total_count += 1
                    # vig_array.append(ProcessZTEumtsVIGRecord.process_vig_records(vig_record, file_name))
                elif mcfcph_record is not None:
                    mcfcph_base_total_count += 1
                    # mcfcph_array.append(ProcessZTEumtsMCFCPHRecord.process_mcfcph_records(mcfcph_record, file_name))
                elif tcicph_record is not None:
                    tcicph_base_total_count += 1
                    # tcicph_array.append(ProcessZTEumtsTCICPHRecord.process_tcicph_records(tcicph_record, file_name))
                elif mscsrvcc_record is not None:
                    mscsrvcc_base_total_count += 1
                    # mscsrvcc_array.append(
                    #         ProcessZTEumtsMSCSRVCCRecord.process_mscsrvcc_records(mscsrvcc_record, file_name))
                elif roam_record is not None:
                    roam_base_total_count += 1
                    # roam_array.append(ProcessZTEumtsROAMRecord.process_roam_records(roam_record, file_name))
                elif mocph_record is not None:
                    mocph_base_total_count += 1
                    # mocph_array.append(ProcessZTEumtsMOCPHRecord.process_mocph_records(mocph_record, file_name))
                elif moecall_record is not None:
                    moecall_base_total_count += 1
                    # moecall_array.append(ProcessZTEumtsMOECallRecord.process_moecall_records(moecall_record, file_name))
                elif termcamelint_record is not None:
                    termcamelint_base_total_count += 1
                    # termcamelint_array.append(
                    #         ProcessZTEumtsTermCAMELIntRecord.process_termcamelint_records(termcamelint_record,
                    #                                                                       file_name))

                i += 1

            except error.SubstrateUnderrunError as e:
                is_record_ended_good = False
                last_record_position = zteumts_file.tell()
                zteumts_file.seek(last_record_position)
                bytes_already_read += (bytes_to_read - len(data))
                bytes_to_read = chunk_size
                break
            except error.PyAsn1Error as p:
                is_record_ended_good = False
                logging.exception(p.message)
                last_record_position = zteumts_file.tell()
                zteumts_file.seek(last_record_position)
                print "PyAsn1Error : data bytes left in this chunk:" + str(len(data)) + " , Current position in file :" \
                      + str(last_record_position) + " , Seek for next chunk: " + str(zteumts_file.tell()) + \
                      ", chunk number is:" + str(k)
                bytes_already_read += (bytes_to_read - len(data))
                bytes_to_read = chunk_size
                break
            except Exception as ep:
                logging.exception(ep.message)

        if is_record_ended_good:
            last_record_position = zteumts_file.tell()
            zteumts_file.seek(last_record_position)
            print "Surprise - chunk ended with exact record after : data bytes left in this chunk:" + str(
                    len(data)) + " , Current position in file :" \
                  + str(last_record_position) + " , Seek for next chunk: " + str(zteumts_file.tell()) + \
                  ", chunk number is:" + str(k) + ", after record number:" + str(i)
            bytes_already_read += bytes_to_read
            bytes_to_read = chunk_size

        k += 1

        if len(data) == 0 and last_record_position == file_size:
            print "Seems we reached end of file with bytes_already_read as:" + str(bytes_already_read)
            break

    commonequip_base_success_count = len(commonequip_array)
    incgateway_base_success_count = len(incgateway_array)
    mcfcall_base_success_count = len(mcfcall_array)
    mocall_base_success_count = len(mocall_array)
    mosms_base_success_count = len(mosms_array)
    mtcall_base_success_count = len(mtcall_array)
    mtlcs_base_success_count = len(mtlcs_array)
    mtsms_base_success_count = len(mtsms_array)
    outgateway_base_success_count = len(outgateway_array)
    ssaction_base_success_count = len(ssaction_array)
    ussd_base_success_count = len(ussd_array)
    gsicph_base_success_count = len(gsicph_array)
    hlrint_base_success_count = len(hlrint_array)
    molcs_base_success_count = len(molcs_array)
    mtrf_base_success_count = len(mtrf_array)
    ncscph_base_success_count = len(ncscph_array)
    nilcs_base_success_count = len(nilcs_array)
    vig_base_success_count = len(vig_array)
    mcfcph_base_success_count = len(mcfcph_array)
    tcicph_base_success_count = len(tcicph_array)
    mscsrvcc_base_success_count = len(mscsrvcc_array)
    roam_base_success_count = len(roam_array)
    mocph_base_success_count = len(mocph_array)
    moecall_base_success_count = len(moecall_array)
    termcamelint_base_success_count = len(termcamelint_array)

    total_count = commonequip_base_total_count + incgateway_base_total_count + mcfcall_base_total_count + \
                  mocall_base_total_count + mosms_base_total_count + mtcall_base_total_count + \
                  mtlcs_base_total_count + mtsms_base_total_count + outgateway_base_total_count + \
                  ssaction_base_total_count + ussd_base_total_count + \
                  gsicph_base_total_count + hlrint_base_total_count + molcs_base_total_count + \
                  mtrf_base_total_count + ncscph_base_total_count + nilcs_base_total_count + \
                  vig_base_total_count + mcfcph_base_total_count + tcicph_base_total_count + \
                  mscsrvcc_base_total_count + \
                  roam_base_total_count + mocph_base_total_count + moecall_base_total_count + \
                  termcamelint_base_total_count

    success_count = commonequip_base_success_count + incgateway_base_success_count + mcfcall_base_success_count + \
                    mocall_base_success_count + mosms_base_success_count + mtcall_base_success_count + \
                    mtlcs_base_success_count + mtsms_base_success_count + outgateway_base_success_count + \
                    ssaction_base_success_count + ussd_base_success_count + gsicph_base_success_count + \
                    hlrint_base_success_count + molcs_base_success_count + mtrf_base_success_count + \
                    ncscph_base_success_count + nilcs_base_success_count + vig_base_success_count + \
                    mcfcph_base_success_count + tcicph_base_success_count + mscsrvcc_base_success_count + \
                    roam_base_success_count + mocph_base_success_count + moecall_base_success_count + \
                    termcamelint_base_success_count

    fail_count = commonequip_base_failed_count + incgateway_base_failed_count + mcfcall_base_failed_count + \
                 mocall_base_failed_count + mosms_base_failed_count + mtcall_base_failed_count + \
                 mtlcs_base_failed_count + mtsms_base_failed_count + outgateway_base_failed_count + \
                 ssaction_base_failed_count + ussd_base_failed_count + gsicph_base_failed_count + \
                 hlrint_base_failed_count + molcs_base_failed_count + mtrf_base_failed_count + \
                 ncscph_base_failed_count + nilcs_base_failed_count + vig_base_failed_count + mcfcph_base_failed_count + \
                 tcicph_base_failed_count + mscsrvcc_base_failed_count + roam_base_failed_count + mocph_base_failed_count + \
                 moecall_base_failed_count + termcamelint_base_failed_count

    zteumts_stats = ZTEUMTSStats(iteration_id, file_id, file_name, check_sum, total_count, success_count, fail_count,
                                 commonequip_base_total_count, commonequip_base_success_count,
                                 commonequip_base_failed_count,
                                 incgateway_base_total_count, incgateway_base_success_count,
                                 incgateway_base_failed_count,
                                 mcfcall_base_total_count, mcfcall_base_success_count, mcfcall_base_failed_count,
                                 mocall_base_total_count, mocall_base_success_count, mocall_base_failed_count,
                                 mosms_base_total_count, mosms_base_success_count, mosms_base_failed_count,
                                 mtcall_base_total_count, mtcall_base_success_count, mtcall_base_failed_count,
                                 mtlcs_base_total_count, mtlcs_base_success_count, mtlcs_base_failed_count,
                                 mtsms_base_total_count, mtsms_base_success_count, mtsms_base_failed_count,
                                 outgateway_base_total_count, outgateway_base_success_count,
                                 outgateway_base_failed_count,
                                 ssaction_base_total_count, ssaction_base_success_count, ssaction_base_failed_count,
                                 ussd_base_total_count, ussd_base_success_count, ussd_base_failed_count,
                                 gsicph_base_total_count, gsicph_base_success_count, gsicph_base_failed_count,
                                 hlrint_base_total_count, hlrint_base_success_count, hlrint_base_failed_count,
                                 molcs_base_total_count, molcs_base_success_count, molcs_base_failed_count,
                                 mtrf_base_total_count, mtrf_base_success_count, mtrf_base_failed_count,
                                 ncscph_base_total_count, ncscph_base_success_count, ncscph_base_failed_count,
                                 nilcs_base_total_count, nilcs_base_success_count, nilcs_base_failed_count,
                                 vig_base_total_count, vig_base_success_count, vig_base_failed_count,
                                 mcfcph_base_total_count, mcfcph_base_success_count, mcfcph_base_failed_count,
                                 tcicph_base_total_count, tcicph_base_success_count, tcicph_base_failed_count,
                                 mscsrvcc_base_total_count, mscsrvcc_base_success_count, mscsrvcc_base_failed_count,
                                 roam_base_total_count, roam_base_success_count, roam_base_failed_count,
                                 mocph_base_total_count, mocph_base_success_count, mocph_base_failed_count,
                                 moecall_base_total_count, moecall_base_success_count, moecall_base_failed_count,
                                 termcamelint_base_total_count, termcamelint_base_success_count,
                                 termcamelint_base_failed_count
                                 )

    cdr_parsed.append(
            ZteumtsParsedRecord(file_name, zteumts_stats, commonequip_array, incgateway_array, mcfcall_array,
                                mocall_array,
                                mosms_array, mtcall_array, mtlcs_array, mtsms_array, outgateway_array, ssaction_array,
                                ussd_array, gsicph_array, hlrint_array, molcs_array, mtrf_array, ncscph_array,
                                nilcs_array, vig_array, mcfcph_array, tcicph_array, mscsrvcc_array, roam_array,
                                mocph_array, moecall_array, termcamelint_array))
    zteumts_file.close()

    return cdr_parsed


if __name__ == "__main__":
    sys.exit(main())
