from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType, TimestampType, ShortType




def get_ggsn_schema():
    ggsn_schema = StructType([StructField("file_id", StringType(), False),
                              StructField("recordtype", StringType(), False),
                              StructField("served_imsi", StringType(), False),
                              StructField("ggsn_address_used", StringType(), False),
                              StructField("charging_id", LongType(), False),
                              StructField("sgsn_address", StringType(), False),
                              StructField("accesspointni", StringType(), False),
                              StructField("pdp_type", StringType(), False),
                              StructField("served_pdp_address", StringType(), False),
                              StructField("dynamic_address_flag", LongType(), False),
                              StructField("traffic_volumes_key2", StringType(), False),
                              StructField("record_opening_time", TimestampType(), False),
                              StructField("duration", LongType(), False),
                              StructField("cause_for_rec_closing", LongType(), False),
                              StructField("record_sequence_number", LongType(), False),
                              StructField("node_id", StringType(), False),
                              StructField("record_extension_key", StringType(), False),
                              StructField("local_sequence_number", StringType(), False),
                              StructField("apn_selection_mode", StringType(), False),
                              StructField("served_msisdn", StringType(), False),
                              StructField("charging_characteristics", StringType(), False),
                              StructField("ch_ch_selection_mode", StringType(), False),
                              StructField("iMSsignalingContext", StringType(), False),
                              StructField("sgsnPLMNIdentifierr", StringType(), False),
                              StructField("served_imei", StringType(), False),
                              StructField("rat_type", IntegerType(), False),
                              StructField("ms_time_zone", StringType(), False),
                              StructField("userLocationInformation1", StringType(), False),
                              StructField("file_created_timestamp", LongType(), False),
                              StructField("year", IntegerType(), False),
                              StructField("month", IntegerType(), False),
                              StructField("day", IntegerType(), False)
                              ])

    return ggsn_schema





def get_egsn_schema():
    egsn_schema = StructType([StructField("file_id", StringType(), False),
                              StructField("recordtype", StringType(), False),
                              StructField("served_imsi", StringType(), False),
                              StructField("ggsn_address_used", StringType(), False),
                              StructField("charging_id", LongType(), False),
                              StructField("sgsn_address", StringType(), False),
                              StructField("access_point_name_ni", StringType(), False),
                              StructField("pdp_type", StringType(), False),
                              StructField("served_pdp_address", StringType(), False),
                              StructField("dynamic_address_flag", LongType(), False),
                              StructField("traffic_volumes_key1", StringType(), False),
                              StructField("record_opening_time", TimestampType(), False),
                              StructField("duration", LongType(), False),
                              StructField("cause_for_rec_closing", LongType(), False),
                              StructField("record_sequence_number", LongType(), False),
                              StructField("node_id", StringType(), False),
                              StructField("record_extension_key", StringType(), False),
                              StructField("local_sequence_number", StringType(), False),
                              StructField("apn_selection_mode", StringType(), False),
                              StructField("served_msisdn", StringType(), False),
                              StructField("charging_characteristics", StringType(), False),
                              StructField("ch_ch_selection_mode", StringType(), False),
                              StructField("iMSsignalingContext", StringType(), False),
                              StructField("sgsnPLMNIdentifierr", StringType(), False),
                              StructField("served_imeisv", StringType(), False),
                              StructField("rat_type", IntegerType(), False),
                              StructField("ms_time_zone", StringType(), False),
                              StructField("userLocationInformation2", StringType(), False),
                              StructField("listofservicedata_key", StringType(), False),
                              StructField("file_created_timestamp", LongType(), False),
                              StructField("year", IntegerType(), False),
                              StructField("month", IntegerType(), False),
                              StructField("day", IntegerType(), False)
                              ])

    return egsn_schema

def get_ericsson_stats_schema():

    # Custom schema to read input
    ericssonggsn_stats_schema = StructType([
                            StructField("iteration_id", StringType(), False),
                            StructField("file_id", StringType(), False),
                            StructField("file_name", StringType(), False),
                            StructField("checksum", StringType(), False),
                            StructField("total_records", LongType(), False),
                            StructField("success_count", LongType(), False),
                            StructField("fail_count", LongType(), False),
                            StructField("ggsn_total_count", LongType(), False),
                            StructField("ggsn_success_count", LongType(), False),
                            StructField("ggsn_failed_count", LongType(), False),
                            StructField("ggsn_traffic_count", LongType(), False),
                            StructField("ggsn_record_extension_count", LongType(), False),
                            StructField("egsn_total_count", LongType(), False),
                            StructField("egsn_success_count", LongType(), False),
                            StructField("egsn_failed_count", LongType(), False),
                            StructField("egsn_traffic_count", LongType(), False),
                            StructField("egsn_pdp_servicedata_count", LongType(), False),
                            StructField("egsn_record_extension_count", LongType(), False),
                            StructField("file_created_timestamp", LongType(), False),
                            StructField("year", IntegerType(), False),
                            StructField("month", IntegerType(), False),
                            StructField("day", IntegerType(), False)])
    return ericssonggsn_stats_schema




def get_traffic_volume_schema_ggsn():

    traffic_volume_schema = StructType([ StructField("file_id", StringType(), False),
                                         StructField("traffic_volumes_key2", StringType(), False),
                                         StructField("change_condition", IntegerType(), False),
                                         StructField("change_time", TimestampType(), False),
                                         StructField("data_volume_gprs_downlink", LongType(), False),
                                         StructField("data_volume_gprs_uplink", LongType(), False),
                                         StructField("qos_negotiated", StringType(), False),
                                         StructField("userLocationInformation", StringType(), False),
                                         StructField("ePCQoSInformation", StringType(), False),
                                         StructField("file_created_timestamp", LongType(), False),
                                         StructField("year", IntegerType(), False),
                                         StructField("month", IntegerType(), False),
                                         StructField("day", IntegerType(), False)
                                         ])

    return traffic_volume_schema


def get_traffic_volume_schema_egsn():

    traffic_volume_schema = StructType([ StructField("file_id", StringType(), False),
                                         StructField("traffic_volumes_key1", StringType(), False),
                                         StructField("change_condition", IntegerType(), False),
                                         StructField("change_time", TimestampType(), False),
                                         StructField("data_volume_gprs_downlink", LongType(), False),
                                         StructField("data_volume_gprs_uplink", LongType(), False),
                                         StructField("qos_negotiated", StringType(), False),
                                         StructField("userLocationInformation", StringType(), False),
                                         StructField("ePCQoSInformation", StringType(), False),
                                         StructField("file_created_timestamp", LongType(), False),
                                         StructField("year", IntegerType(), False),
                                         StructField("month", IntegerType(), False),
                                         StructField("day", IntegerType(), False)
                                         ])

    return traffic_volume_schema




def get_record_extension_schema_ggsn():

    record_extension_schema = StructType([ StructField("file_id", StringType(), False),
                                           StructField("recordExtensionsKey", StringType(), False),
                                           StructField("identifier", StringType(), False),
                                           StructField("information", StringType(), False),
                                           StructField("significance", StringType(), False),
                                           StructField("file_created_timestamp", LongType(), False),
                                           StructField("year", IntegerType(), False),
                                           StructField("month", IntegerType(), False),
                                           StructField("day", IntegerType(), False)
                                           ])

    return record_extension_schema


def get_record_extension_schema_egsn():

    record_extension_schema = StructType([ StructField("file_id", StringType(), False),
                                           StructField("recordExtensionsKey", StringType(), False),
                                           StructField("identifier", StringType(), False),
                                           StructField("significance", StringType(), False),
                                           StructField("file_created_timestamp", LongType(), False),
                                           StructField("year", IntegerType(), False),
                                           StructField("month", IntegerType(), False),
                                           StructField("day", IntegerType(), False)
                                           ])

    return record_extension_schema
def get_service_data_schema_egsn():
    egsn_schema_list_of_ser_data = StructType([StructField("file_id", StringType(), False),
                              StructField("listofservicedata_key", StringType(), False),
                              StructField("ratinggroup", StringType(), False),
                              StructField("resultCode", StringType(), False),
                              StructField("local_Sequence_Number", StringType(), False),
                              StructField("timeOfFirstUsage", TimestampType(), False),
                              StructField("timeOfLastUsage", TimestampType(), False),
                              StructField("timeUsage", StringType(), False),
                              StructField("sgsn-Address", StringType(), False),
                              StructField("datavolumeFBCUplink", StringType(), False),
                              StructField("datavolumeFBCDownlink", StringType(), False),
                              StructField("timeOfReport", TimestampType(), False),
                              StructField("failureHandlingContinue", StringType(), False),
                              StructField("serviceIdentifier", StringType(), False),
                              StructField("pSFurnishChargingInformation",StringType(), False),
                              StructField("aFRecordInformation", StringType(), False),
                              StructField("userLocationInformation", StringType(), False),
                              StructField("eventBasedChargingInformation", StringType(), False),
                              StructField("sGSNPLMNIdentifier", StringType(), False),
                              StructField("rat_type", StringType(), False),
                              StructField("qoSInformationNeg", StringType(), False),
                              StructField("file_created_timestamp", LongType(), False),
                              StructField("year", IntegerType(), False),
                              StructField("month", IntegerType(), False),
                              StructField("day", IntegerType(), False)
                              ])

    return egsn_schema_list_of_ser_data
