from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType, TimestampType, ShortType

def get_traffic_volume_schema():

    traffic_volume_schema = StructType([ StructField("file_id", StringType(), False),
                                         StructField("trafficVolumesKey", StringType(), False),
                                         StructField("changeCondition", IntegerType(), False),
                                         StructField("changeTime", TimestampType(), False),
                                         StructField("dataVolumeGPRSUplink", LongType(), False),
                                         StructField("dataVolumeGPRSDownlink", LongType(), False),
                                         StructField("qosRequested", StringType(), False),
                                         StructField("qosNegotiated", StringType(), False)
                                         ])

    return traffic_volume_schema


def get_camel_info_pdp_schema():

    camel_info_pdp_schema = StructType([ StructField("file_id", StringType(), False),
                                         StructField("cAMELInformationPDPKey", StringType(), False),
                                         StructField("cAMELAccessPointNameNI", StringType(), False),
                                         StructField("cAMELAccessPointNameOI", StringType(), False),
                                         StructField("defaultTransactionHandling", IntegerType(), False),
                                         StructField("fFDAppendIndicator", IntegerType(), False),
                                         StructField("freeFormatData", StringType(), False),
                                         StructField("levelOfCAMELService", LongType(), False),
                                         StructField("numberOfDPEncountered", LongType(), False),
                                         StructField("sCFAddress", StringType(), False),
                                         StructField("serviceKey", LongType(), False)
                                         ])
    return camel_info_pdp_schema

def get_camel_info_sms_schema():

    camel_info_sms_schema = StructType([ StructField("file_id", StringType(), False),
                                         StructField("cAMELInformationSMSKey", StringType(), False),
                                         StructField("cAMELCallingPartyNumber", StringType(), False),
                                         StructField("cAMELDestinationSubscriberNumber", StringType(), False),
                                         StructField("cAMELSMSCAddress", StringType(), False),
                                         StructField("defaultSMSHandling", IntegerType(), False),
                                         StructField("freeFormatData", StringType(), False),
                                         StructField("sCFAddress", StringType(), False),
                                         StructField("serviceKey", LongType(), False),
                                         StructField("smsReferenceNumber", StringType(), False)
                                         ])

    return camel_info_sms_schema

def get_record_extension_schema():

    record_extension_schema = StructType([ StructField("file_id", StringType(), False),
                                           StructField("recordExtensionsKey", StringType(), False),
                                           StructField("identifier", StringType(), False),
                                           StructField("information", StringType(), False),
                                           StructField("significance", StringType(), False)])

    return record_extension_schema


def get_smo_schema():

    # Custom schema to read input
    smo_schema = StructType([StructField("file_id", StringType(), False),
                             StructField("cAMELInformationPDPKey", StringType(), False),
                             StructField("cellIdentifier", StringType(), False),
                             StructField("chargingCharacteristics", StringType(), False),
                             StructField("chChSelectionMode", StringType(), False),
                             StructField("destinationNumber", StringType(), False),
                             StructField("eventTimeStamp", TimestampType(), False),
                             StructField("localSequenceNumber", LongType(), False),
                             StructField("locationAreaCode", StringType(), False),
                             StructField("messageReference", StringType(), False),
                             StructField("msNetworkCapability", StringType(), False),
                             StructField("nodeID", StringType(), False),
                             StructField("pLMNIdentifier", StringType(), False),
                             StructField("rATType", LongType(), False),
                             StructField("recordingEntity", StringType(), False),
                             StructField("recordType", LongType(), False),
                             StructField("routingArea", StringType(), False),
                             StructField("servedIMEI", StringType(), False),
                             StructField("servedIMSI", StringType(), False),
                             StructField("servedMSISDN", StringType(), False),
                             StructField("serviceCentre", StringType(), False),
                             StructField("smsResult", LongType(), False),
                             StructField("timeframe_day", LongType(), False),
                             StructField("timeframe_hr", IntegerType(), False)])
    return smo_schema

def get_smt_schema():

    # Custom schema to read input
    smt_schema = StructType([
        StructField("file_id", StringType(), False),
        StructField("cellIdentifier", StringType(), False),
        StructField("cellIdentifierLastSM", StringType(), False),
        StructField("chargingCharacteristics", StringType(), False),
        StructField("chChSelectionMode", StringType(), False),
        StructField("eventTimeStamp", TimestampType(), False),
        StructField("localSequenceNumber", LongType(), False),
        StructField("locationAreaCode", StringType(), False),
        StructField("locationAreaLastSM", StringType(), False),
        StructField("msNetworkCapability", StringType(), False),
        StructField("nodeID", StringType(), False),
        StructField("numberOfSM", LongType(), False),
        StructField("pLMNIdentifier", StringType(), False),
        StructField("pLMNIdentifierLastSM", StringType(), False),
        StructField("rATType", LongType(), False),
        StructField("recordingEntity", StringType(), False),
        StructField("recordType", LongType(), False),
        StructField("routingArea", StringType(), False),
        StructField("routingAreaLastSM", StringType(), False),
        StructField("servedIMEI", StringType(), False),
        StructField("servedIMSI", StringType(), False),
        StructField("servedMSISDN", StringType(), False),
        StructField("serviceCentre", StringType(), False),
        StructField("smsResult", LongType(), False),
        StructField("timeframe_day", LongType(), False),
        StructField("timeframe_hr", IntegerType(), False)])
    return smt_schema

def get_pdp_schema():

    # Custom schema to read input
    pdp_schema = StructType([StructField("file_id", StringType(), False),
                             StructField("accessPointNameNI", StringType(), False),
                             StructField("accessPointNameOI", StringType(), False),
                             StructField("apnSelectionMode", StringType(), False),
                             StructField("cAMELInformationPDPKey", StringType(), False),
                             StructField("causeForRecClosing", LongType(), False),
                             StructField("cellIdentifier", StringType(), False),
                             StructField("chargingCharacteristics", StringType(), False),
                             StructField("chargingID", LongType(), False),
                             StructField("chChSelectionMode", StringType(), False),
                             StructField("duration", LongType(), False),
                             StructField("dynamicAddressFlag", StringType(), False),
                             StructField("ggsnAddressUsed", StringType(), False),
                             StructField("integer", LongType(), False),
                             StructField("localSequenceNumber", LongType(), False),
                             StructField("locationAreaCode", StringType(), False),
                             StructField("msNetworkCapability", StringType(), False),
                             StructField("mSTimeZone", StringType(), False),
                             StructField("networkInitiation", IntegerType(), False),
                             StructField("nodeID", StringType(), False),
                             StructField("pdpType", StringType(), False),
                             StructField("pLMNIdentifier", StringType(), False),
                             StructField("rATType", LongType(), False),
                             StructField("recordExtensionsKey", StringType(), False),
                             StructField("recordOpeningTime", TimestampType(), False),
                             StructField("recordSequenceNumber", LongType(), False),
                             StructField("recordType", LongType(), False),
                             StructField("routingArea", StringType(), False),
                             StructField("servedIMEI", StringType(), False),
                             StructField("servedIMSI", StringType(), False),
                             StructField("servedMSISDN", StringType(), False),
                             StructField("servedPDPAddress", StringType(), False),
                             StructField("sgsnAddress", StringType(), False),
                             StructField("sgsnChange", ShortType(), False),
                             StructField("trafficVolumesKey", StringType(), False),
                             StructField("timeframe_day", LongType(), False),
                             StructField("timeframe_hr", IntegerType(), False)])
    return pdp_schema


def get_ericsson_stats_schema():

    # Custom schema to read input
    ericsson_stats_schema = StructType([
                            StructField("iteration_id", StringType(), False),
                            StructField("file_id", StringType(), False),
                            StructField("file_name", StringType(), False),
                            StructField("checksum", StringType(), False),
                            StructField("total_records", LongType(), False),
                            StructField("success_count", LongType(), False),
                            StructField("fail_count", LongType(), False),
                            StructField("pdp_base_total_count", LongType(), False),
                            StructField("pdp_base_success_count", LongType(), False),
                            StructField("pdp_base_failed_count", LongType(), False),
                            StructField("pdp_traffic_count", LongType(), False),
                            StructField("pdp_camel_count", LongType(), False),
                            StructField("pdp_rec_ext_count", LongType(), False),
                            StructField("smo_base_total_count", LongType(), False),
                            StructField("smo_base_success_count", LongType(), False),
                            StructField("smo_base_failed_count", LongType(), False),
                            StructField("smo_camel_count", LongType(), False),
                            StructField("smt_base_total_count", LongType(), False),
                            StructField("smt_base_success_count", LongType(), False),
                            StructField("smt_base_failed_count", LongType(), False),
                            StructField("timeframe_day", LongType(), False),
                            StructField("timeframe_hr", IntegerType(), False)
                         ])
    return ericsson_stats_schema
