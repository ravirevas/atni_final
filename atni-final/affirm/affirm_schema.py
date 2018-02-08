from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType, TimestampType, ShortType

def get_pgw_schema():

    pgw_schema = StructType([StructField("file_id", StringType(), False),
                             StructField("recordType", IntegerType(), False),
                             StructField("servedIMSI", StringType(), False),
                             StructField("pgwAddress", StringType(), False),
                             StructField("chargingID", LongType(), False),
                             StructField("accessPointNameNI", StringType(), False),
                             StructField("servingnodeaddress", StringType(), False),
                             StructField("pdpType", StringType(), False),
                             StructField("servedPDPAddress", StringType(), False),
                             StructField("dynamicAddressFlag", StringType(), False),
                             StructField("recordOpeningTime", StringType(), False),
                             StructField("duration", LongType(), False),
                             StructField("causeForRecClosing", StringType(), False),
                             StructField("diagonstices", StringType(), False),
                             StructField("recordSequenceNumber", LongType(), False),
                             StructField("nodeID", StringType(), False),
                             StructField("localSequenceNumber", IntegerType(), False),
                             StructField("apnSelectionMode", StringType(), False),
                             StructField("chargingCharacteristics", StringType(), False),
                             StructField("chChSelectionMode", StringType(), False),
                             StructField("servingNodePLMNIdentifier", StringType(), False),
                             StructField("pSFurnishChargingInformation", StringType(), False),
                             StructField("servedIMEISV", StringType(), False),
                             StructField("mSTimeZone", StringType(), False),
                             StructField("userLocationInformation", StringType(), False),
                             StructField("listOfServiceData_key", StringType(), False),
                             StructField("servingNodeType", StringType(), False),
                             StructField("subscriptionIDType", StringType(), False),
                             StructField("subscriptionIDData", StringType(), False),
                             StructField("pGWPLMNIdentifier", StringType(), False),
                             StructField("servedMSISDN", StringType(), False),
                             StructField("starttime", StringType(), False),
                             StructField("stoptime", StringType(), False),
                             StructField("pDNConnectionID", IntegerType(), False),
                             StructField("servedPDPPDNAddressExt", StringType(), False),
                             StructField("rATType", LongType(), False),
                             StructField("file_created_timestamp", IntegerType(), False),
                             StructField("year", IntegerType(), False),
                             StructField("month", IntegerType(), False),
                             StructField("day", IntegerType(), False)
                             ])

    return pgw_schema


def get_cdr_pdp_lsd_schema():
    listofservicedata_schema = StructType([StructField("file_id", StringType(), False),
                                         StructField("listofservicedata_key", StringType(), False),
                                         StructField("ratingGroup", StringType(), False),
                                         StructField("chargingRuleBaseName", StringType(), False),
                                         StructField("resultCode", StringType(), False),
                                         StructField("localSequenceNumber", StringType(), False),
                                         StructField("timeOfFirstUsage", StringType(), False),
                                         StructField("timeOfLastUsage", StringType(), False),
                                         StructField("timeUsage", StringType(), False),
                                         StructField("serviceConditionChange", StringType(), False),
                                         StructField("servingNodeAddress", StringType(), False),
                                         StructField("datavolumeFBCUplink", StringType(), False),
                                         StructField("datavolumeFBCDownlink", StringType(), False),
                                         StructField("timeOfReport", StringType(), False),
                                         StructField("failureHandlingContinue", StringType(), False),
                                         StructField("serviceIdentifier", IntegerType(), False),
                                         StructField("pSFurnishChargingInformation", StringType(), False),
                                         StructField("userLocationInformation", StringType(), False),
                                         StructField("file_created_timestamp", IntegerType(), False),
                                         StructField("year", IntegerType(), False),
                                         StructField("month", IntegerType(), False),
                                         StructField("day", IntegerType(), False)
                                            ])

    return listofservicedata_schema


def get_sgw_schema():

    sgw_schema = StructType([StructField("file_id", StringType(), False),
                             StructField("recordType", LongType(), False),
                             StructField("servedIMSI", StringType(), False),
                             StructField("s-GWAddress", StringType(), False),
                             StructField("chargingID", LongType(), False),
                             StructField("servingNodeAddress", StringType(), False),
                             StructField("accessPointNameNI", StringType(), False),
                             StructField("pdpPDNType", StringType(), False),
                             StructField("servedPDPPDNAddressEx", StringType(), False),
                             StructField("dynamicAddressFlag", StringType(), False),
                             StructField("listOfTrafficVolumes", StringType(), False),
                             StructField("recordOpeningTime", TimestampType(), False),
                             StructField("duration", LongType(), False),
                             StructField("causeForRecClosing", LongType(), False),
                             StructField("diagnostics", StringType(), False),
                             StructField("recordSequenceNumber", LongType(), False),
                             StructField("nodeID", StringType(), False),
                             StructField("recordExtensionsKey", StringType(), False),
                             StructField("localSequenceNumber", LongType(), False),
                             StructField("apnSelectionMode", StringType(), False),
                             StructField("servedMSISDN", StringType(), False),
                             StructField("chargingCharacteristics", StringType(), False),
                             StructField("chChSelectionMode", StringType(), False),
                             StructField("servingNodePLMNIdentifier", StringType(), False),
                             StructField("iMSsignalingContext", StringType(), False),
                             StructField("servedIMEISV", StringType(), False),
                             StructField("rATType", LongType(), False),
                             StructField("mSTimeZone", StringType(), False),
                             StructField("userLocationInformation", StringType(), False),
                             StructField("sGWChange", StringType(), False),
                             StructField("servingNodeType", StringType(), False),
                             StructField("p-GWAddressUsed", StringType(), False),
                             StructField("p-GWPLMNIdentifier", StringType(), False),
                             StructField("startTime", TimestampType(), False),
                             StructField("stopTime", TimestampType(), False),
                             StructField("pDNConnectionID", IntegerType(), False),
                             StructField("userCSGInformation", StringType(), False),
                             StructField("servedPDPAddressExt", StringType(), False),
                             StructField("dynamicAddressFlagExt", StringType(), False),
                             StructField("s-GWiPv6Address", StringType(), False),
                             StructField("servingNodeiPv6Address", StringType(), False),
                             StructField("p-GWiPv6AddressUsed", StringType(), False),
                             StructField("lowAccessPriorityIndicator", StringType(), False)
                             ])

    return sgw_schema

def get_record_extension_schema():

    record_extension_schema = StructType([ StructField("file_id", StringType(), False),
                                           StructField("recordExtensionsKey", StringType(), False),
                                           StructField("identifier", StringType(), False),
                                           StructField("information", StringType(), False),
                                           StructField("significance", StringType(), False)])

    return record_extension_schema



def get_affirm_stats_schema():

    # Custom schema to read input
    affirm_stats_schema = StructType([
                            StructField("iteration_id", StringType(), False),
                            StructField("file_id", StringType(), False),
                            StructField("file_name", StringType(), False),
                            StructField("checksum", StringType(), False),
                            StructField("total_count", LongType(), False),
                            StructField("success_count", LongType(), False),
                            StructField("fail_count", LongType(), False),
                            StructField("pgw_total_count", LongType(), False),
                            StructField("pgw_success_count", LongType(), False),
                            StructField("pgw_failed_count", LongType(), False),
                            StructField("pgw_lsd_count",LongType,False),
                            StructField("pdp_rec_ext_count", LongType(), False),
                            StructField("sgw_total_count", LongType(), False),
                            StructField("sgw_success_count", LongType(), False),
                            StructField("sgw_failed_count", LongType(), False),
                            StructField("file_created_timestamp", LongType(), False),
                            StructField("year", IntegerType(), False),
                            StructField("month", IntegerType(), False),
                            StructField("day", IntegerType(), False)

                         ])
    return affirm_stats_schema









