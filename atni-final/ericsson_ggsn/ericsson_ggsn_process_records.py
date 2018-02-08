import sys

import binascii
import socket
import struct
from ericsson_ggsn_asn_gen import *
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType, TimestampType, ShortType

from atni.parsers.constants import *
from atni.parsers.utilities import *

import StringIO
#from ericsson_asn_gen import CallEventRecord
from pyasn1.codec.ber import decoder
from pyasn1 import error
import logging, sys, os
import time
from ericsson_ggsn_schema import *
from ericsson_ggsn_parsed import EricssonggsnParsedRecord
from ericsson_ggsn_stats import EricssonStats
import uuid
import datetime
from datetime import date








def process_ggsn_records(ggsn_record, file_id,file_created_timestamp):


    #print "starting of process ggsn"

    #servingnodeaddress = (ggsn_record.getComponentByName("sgsnAddress"))
    # print servingnodeaddress
    sgsn_address_tmp = ggsn_record.getComponentByName("sgsnAddress")
    for idx in range(len(sgsn_address_tmp)):
        sgsn_ip_binary_address = sgsn_address_tmp.getComponentByPosition(idx)
        sgsnaddress_iPBinaryAddress = sgsn_ip_binary_address.getComponentByName("iPBinaryAddress")
        sgsnaddress_iPBinV4Address = sgsnaddress_iPBinaryAddress.getComponentByName("iPBinV4Address")
        sgsn_address = socket.inet_ntoa(
            binascii.unhexlify(binascii.hexlify(str(sgsnaddress_iPBinV4Address))))


    recordtype = RecordType((str(ggsn_record.getComponentByName("recordType")))).prettyPrint()
    served_imsi = get_imei_and_imsi(ggsn_record, "servedIMSI")
    ggsn_address_used_obj = ggsn_record.getComponentByName("ggsnAddress")
    ggsn_ip_binary_address = ggsn_address_used_obj.getComponentByName("iPBinaryAddress")
    ggsn_address_used = socket.inet_ntoa(
        binascii.unhexlify(binascii.hexlify(str(ggsn_ip_binary_address.getComponent()))))

    charging_id = long(str(ggsn_record.getComponentByName("chargingID")))


    #AccessPointNameNI = ggsn_record.getComponentByName("accessPointNameNI")
    access_point_name_ni = AccessPointNameNI(ggsn_record.getComponentByName("accessPointNameNI")).prettyPrint()
    pdp_type = binascii.hexlify(str(ggsn_record.getComponentByName("pdpType"))).replace("f", "")
    #################servedPDPAddress start#################"
    pdp_address = ggsn_record.getComponentByName("servedPDPAddress")
    ip_address = pdp_address.getComponentByName("iPAddress")
    ip_binary_address = ip_address.getComponentByName("iPBinaryAddress")
    served_pdp_address = socket.inet_ntoa(
        binascii.unhexlify(binascii.hexlify(str(ip_binary_address.getComponent()))))
    #################servedPDPAddress end#################"


        #################recordOpeningTime start#################
    record_opening_time_tmp = binascii.hexlify(str(ggsn_record.getComponentByName("recordOpeningTime")))
    record_opening_time = append_timezone_offset(parseTimestamp(record_opening_time_tmp))

    #################recordOpeningTime end#################"

    duration = 0
    duration_tmp = ggsn_record.getComponentByName("duration")
    if duration_tmp is not None:
        duration = long(str(duration_tmp))

    cause_for_rec_closing = long(str(ggsn_record.getComponentByName("causeForRecClosing")))

    record_sequence_num_tmp = ggsn_record.getComponentByName("recordSequenceNumber")
    if record_sequence_num_tmp is not None:
        record_sequence_number = long(record_sequence_num_tmp)
    else:
        record_sequence_number = -255

    node_id = str(ggsn_record.getComponentByName("nodeID"))



   # local_sequence_number_tmp = ggsn_record.getComponentByName("localSequenceNumber")
    #if local_sequence_number_tmp is not None:
     #   local_sequence_number = long(str(local_sequence_number_tmp))
    #else:
     #   local_sequence_number = "no values"
    #local_sequence_number = str(ggsn_record.getComponentByName("localSequenceNumber"))

    local_sequence_number = str((ggsn_record.getComponentByName("localSequenceNumber")))

    apn_selection_mode_tmp = APNSelectionMode(ggsn_record.getComponentByName("apnSelectionMode"))
    apn_selection_mode = ""
    if apn_selection_mode_tmp == 0:
        apn_selection_mode = "mSorNetworkProvidedSubscriptionVerified"
    elif apn_selection_mode_tmp == 1:
        apn_selection_mode = "mSProvidedSubscriptionNotVerified"
    elif apn_selection_mode == 2:
        apn_selection_mode = "networkProvidedSubscriptionNotVerified"

    served_msisdn = get_imei_and_imsi(ggsn_record, "servedMSISDN")
    charging_characteristics = binascii.hexlify(str(ggsn_record.getComponentByName("chargingCharacteristics")))
    ch_ch_selection_mode_tmp = ChChSelectionMode(ggsn_record.getComponentByName("chChSelectionMode"))
    ch_ch_selection_mode = get_ch_ch_selection_mode(ch_ch_selection_mode_tmp)

    iMSsignalingContext = ""
    iMSsignalingContext_tmp = ggsn_record.getComponentByName("iMSsignalingContext")
    if iMSsignalingContext_tmp is not None:
        iMSsignalingContext = binascii.hexlify(str(iMSsignalingContext_tmp))

    sgsnPLMNIdentifierr = get_plmn_id(binascii.hexlify(str(ggsn_record.getComponentByName("sgsnPLMNIdentifier"))))

    served_imei = binascii.hexlify(binascii.hexlify(str((ggsn_record.getComponentByName("servedIMEISV")))))
    #served_imei = (binascii.hexlify(str((ggsn_record.getComponentByName("servedIMEISV")))))

    rat_type = 0
    rat_type_tmp = ggsn_record.getComponentByName("rATType")
    if rat_type_tmp is not None:
        rat_type = int(str(rat_type_tmp))

    ms_time_zone = ""
    ms_time_zone_tmp = ggsn_record.getComponentByName("mSTimeZone")
    if ms_time_zone_tmp is not None:
     ms_time_zone = binascii.hexlify(str(ms_time_zone_tmp))
    else :
        ms_time_zone= str(ms_time_zone_tmp)


    userLocationInformation1 = binascii.hexlify(str(ggsn_record.getComponentByName("userLocationInformation")))


   # userLocationInformation = format(
       # int(binascii.hexlify(str(ggsn_record.getComponentByName("userLocationInformation"))), 16), '05')

    #userLocationInformation1 = int(binascii.hexlify(str(ggsn_record.getComponentByName("userLocationInformation"))))
    #servingNodeType = ggsn_record.getComponentByName("servingNodeType")


    #servedPDPPDNAddressExt = binascii.hexlify(str(ggsn_record.getComponentByName("servedPDPPDNAddressExt")))

    dynamic_address_flag = 0
    dynamic_address_flag_tmp = ggsn_record.getComponentByName("dynamicAddressFlag")
    if dynamic_address_flag_tmp is not None:
        dynamic_address_flag = long(str(dynamic_address_flag_tmp))

    list_of_traffic_volumes = ggsn_record.getComponentByName("listOfTrafficVolumes")

    #key2 = str(record_opening_time_tmp) + "_" + str(local_sequence_number)
    traffic_volumes_key2 = str(uuid.uuid4())
    #print traffic_volumes_key
    ggsn_cdr_traffic_volumes_array = []
    for idx in range(len(list_of_traffic_volumes)):

        change_of_char_cond = list_of_traffic_volumes.getComponentByPosition(idx)
        #qos_requested = binascii.hexlify(str(change_of_char_cond.getComponentByName("qosRequested")))
        #qos_negotiated = binascii.hexlify(str(change_of_char_cond.getComponentByName("qosNegotiated")))
        qos_negotiated = ""
        qos_negotiated_tmp = change_of_char_cond.getComponentByName("qosNegotiated")
        if qos_negotiated_tmp is not None:
            qos_negotiated = binascii.hexlify(str(qos_negotiated_tmp))


        data_volume_gprs_uplink = 0
        data_volume_gprs_uplink_tmp = binascii.hexlify(
                str(change_of_char_cond.getComponentByName("dataVolumeGPRSUplink")))
        if data_volume_gprs_uplink_tmp is not None:
                data_volume_gprs_uplink = long(str(data_volume_gprs_uplink_tmp))

        data_volume_gprs_downlink = 0
        data_volume_gprs_downlink_tmp = binascii.hexlify(
                str(change_of_char_cond.getComponentByName("dataVolumeGPRSDownlink")))
        if data_volume_gprs_downlink_tmp is not None:
                data_volume_gprs_downlink = long(str(data_volume_gprs_downlink_tmp))

        change_condition = 0
        change_condition_tmp = binascii.hexlify(str(change_of_char_cond.getComponentByName("changeCondition")))
        if change_condition_tmp is not None:
                change_condition = int(str(change_condition_tmp))


        change_time = parseTimestamp(
                str(binascii.hexlify(str(change_of_char_cond.getComponentByName("changeTime")))))

        userLocationInformation = (binascii.hexlify(str(change_of_char_cond.getComponentByName("userLocationInformation"))))

            #ePCQoSInformation = (
                #(str(change_of_char_cond.getComponentByName("ePCQoSInformation"))))

        ePCQoSInformation= ""
        ePCQoSInformation_tmp = change_of_char_cond.getComponentByName("ePCQoSInformation")
        if ePCQoSInformation_tmp is not None:
                ePCQoSInformation =  binascii.hexlify((str(change_of_char_cond.getComponentByName("ePCQoSInformation"))))

        year = int(file_created_timestamp[0:-4])
        month = int(file_created_timestamp[4:-2])
        day = int(file_created_timestamp[6:])

        traffic_volume_item = [file_id, traffic_volumes_key2, change_condition, change_time,
                               data_volume_gprs_downlink, data_volume_gprs_uplink,
                               qos_negotiated,userLocationInformation,ePCQoSInformation,long(file_created_timestamp),year,month,day
                              ]
        #print traffic_volume_item

        ggsn_cdr_traffic_volumes_array.append(traffic_volume_item)

            #print "traffic_volume_arry is"
            #print ggsn_cdr_traffic_volumes_array

        #################listOfTrafficVolumes end#################"

    record_extension_key = str(uuid.uuid4())
    record_extension = ggsn_record.getComponentByName("recordExtensions")
    identifier = ""
    ts48018BssgpCause = ""
    ts25413RanapCause = ""
    significance = ""
    ggsn_cdr_rec_ext_array = []
    if record_extension is not None:
        management_extensions = record_extension.getComponentByName("ManagementExtensions")
        for idx in range(len(management_extensions)):
            management_extension = management_extensions.getComponentByPosition(0)
            identifier = management_extension.gettComponentByName("identifier")
            significance = management_extension.gettComponentByName("significance")
            gprsSCdrExtensions = management_extension.gettComponentByName("information")

            if gprsSCdrExtensions is not None:
                extendedDiagnostics = gprsSCdrExtensions.getComponentByName("extendedDiagnostics")

                if extendedDiagnostics is not None:
                    ts48018BssgpCause = extendedDiagnostics.getComponentByName("ts48018BssgpCause")
                    ts25413RanapCause = extendedDiagnostics.getComponentByName("ts25413RanapCause")

            rec_extensions_item = [file_id, record_extension_key, str(identifier), str(ts48018BssgpCause),
                                   str(ts25413RanapCause), str(significance),long(file_created_timestamp),year,month,day]
            ggsn_cdr_rec_ext_array.append(rec_extensions_item)

    # print "recordtypeis"
    # print recordtype
    # print"servedimsi is"
    # print served_imsi
    # print "ggsn address is"
    # print ggsn_address_used
    # print"charging_id is"
    # print charging_id
    # print "accesspointNI is"
    # print access_point_name_ni
    # print"pdp type is"
    # print pdp_type
    # print"served pdp address is"
    # print served_pdp_address
    # print"dynamic address flag is"
    # print dynamic_address_flag
    # print"record opening time is"
    # print record_opening_time
    # print "duration is"
    # print duration
    # print"cause_for_reclosing is"
    # print cause_for_rec_closing
    # print "record sequence no. is"
    # print record_sequence_number
    # print"node_id is"
    # print node_id
    # print "record extension is"
    # print record_extension
    # print "localsequencenumber is"
    # print local_sequence_number
    # print "apn_selection_mode is"
    # print apn_selection_mode
    # print"served_msisdn is"
    # print served_msisdn
    # print"charging_charter is"
    # print charging_characteristics
    # print"chselection mode is"
    # print ch_ch_selection_mode
    # print"imssignalingcontext"
    # print iMSsignalingContext
    # print "sgsnPLMNIdentifierr is"
    # print sgsnPLMNIdentifierr
    # print"servedimei is"
    # print served_imei
    # print"ratype is"
    # print rat_type
    # print "mstimezone is"
    # print ms_time_zone
    # print"userLocationInformation"
    # print userLocationInformation1
    # print "servingnodeaddress"
    # print servingnodeaddress


    ggsn_record_item = [file_id, recordtype,served_imsi, ggsn_address_used,charging_id,sgsn_address,access_point_name_ni,pdp_type,served_pdp_address,dynamic_address_flag,traffic_volumes_key2,
                        (record_opening_time),duration,cause_for_rec_closing,record_sequence_number,node_id,record_extension_key,local_sequence_number,
                        apn_selection_mode,served_msisdn,charging_characteristics,ch_ch_selection_mode,iMSsignalingContext,sgsnPLMNIdentifierr,
                        served_imei,rat_type,ms_time_zone,userLocationInformation1,long(file_created_timestamp),year,month,day]


    #print ggsn_record_item

    return ggsn_record_item,ggsn_cdr_traffic_volumes_array,ggsn_cdr_rec_ext_array

def process_egsn_records(egsn_record, file_id,file_created_timestamp):
    #print "starting of process egsn"



    recordtype = RecordType((str(egsn_record.getComponentByName("recordType")))).prettyPrint()
    served_imsi = get_imei_and_imsi(egsn_record, "servedIMSI")
    ggsn_address_used_obj = egsn_record.getComponentByName("ggsnAddress")
    ggsn_ip_binary_address = ggsn_address_used_obj.getComponentByName("iPBinaryAddress")
    ggsn_address_used = socket.inet_ntoa(
        binascii.unhexlify(binascii.hexlify(str(ggsn_ip_binary_address.getComponent()))))

    charging_id = long(str(egsn_record.getComponentByName("chargingID")))

    # AccessPointNameNI = ggsn_record.getComponentByName("accessPointNameNI")
    access_point_name_ni = AccessPointNameNI(egsn_record.getComponentByName("accessPointNameNI")).prettyPrint()
    pdp_type = binascii.hexlify(str(egsn_record.getComponentByName("pdpType"))).replace("f", "")

    local_sequence_number = str((egsn_record.getComponentByName("localSequenceNumber")))

    record_sequence_num_tmp = egsn_record.getComponentByName("recordSequenceNumber")
    if record_sequence_num_tmp is not None:
        record_sequence_number = long(record_sequence_num_tmp)
    else:
        record_sequence_number = -255

        #################servedPDPAddress start#################"
    pdp_address = egsn_record.getComponentByName("servedPDPAddress")
    ip_address = pdp_address.getComponentByName("iPAddress")
    ip_binary_address = ip_address.getComponentByName("iPBinaryAddress")
    served_pdp_address = socket.inet_ntoa(
        binascii.unhexlify(binascii.hexlify(str(ip_binary_address.getComponent()))))

        #################servedPDPAddress end#################"

        #################recordOpeningTime start#################
    record_opening_time_tmp = binascii.hexlify(str(egsn_record.getComponentByName("recordOpeningTime")))
    record_opening_time = append_timezone_offset(parseTimestamp(record_opening_time_tmp))

        #################recordOpeningTime end#################"

    duration = 0
    duration_tmp = egsn_record.getComponentByName("duration")
    if duration_tmp is not None:
        duration = long(str(duration_tmp))

    cause_for_rec_closing = long(str(egsn_record.getComponentByName("causeForRecClosing")))


    node_id = str(egsn_record.getComponentByName("nodeID"))

    dynamic_address_flag = 0
    dynamic_address_flag_tmp = egsn_record.getComponentByName("dynamicAddressFlag")
    if dynamic_address_flag_tmp is not None:
        dynamic_address_flag = long(str(dynamic_address_flag_tmp))




     #record_extension = egsn_record.getComponentByName("recordExtensions")

    pSFurnishChargingInformation = (egsn_record.getComponentByName("pSFurnishChargingInformation"))

    apn_selection_mode_tmp = APNSelectionMode(egsn_record.getComponentByName("apnSelectionMode"))
    apn_selection_mode = ""
    if apn_selection_mode_tmp == 0:
        apn_selection_mode = "mSorNetworkProvidedSubscriptionVerified"
    elif apn_selection_mode_tmp == 1:
        apn_selection_mode = "mSProvidedSubscriptionNotVerified"
    elif apn_selection_mode == 2:
        apn_selection_mode = "networkProvidedSubscriptionNotVerified"

    served_msisdn = get_imei_and_imsi(egsn_record, "servedMSISDN")
    charging_characteristics = binascii.hexlify(str(egsn_record.getComponentByName("chargingCharacteristics")))
    ch_ch_selection_mode_tmp = ChChSelectionMode(egsn_record.getComponentByName("chChSelectionMode"))
    ch_ch_selection_mode = get_ch_ch_selection_mode(ch_ch_selection_mode_tmp)

    iMSsignalingContext = ""
    iMSsignalingContext_tmp = egsn_record.getComponentByName("iMSsignalingContext")
    if iMSsignalingContext_tmp is not None:
        iMSsignalingContext = binascii.hexlify(str(iMSsignalingContext_tmp))

    sgsnPLMNIdentifierr = get_plmn_id(binascii.hexlify(str(egsn_record.getComponentByName("sgsnPLMNIdentifier"))))

    served_imeisv = binascii.hexlify(binascii.hexlify(str((egsn_record.getComponentByName("servedIMEISV")))))
    # served_imei = (binascii.hexlify(str((ggsn_record.getComponentByName("servedIMEISV")))))

    rat_type = 0
    rat_type_tmp = egsn_record.getComponentByName("rATType")
    if rat_type_tmp is not None:
        rat_type = int(str(rat_type_tmp))

    ms_time_zone = ""
    ms_time_zone_tmp = egsn_record.getComponentByName("mSTimeZone")
    if ms_time_zone_tmp is not None:
        ms_time_zone = binascii.hexlify(str(ms_time_zone_tmp))
    else:
        ms_time_zone =str(ms_time_zone_tmp)

    sgsn_address_tmp = egsn_record.getComponentByName("sgsnAddress")
    for idx in range(len(sgsn_address_tmp)):
        sgsn_ip_binary_address = sgsn_address_tmp.getComponentByPosition(idx)
        sgsnaddress_iPBinaryAddress = sgsn_ip_binary_address.getComponentByName("iPBinaryAddress")
        sgsnaddress_iPBinV4Address = sgsnaddress_iPBinaryAddress.getComponentByName("iPBinV4Address")
        sgsn_address = socket.inet_ntoa(
            binascii.unhexlify(binascii.hexlify(str(sgsnaddress_iPBinV4Address))))




    userLocationInformation2 = binascii.hexlify(str(egsn_record.getComponentByName("userLocationInformation")))



    list_of_traffic_volumes = egsn_record.getComponentByName("listOfTrafficVolumes")

    #key1 = str(record_opening_time_tmp) + "_" + str(local_sequence_number)
    traffic_volumes_key1 = str(uuid.uuid4())
    # print"keyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"
    # print traffic_volumes_key
    egsn_cdr_traffic_volumes_array = []
    for idx in range(len(list_of_traffic_volumes)):
        change_of_char_cond = list_of_traffic_volumes.getComponentByPosition(idx)
        # qos_requested = binascii.hexlify(str(change_of_char_cond.getComponentByName("qosRequested")))
        # qos_negotiated = binascii.hexlify(str(change_of_char_cond.getComponentByName("qosNegotiated")))
        qos_negotiated = ""
        qos_negotiated_tmp = change_of_char_cond.getComponentByName("qosNegotiated")
        if qos_negotiated_tmp is not None:
            qos_negotiated = binascii.hexlify(str(qos_negotiated_tmp))

        data_volume_gprs_uplink = 0
        data_volume_gprs_uplink_tmp = binascii.hexlify(
            str(change_of_char_cond.getComponentByName("dataVolumeGPRSUplink")))
        if data_volume_gprs_uplink_tmp is not None:
            data_volume_gprs_uplink = long(str(data_volume_gprs_uplink_tmp))

        data_volume_gprs_downlink = 0
        data_volume_gprs_downlink_tmp = binascii.hexlify(
            str(change_of_char_cond.getComponentByName("dataVolumeGPRSDownlink")))
        if data_volume_gprs_downlink_tmp is not None:
            data_volume_gprs_downlink = long(str(data_volume_gprs_downlink_tmp))

        change_condition = 0
        change_condition_tmp = binascii.hexlify(str(change_of_char_cond.getComponentByName("changeCondition")))
        if change_condition_tmp is not None:
            change_condition = int(str(change_condition_tmp))

        change_time = parseTimestamp(
            str(binascii.hexlify(str(change_of_char_cond.getComponentByName("changeTime")))))

        userLocationInformation = (
            binascii.hexlify(str(change_of_char_cond.getComponentByName("userLocationInformation"))))

        # ePCQoSInformation = (
        # (str(change_of_char_cond.getComponentByName("ePCQoSInformation"))))


        ePCQoSInformation = ""
        ePCQoSInformation_tmp = change_of_char_cond.getComponentByName("ePCQoSInformation")
        if ePCQoSInformation_tmp is not None:
            ePCQoSInformation = binascii.hexlify((str(change_of_char_cond.getComponentByName("ePCQoSInformation"))))

        year = int(file_created_timestamp[0:-4])
        month = int(file_created_timestamp[4:-2])
        day = int(file_created_timestamp[6:])

        traffic_volume_item = [file_id, traffic_volumes_key1,change_condition,change_time,
                               data_volume_gprs_downlink, data_volume_gprs_uplink,
                               qos_negotiated, userLocationInformation, ePCQoSInformation,long(file_created_timestamp),year,month,day]
        egsn_cdr_traffic_volumes_array.append(traffic_volume_item)


        ##############################list of service data start ########################################################"

    listofservicedata = egsn_record.getComponentByName("listOfServiceData")

    listofservicedata_key = str(uuid.uuid4())
    egsn_cdr_service_data_array = []
    if listofservicedata is not None:
            for idx in range(len(listofservicedata)):
                change_of_service_cond = listofservicedata.getComponentByPosition(idx)
                ratinggroup = (binascii.hexlify(str(change_of_service_cond.getComponentByName("ratingGroup"))))

                #print"ratiinggroup....................."
                #print ratinggroup
                resultCode = " "
                resultCode_tmp = change_of_service_cond.getComponentByName("resultCode")

                if resultCode_tmp is not None:
                    resultCode = binascii.hexlify(str(resultCode_tmp))

                #print "resultcode..............."
                #print resultCode
                integer = 0
                local_Sequence_Number = (str(change_of_service_cond.getComponentByName("localSequenceNumber")))
                #print"localsequenceno.lsit of ser"
                #print local_sequence_number
                timeOfFirstUsage_tmp = binascii.hexlify(
                    str(change_of_service_cond.getComponentByName("timeOfFirstUsage")))
                #print"timeoffirst"

                timeOfFirstUsage = append_timezone_offset(parseTimestamp(timeOfFirstUsage_tmp))
                #print timeOfFirstUsage
                timeOfLastUsage_tmp = binascii.hexlify(
                    str(change_of_service_cond.getComponentByName("timeOfLastUsage")))
                #print"tiemlast usegae"
                timeOfLastUsage = append_timezone_offset(parseTimestamp(timeOfLastUsage_tmp))
                #print timeOfFirstUsage

                timeUsage = str(change_of_service_cond.getComponentByName("timeUsage"))
                #print "time usgae"
                #print timeUsage
                ######################## service condition change starts ################################
                serviceConditionChange_temp = change_of_service_cond.getComponentByName("serviceConditionChange")


                serviceConditionChange = serviceConditionChange_temp
                if serviceConditionChange == 0:
                    serviceConditionChange = "qoSChange"
                elif serviceConditionChange == 1:
                    serviceConditionChange = "sGSNChange"
                elif serviceConditionChange == 2:
                    serviceConditionChange = "sGSNPLMNIDChange"
                elif serviceConditionChange == 3:
                    serviceConditionChange = "tariffTimeSwitch"
                elif serviceConditionChange == 4:
                    serviceConditionChange = "pDPContextRelease"
                elif serviceConditionChange == 5:
                    serviceConditionChange = "rATChange"
                elif serviceConditionChange == 6:
                    serviceConditionChange = "serviceIdledOut"
                elif serviceConditionChange == 7:
                    serviceConditionChange = "reserved1"
                elif serviceConditionChange == 8:
                    serviceConditionChange = "configurationChange"
                elif serviceConditionChange == 9:
                    serviceConditionChange = "serviceStop"
                elif serviceConditionChange == 10:
                    serviceConditionChange = "dCCATimeThresholdReached"
                elif serviceConditionChange == 11:
                    serviceConditionChange = "dCCAVolumeThresholdReached"
                elif serviceConditionChange == 12:
                    serviceConditionChange = "dCCAServiceSpecificUnitThresholdReached"
                elif serviceConditionChange == 13:
                    serviceConditionChange = "dCCATimeExhausted"
                elif serviceConditionChange == 14:
                    serviceConditionChange = "dCCAVolumeExhausted"
                elif serviceConditionChange == 15:
                    serviceConditionChange = "dCCAValidityTimeout"
                elif serviceConditionChange == 16:
                    serviceConditionChange = "reserved2"
                elif serviceConditionChange == 17:
                    serviceConditionChange = "dCCAReauthorisationRequest"
                elif serviceConditionChange == 18:
                    serviceConditionChange = "dCCAContinueOngoingSession"
                elif serviceConditionChange == 19:
                    serviceConditionChange = "dCCARetryAndTerminateOngoingSession"
                elif serviceConditionChange == 20:
                    serviceConditionChange = "dCCATerminateOngoingSession"
                elif serviceConditionChange == 21:
                    serviceConditionChange = "cGI-SAIChange"
                elif serviceConditionChange == 22:
                    serviceConditionChange = "rAIChange"
                elif serviceConditionChange == 23:
                    serviceConditionChange = "dCCAServiceSpecificUnitExhausted"
                elif serviceConditionChange == 24:
                    serviceConditionChange = "recordClosure"
                elif serviceConditionChange == 25:
                    serviceConditionChange = "timeLimit"
                elif serviceConditionChange == 26:
                    serviceConditionChange = "volumeLimit"
                elif serviceConditionChange == 27:
                    serviceConditionChange = "serviceSpecificUnitLimit"
                elif serviceConditionChange == 28:
                    serviceConditionChange = "envelopeClosure"
                elif serviceConditionChange == 29:
                    serviceConditionChange = "eCGIChange"
                elif serviceConditionChange == 30:
                    serviceConditionChange = "tAIChange"
                elif serviceConditionChange == 31:
                    serviceConditionChange = "userLocationChange"

                #print "service condition change"
                #print serviceConditionChange
                #print "serviceconditmp"
                #print serviceConditionChange_temp

                qoSInformationNeg = "none"
                #print"qoatinfoneg"
                #print qoSInformationNeg

                # TODO
                #servingNodeAddress = binascii.hexlify(str(change_of_service_cond.getComponentByName("sgsn-Address")))
                #print "sgsnaddressin lsit"
                #servingNodeAddress="missing"
                sgsn_address_used_obj = change_of_service_cond.getComponentByName("sgsn-Address")
                sgsn_ip_binary_address = sgsn_address_used_obj.getComponentByName("iPBinaryAddress")
                sgsn_address_used = socket.inet_ntoa(
                    binascii.unhexlify(binascii.hexlify(str(ggsn_ip_binary_address.getComponent()))))
                #print "sgsn addresss is now"
                #print ggsn_address_used
                datavolumeFBCUplink = 0
                datavolumeFBCUplink_temp = binascii.hexlify(
                    str(change_of_service_cond.getComponentByName("datavolumeFBCUplink")))
                if datavolumeFBCUplink_temp is not None:
                    datavolumeFBCUplink = (str(datavolumeFBCUplink_temp))
                    #print"datavolfcbuplink"
                    #print datavolumeFBCUplink
                datavolumeFBCDownlink = 0
                datavolumeFBCDownlink_temp = binascii.hexlify(
                    str(change_of_service_cond.getComponentByName("datavolumeFBCDownlink")))
                if datavolumeFBCDownlink_temp is not None:
                    datavolumeFBCDownlink = (str(datavolumeFBCDownlink_temp))
                    #print "datavolumedown link"
                    #print datavolumeFBCDownlink
                # TODO
                # timeOfReport_tmp = binascii.hexlify(str(change_of_service_cond.getComponentByName("timeOfReport")))
                # timeOfReport = append_timezone_offset(parseTimestamp(timeOfReport_tmp))
                # # timeOfReport = parseTimestamp(
                # str(binascii.hexlify(str(change_of_service_cond.getComponentByName("timeOfReport")))))
                failureHandlingContinue = 0
                failureHandlingContinue_tmp = change_of_service_cond.getComponentByName("failureHandlingContinue")
                if failureHandlingContinue_tmp is not None:
                    failureHandlingContinue = (str(failureHandlingContinue_tmp))
                    #print "failurehandling"
                    #print failureHandlingContinue
                serviceIdentifier = int(
                    binascii.hexlify(str(change_of_service_cond.getComponentByName("serviceIdentifier"))))
                #print "service identifier"
                #print serviceIdentifier
                pSFurnishChargingInformation = binascii.hexlify(
                    binascii.hexlify(str(change_of_service_cond.getComponentByName("pSFurnishChargingInformation"))))
                #print "psfurnishgcharing"
                #print pSFurnishChargingInformation
                userLocationInformation = binascii.hexlify(
                    str(change_of_service_cond.getComponentByName("userLocationInformation")))
                #print"userlocation information"
                #print userLocationInformation
                aFRecordInformation = (str(change_of_service_cond.getComponentByName("aFRecordInformation")))
                #print "affffffffffffff"
                #print aFRecordInformation

                eventBasedChargingInformation = (
                    str(change_of_service_cond.getComponentByName("eventBasedChargingInformation")))
                #print"eventbasedcharinginformation"
                #print eventBasedChargingInformation
                sGSNPLMNIdentifier = binascii.hexlify(
                    str(change_of_service_cond.getComponentByName("sGSNPLMNIdentifier")))
                #print"sgsnlmplinti"
                #print sGSNPLMNIdentifier
                rat_type_tmp = change_of_service_cond.getComponentByName("rATType")
                if rat_type_tmp is not None:
                    rat_type = int(str(rat_type_tmp))
                #print"raaaaaaaaaat"
                #print rat_type

                timeOfReport_tmp = binascii.hexlify(str(change_of_service_cond.getComponentByName("timeOfReport")))
                timeOfReport = append_timezone_offset(parseTimestamp(timeOfReport_tmp))
                #print"timeofreport"
                #print timeOfReport



                listOfServiceData_item = [file_id, listofservicedata_key,ratinggroup, resultCode,
                                          local_Sequence_Number,
                                          timeOfFirstUsage, timeOfLastUsage, timeUsage,
                                          sgsn_address_used, datavolumeFBCUplink, datavolumeFBCDownlink,
                                          timeOfReport, failureHandlingContinue, serviceIdentifier,
                                          pSFurnishChargingInformation,aFRecordInformation,
                                          userLocationInformation,eventBasedChargingInformation,sGSNPLMNIdentifier,
                                          rat_type,qoSInformationNeg,long(file_created_timestamp),year,month,day]

                egsn_cdr_service_data_array.append(listOfServiceData_item)



        ###############################list of service data ends ###########################"
    record_extension_key = str(uuid.uuid4())
    management_extensions = egsn_record.getComponentByName("recordExtensions")
    identifier = ""
    significance = ""
    egsn_cdr_rec_ext_array = []

    if management_extensions is not None:
        # management_extensions = record_extension.getComponentByName("ManagementExtensions")
        for idx in range(len(management_extensions)):
            management_extension = management_extensions.getComponentByPosition(0)
            identifier = management_extension.getComponentByName("identifier")
            significance = management_extension.getComponentByName("significance")
            gprsSCdrExtensions = management_extension.getComponentByName("information")
            rec_extensions_item1 = [file_id, record_extension_key,str(identifier),str(significance),long(file_created_timestamp),year,month,day]

            egsn_cdr_rec_ext_array.append(rec_extensions_item1)


    #print "list of service data"
    #print egsn_cdr_service_data_array
    #print "recordtype for egsn...."
    #print recordtype
    #print"servedimsi for egsn"
    #print served_imsi
    #print"ggsn_address for egsn"
    #print ggsn_address_used
    #print"chargingid for egsn"
    #print charging_id
    #print"accesss_pointname for egsn"
    #print access_point_name_ni
    #print"pdptype for egsn"
    #print pdp_type
    #print "localsequence no. egsn"
    #print local_sequence_number
    #print"recordsequence no. egsn"
    #print record_sequence_number
    #print"served pdp address egsn"
    #print served_pdp_address
    #print"record opening time egsn"
    #print record_opening_time
    #print "duration egsn"
    #print duration
    #print "cause for rec closing egsn"
    #print cause_for_rec_closing
    #print"node id egsn"
    #print node_id
    #print"duration egsn"
    #print duration
    #print"traffic key egsn"
    #print traffic_volumes_key1
    #print "recordextension egsn"
    #print record_extension
    #print"furnishing information"
    #print pSFurnishChargingInformation
    #print "apnselection mode egsn"
    #print apn_selection_mode
    #print "serve misn egsn"
    #print served_msisdn
    #print "charging charteristics egsn"
    #print charging_characteristics
    #print "chselection mode egsn"
    #print ch_ch_selection_mode
    #print "imsignal context egsn"
    #print iMSsignalingContext
    #print "sgsn plmidentifier egsn"
    #print sgsnPLMNIdentifierr
    #print "servedimeisv egsn"
    #print served_imeisv
    #print "rattype egsn"
    #print rat_type
    #print "mstimezone egsn"
    #print ms_time_zone
    #print"userlocation information egsn"
    #print userLocationInformation2


    egsn_record_item = [file_id, recordtype,served_imsi, ggsn_address_used,charging_id,sgsn_address,access_point_name_ni,pdp_type,served_pdp_address,dynamic_address_flag,traffic_volumes_key1,
                        record_opening_time,duration,cause_for_rec_closing,record_sequence_number,node_id,record_extension_key,local_sequence_number,
                        apn_selection_mode,served_msisdn,charging_characteristics,ch_ch_selection_mode,iMSsignalingContext,sgsnPLMNIdentifierr,
                        served_imeisv,rat_type,ms_time_zone,userLocationInformation2,listofservicedata_key,long(file_created_timestamp),year,month,day]


    #print egsn_record_item


    return egsn_record_item,egsn_cdr_traffic_volumes_array,egsn_cdr_service_data_array,egsn_cdr_rec_ext_array


def file_date(hdfs_file_name):
  fileName=(os.path.splitext(os.path.splitext(os.path.basename(hdfs_file_name))[0])[0])
  temp=fileName.split("_")
  temp1=temp[2]
  filedate=temp1[0:-6]

  print filedate+"&&&&&&&&&&&&&&&&"

  return filedate



def parse_ericsson_file(hdfs_file_name, content, iteration_id):

    print "$$$$$$$$$ Inside parsing hdfs_file_name $$$$$$$$$$$$$$$$$"
    ericsson_file = StringIO.StringIO()
    ericsson_file.write(content)

    ericsson_file.seek(0)

    # Getting file name details to append to records
    _absoluteFileName = hdfs_file_name.rpartition(':')[2]
    file_name = _absoluteFileName.rpartition('/')[2]
    filePath = _absoluteFileName.rpartition('/')[0]
    file_created_timestamp = file_date(file_name)
    print file_name + "%%%%%%%%%%%%%%%%%%%"
    print file_created_timestamp + "$$$$$$$$$$$$$$"
    year = int(file_created_timestamp[0:-4])
    month = int(file_created_timestamp[4:-2])
    day = int(file_created_timestamp[6:])
    print "####### main path:" + hdfs_file_name

    check_sum = get_file_check_sum(str(hdfs_file_name))
    #check_sum = 123478987654321
    file_id = str(uuid.uuid4())
    chunk_size = ERICSSON_FILE_CHUNK_SIZE

    bytes_already_read = 0
    print bytes_already_read
    k = 0

    total_count = 0
    ggsn_total_count = 0
    ggsn_success_count = 0
    ggsn_failed_count = 0

    ggsn_pdp_traffic_count = 0
    ggsn_pdp_rec_ext_count = 0

    egsn_pdp_traffic_count = 0
    egsn_pdp_servicedata_count = 0
    egsn_pdp_rec_ext_count = 0

    egsn_base_total_count = 0
    egsn_base_success_count = 0
    egsn_base_failed_count = 0

    sgw_base_total_count = 0

    sys.tracebacklimit = 0
    start_time_for_iteration = time.time()

    file_size = len(content)
    # We read the entire file in smaller chunks. The chunk most likely will not end with completed record.
    # We initialize the bytes to read with a specific configured value from constants file
    bytes_to_read = chunk_size
    print "File size:" + str(file_size)
    cdr_parsed = []
    ggsn_array = []
    ggsn_cdr_pdp_traffic_volume_array = []
    ggsn_cdr_pdp_rec_extension_array = []
    egsn_pdp_array = []
    egsn_cdr_pdp_traffic_volume_array = []
    egsn_cdr_pdp_service_data_array = []
    egsn_cdr_pdp_rec_extension_array = []



    # With each iteration the bytes already read would be updated, and we need to iterate until the end of the file size.
    while bytes_already_read < file_size:


        data = ericsson_file.read(bytes_to_read)
        # ### print "Before reading:" + str(fh.tell()) + ", After reading:" + str(fh.tell()) + "-" + str(len(data))
        is_record_ended_good = True
        k += 1
        # The decoder decodes each record from the chunk until an exception is thrown due to last record
        # having insufficent(or lesser) length
        while data != "":
            try:
                ab, data = decoder.decode(data, asn1Spec=GPRSRecord())
                total_count +=1

                ggsn_record = ab.getComponentByName("ggsnPDPRecord")
                egsn_record = ab.getComponentByName("egsnPDPRecord")



                if ggsn_record is not None:


                    ggsn_total_count += 1
                    #print "file under ggsn process records is parsed"
                    ggsn_item,ggsn_traffic_volumes_array,ggsn_rec_ext_item = process_ggsn_records(ggsn_record, file_id,file_created_timestamp)

                    ggsn_array.append(ggsn_item)

                    if ggsn_traffic_volumes_array is not None and len(ggsn_traffic_volumes_array) > 0:
                        for idx in range(len(ggsn_traffic_volumes_array)):
                           ggsn_cdr_pdp_traffic_volume_array.append(ggsn_traffic_volumes_array[idx])

                    if ggsn_rec_ext_item is not None and len(ggsn_rec_ext_item) > 0:
                        for idx in range(len(ggsn_rec_ext_item)):
                         ggsn_cdr_pdp_rec_extension_array.append(ggsn_rec_ext_item[idx])

                elif egsn_record is not None:
                    egsn_base_total_count += 1

                    egsn_item,egsn_traffic_volumes_array,egsn_service_data_array,egsn_rec_ext_item = process_egsn_records(egsn_record, file_id,file_created_timestamp)
                    egsn_pdp_array.append(egsn_item)

                    if egsn_traffic_volumes_array is not None and len(egsn_traffic_volumes_array) > 0:
                        for idx in range(len(egsn_traffic_volumes_array)):
                           egsn_cdr_pdp_traffic_volume_array.append(egsn_traffic_volumes_array[idx])
                    if egsn_service_data_array is not None and len(egsn_service_data_array) > 0:
                        for idx in range(len(egsn_service_data_array)):
                           egsn_cdr_pdp_service_data_array.append(egsn_service_data_array[idx])

                    if egsn_rec_ext_item is not None and len(egsn_rec_ext_item) > 0:
                        for idx in range (len(egsn_rec_ext_item)):
                         egsn_cdr_pdp_rec_extension_array.append(egsn_rec_ext_item[idx])






            # When an exception occurs with SubstrateUnderrunError, it is due to smaller record size. So we need to
            # get the remaining bytes in the chunk and reposition the pointer so that the remaining bytes will be added
            # to the next chunk and the process repeats
            except error.SubstrateUnderrunError as e:
                is_record_ended_good = False
                # ###logging.exception('decode failure ######### :' + str(i))
                last_record_position = ericsson_file.tell()
                ericsson_file.seek(last_record_position - len(data))
                # print "SubstrateUnderrunError : data bytes left in this chunk:" + str(len(data)) + " , Current position in file :" \
                #       + str(last_record_position) + " , Seek for next chunk: " + str(ericsson_file.tell()) + \
                #       ", chunk number is:" + str(k)
                bytes_already_read += (bytes_to_read - len(data))
                bytes_to_read = chunk_size + len(data)
                break
            except error.PyAsn1Error as p:
                is_record_ended_good = False
                logging.exception(p.message)
                last_record_position = ericsson_file.tell()
                ericsson_file.seek(last_record_position - len(data))
                print "PyAsn1Error : data bytes left in this chunk:" + str(len(data)) + " , Current position in file :" \
                      + str(last_record_position) + " , Seek for next chunk: " + str(ericsson_file.tell()) + \
                      ", chunk number is:" + str(k)
                bytes_already_read += (bytes_to_read - len(data))
                bytes_to_read = chunk_size + len(data)
                break
            except Exception as ep:
                logging.exception(ep.message)

        # We also need to handle the scenario when our chunk is ending with an exact record. The len(data) would be
        # zero in that case
        if is_record_ended_good:
            last_record_position = ericsson_file.tell()
            ericsson_file.seek(last_record_position - len(data))
            # print "Surprise - chunk ended with exact record after : data bytes left in this chunk:" + str(len(data)) + " , Current position in file :" \
            #       + str(last_record_position) + " , Seek for next chunk: " + str(ericsson_file.tell()) + \
            #       ", chunk number is:" + str(k) + ", after record number:" + str(i)
            bytes_already_read += (bytes_to_read - len(data))
            bytes_to_read = chunk_size + len(data)

        # ###print "One chunk done:" + str(k) + " , Bytes already read is :" + str(bytes_already_read) + " , "  +
        # "Bytes to be read is :" + str(bytes_to_read)

        # The len(data) would also be zero if we hit the last chunk abd last record. Then we break out of main loop
        # which iterates over the chunk
        if len(data) == 0 and bytes_already_read > file_size:
            print "Seems we reached end of file with bytes_already_read as:" + str(bytes_already_read)
            break

    ggsn_success_count = len(ggsn_array)
    ggsn_pdp_traffic_count = len(ggsn_cdr_pdp_traffic_volume_array)
    egsn_base_success_count = len(egsn_pdp_array)
    ggsn_pdp_rec_ext_count = len(ggsn_cdr_pdp_rec_extension_array)
    egsn_pdp_traffic_count = len(egsn_cdr_pdp_traffic_volume_array)
    egsn_pdp_servicedata_count = len(egsn_cdr_pdp_service_data_array)
    egsn_pdp_rec_ext_count = len(ggsn_cdr_pdp_rec_extension_array)


    total_count = ggsn_total_count + egsn_base_total_count
    success_count = ggsn_success_count + egsn_base_success_count
    fail_count = ggsn_failed_count + egsn_base_failed_count

    ericsson_ggsn_stats = EricssonStats(iteration_id ,file_id, file_name,check_sum ,
                                        total_count,success_count,fail_count,ggsn_total_count, ggsn_success_count, ggsn_failed_count, ggsn_pdp_traffic_count, ggsn_pdp_rec_ext_count,
                                        egsn_base_total_count,egsn_base_success_count,egsn_base_failed_count,egsn_pdp_traffic_count,egsn_pdp_servicedata_count, egsn_pdp_rec_ext_count
                                        )

    cdr_parsed.append(EricssonggsnParsedRecord(file_name, ericsson_ggsn_stats,ggsn_array,ggsn_cdr_pdp_traffic_volume_array,ggsn_cdr_pdp_rec_extension_array,
                                               egsn_pdp_array,egsn_cdr_pdp_traffic_volume_array,egsn_cdr_pdp_service_data_array,egsn_cdr_pdp_rec_extension_array))



    ericsson_file.close()

    print "size of cdr pdpggsn array :" + str(len(ggsn_array))
    print "size of cdr pdpegsn array :"+str(len(egsn_pdp_array))
    print "size of cdr pdp ggsntrafiicvolume:"+str(len(ggsn_cdr_pdp_traffic_volume_array))
    print "size of cdr pdp egsntrafiicvolume:" + str(len(egsn_cdr_pdp_traffic_volume_array))
    print "size of cdr pdp egsnrecord ext:"+str(len(egsn_cdr_pdp_rec_extension_array))
    print "size of cdr pdp ggsn record ext:"+str(len(ggsn_cdr_pdp_rec_extension_array))
    print "size of cdr pdp egsnservicedata:" + str(len(egsn_cdr_pdp_service_data_array))


    return cdr_parsed





