import StringIO
import socket
import sys
import uuid

from pyasn1 import error
from pyasn1.codec.ber import decoder
from datetime import datetime,date
from affirm_asn_gen import *
from affirm_parsed import *
from affirm_stats import AffirmStats
from common.utilities import *


def process_pgw_records(pgw_record, file_id,file_created_timestamp):

    print "starting of process"+file_created_timestamp
    recordtype = int(pgw_record.getComponentByName("recordType"))
    servedIMSI = IMSI(pgw_record.getComponentByName("servedIMSI")).prettyPrint()
    pgwaddress = pgw_record.getComponentByName("p-GWAddress")
    pgwaddress_ip_binary_address = pgwaddress.getComponentByName("iPBinaryAddress")
    p_GWAddress = socket.inet_ntoa(
        binascii.unhexlify(binascii.hexlify(str(pgwaddress_ip_binary_address.getComponent()))))

    charging_id = long(str(pgw_record.getComponentByName("chargingID")))
    access_point_name_ni = AccessPointNameNI(pgw_record.getComponentByName("accessPointNameNI")).prettyPrint()


    serving_node_address_tmp = pgw_record.getComponentByName("servingNodeAddress")
    for idx in range(len(serving_node_address_tmp)):
        servingnode_ip_binary_address = serving_node_address_tmp.getComponentByPosition(idx)
        servingnodeaddress_iPBinaryAddress = servingnode_ip_binary_address.getComponentByName("iPBinaryAddress")
        servingnodeaddress_iPBinV4Address = servingnodeaddress_iPBinaryAddress.getComponentByName("iPBinV4Address")
        serving_node_address = socket.inet_ntoa(binascii.unhexlify(binascii.hexlify(str(servingnodeaddress_iPBinV4Address))))

    pdp_type = binascii.hexlify(str(pgw_record.getComponentByName("pdpPDNType"))).replace("f", "")
    #################servedPDPDNAddress start#################"
    pdp_address = pgw_record.getComponentByName("servedPDPPDNAddress")
    ip_address = pdp_address.getComponentByName("iPAddress")
    ip_binary_address = ip_address.getComponentByName("iPBinaryAddress")
    servedPDPPDNAddress = socket.inet_ntoa(binascii.unhexlify(binascii.hexlify(str(ip_binary_address.getComponent()))))
    #################servedPDPAddress end#################"
    dynamic_address_flag = 0
    dynamic_address_flag_tmp = pgw_record.getComponentByName("dynamicAddressFlag")
    if dynamic_address_flag_tmp is not None:
        dynamic_address_flag = str(dynamic_address_flag_tmp)
    #################recordOpeningTime start################
    record_opening_time_tmp = binascii.hexlify(str(pgw_record.getComponentByName("recordOpeningTime")))
    record_opening_time = str(append_timezone_offset(parseTimestamp(record_opening_time_tmp)))
    #################recordOpeningTime end#################"
    duration = 0
    duration_tmp = pgw_record.getComponentByName("duration")
    if duration_tmp is not None:
        duration = int(str(duration_tmp))
    cause_for_rec_closing = str(pgw_record.getComponentByName("causeForRecClosing"))

    if pgw_record.getComponentByName("diagnostics") is None:
        diagnostics = ""
    else:
        diagnostics = str(pgw_record.getComponentByName("diagnostics"))

    record_sequence_num = int(binascii.hexlify(str(pgw_record.getComponentByName("recordSequenceNumber"))))
    if record_sequence_num is not None:
        record_sequence_number = record_sequence_num
    else:
        record_sequence_number = -255

    node_id = str(pgw_record.getComponentByName("nodeID"))

    local_sequence_number = int(str(pgw_record.getComponentByName("localSequenceNumber")))

    apn_selection_mode_tmp = APNSelectionMode(pgw_record.getComponentByName("apnSelectionMode"))
    apn_selection_mode = ""
    if apn_selection_mode_tmp == 0:
        apn_selection_mode = "mSorNetworkProvidedSubscriptionVerified"
    elif apn_selection_mode_tmp == 1:
        apn_selection_mode = "mSProvidedSubscriptionNotVerified"
    elif apn_selection_mode == 2:
        apn_selection_mode = "networkProvidedSubscriptionNotVerified"

    charging_characteristics = binascii.hexlify(str(pgw_record.getComponentByName("chargingCharacteristics")))

    ch_ch_selection_mode_tmp = ChChSelectionMode(pgw_record.getComponentByName("chChSelectionMode"))
    ch_ch_selection_mode = get_ch_ch_selection_mode(ch_ch_selection_mode_tmp)

    servingNodePLMNIdentifier = binascii.hexlify(str(pgw_record.getComponentByName("servingNodePLMNIdentifier")))


    pSFurnishChargingInformation_tmp = pgw_record.getComponentByName("pSFurnishChargingInformation")
    if pSFurnishChargingInformation_tmp is not None:
        pSFurnishChargingInformation = str(pSFurnishChargingInformation_tmp)
    else:
        pSFurnishChargingInformation = ""

    ms_time_zone_tmp = (binascii.hexlify(str(pgw_record.getComponentByName("mSTimeZone"))))
    ms_time_zone = ""
    if ms_time_zone_tmp is not None:
        ms_time_zone = ms_time_zone_tmp

    userLocationInformation = (binascii.hexlify(str(pgw_record.getComponentByName("userLocationInformation"))))

    ##############################list of service data start ########################################################"
    listofservicedata = pgw_record.getComponentByName("listOfServiceData")
    listofservicedata_key = str(uuid.uuid4())
    listOfServiceData_array = []
    for idx in range(len(listofservicedata)):
        change_of_service_cond = listofservicedata.getComponentByPosition(idx)
        ratinggroup = (binascii.hexlify(str(change_of_service_cond.getComponentByName("ratingGroup"))))
        chargingRuleBaseName = binascii.hexlify(str(change_of_service_cond.getComponentByName("chargingRuleBaseName")))
        resultCode = ""
        resultCode_tmp = change_of_service_cond.getComponentByName("resultCode")
        if resultCode_tmp is not None:
            try:
                resultCode = str(resultCode_tmp)
            except error.PyAsn1Error as ep:
                resultCode = ""

        local_Sequence_Number = (str(change_of_service_cond.getComponentByName("localSequenceNumber")))

        try:
            timeOfFirstUsage_tmp = binascii.hexlify(str(change_of_service_cond.getComponentByName("timeOfFirstUsage")))
            timeOfFirstUsage = str(append_timezone_offset(parseTimestamp(timeOfFirstUsage_tmp)))
        except error.PyAsn1Error as ep:
            timeOfFirstUsage = ""
        except Exception as ep:
            timeOfFirstUsage = ""

        timeOfLastUsage = ""
        try:
            timeOfLastUsage_tmp = binascii.hexlify(str(change_of_service_cond.getComponentByName("timeOfLastUsage")))
            timeOfLastUsage = str(append_timezone_offset(parseTimestamp(timeOfLastUsage_tmp)))
        except error.PyAsn1Error as ep:
            timeOfLastUsage = ""
        except Exception as ep:
            timeofLastUsage = ""

        try:
            timeUsage = str(change_of_service_cond.getComponentByName("timeUsage"))
        except error.PyAsn1Error as ep:
            timeUsage = ""
        except Exception as ep:
            timeUsage = ""

        serviceConditionChange_temp = (change_of_service_cond.getComponentByName("serviceConditionChange"))
        serviceConditionChange = ""
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
            serviceConditionChange = "qoSChange"
        elif serviceConditionChange == 31:
            serviceConditionChange = "tAIChange"
        elif serviceConditionChange == 32:
            serviceConditionChange = "userLocationChange"
        ######################## service change condition ends ##################################
        #TODO - seems not required
        qoSInformationNeg = str(change_of_service_cond.getComponentByName("qoSInformationNeg"))

        serving_node_address_tmp = pgw_record.getComponentByName("servingNodeAddress")
        for idx in range(len(serving_node_address_tmp)):
            servingnode_ip_binary_address = serving_node_address_tmp.getComponentByPosition(idx)
            servingnodeaddress_iPBinaryAddress = servingnode_ip_binary_address.getComponentByName("iPBinaryAddress")
            servingnodeaddress_iPBinV4Address = servingnodeaddress_iPBinaryAddress.getComponentByName("iPBinV4Address")
            serving_node_address = socket.inet_ntoa(binascii.unhexlify(binascii.hexlify(str(servingnodeaddress_iPBinV4Address))))

        ######################## service condition change starts ################################"

        datavolumeFBCUplink = 0
        datavolumeFBCUplink_temp = binascii.hexlify(
            str(change_of_service_cond.getComponentByName("datavolumeFBCUplink")))
        if datavolumeFBCUplink_temp is not None:
            datavolumeFBCUplink = (str(datavolumeFBCUplink_temp))

        datavolumeFBCDownlink = 0
        datavolumeFBCDownlink_temp = binascii.hexlify(
            str(change_of_service_cond.getComponentByName("datavolumeFBCDownlink")))
        if datavolumeFBCDownlink_temp is not None:
            datavolumeFBCDownlink = (str(datavolumeFBCDownlink_temp))

        timeOfReport_tmp = binascii.hexlify(str(change_of_service_cond.getComponentByName("timeOfReport")))
        timeOfReport = str(append_timezone_offset(parseTimestamp(timeOfReport_tmp)))
        # # timeOfReport = parseTimestamp(
        # str(binascii.hexlify(str(change_of_service_cond.getComponentByName("timeOfReport")))))
        failureHandlingContinue = ""
        failureHandlingContinue_tmp = change_of_service_cond.getComponentByName("failureHandlingContinue")
        if failureHandlingContinue_tmp is not None:
            try:
                failureHandlingContinue = str(failureHandlingContinue_tmp)
            except error.PyAsn1Error as ep:
                failureHandlingContinue = ""

        serviceIdentifier = int(binascii.hexlify(str(change_of_service_cond.getComponentByName("serviceIdentifier"))))

        try:
            pSFurnishChargingInformation = str(pSFurnishChargingInformation_tmp)
        except error.PyAsn1Error as ep:
            pSFurnishChargingInformation = ""

        userLocationInformation = (binascii.hexlify(str(change_of_service_cond.getComponentByName("userLocationInformation"))))
        year = int(file_created_timestamp[0:-4])
        month = int(file_created_timestamp[4:-2])
        day = int(file_created_timestamp[6:])
        # year = '2016'
        # month = '10'
        # day = '09'

        listOfServiceData_item = [file_id, listofservicedata_key ,ratinggroup, chargingRuleBaseName, resultCode, local_Sequence_Number,
                                  timeOfFirstUsage, timeOfLastUsage, timeUsage,serviceConditionChange,
                                  serving_node_address, datavolumeFBCUplink, datavolumeFBCDownlink,
                                  timeOfReport, failureHandlingContinue, serviceIdentifier,pSFurnishChargingInformation,
                                  userLocationInformation,int(file_created_timestamp), year, month, day]

        listOfServiceData_array.append(listOfServiceData_item)


    ###############################list of service data ends ###########################"

    ###################### SERVING NODE TYPE ###########################################
    servingNodeType_seq = pgw_record.getComponentByName("servingNodeType")
    result = ""
    if servingNodeType_seq is not None and len(servingNodeType_seq) > 0:
        for idx in range(len(servingNodeType_seq)):
            result = result + str(ServingNodeType(servingNodeType_seq[idx])) + ","
        servingNodeType = result.rstrip(",")
    else:
        servingNodeType = ""

    ################################# served MNNAI ###########################
    servedMNNAI_temp = pgw_record.getComponentByName("servedMNNAI")
    subscriptionIDType = str(SubscriptionIDType(servedMNNAI_temp.getComponentByName("subscriptionIDType")))
    subscriptionIDData = str(servedMNNAI_temp.getComponentByName("subscriptionIDData"))

        ############################################################################
    served_imeisv = (binascii.hexlify(str((pgw_record.getComponentByName("servedIMEISV")))))
    served_imsi = binascii.hexlify(str(pgw_record.getComponentByName("servedIMSI")))

    served_msisdn = ""
    try:
        served_msisdn = get_imei_and_imsi(pgw_record,"servedMSISDN")
    except error.PyAsn1Error as ep:
        served_msisdn = ""

    ########################starttime & stop time###############
    try:
        start_time = str(parseTimestamp(str(binascii.hexlify(str(pgw_record.getComponentByName("startTime"))))))
    except error.PyAsn1Error as ep:
        start_time = ""

    try:
        stop_time = str(parseTimestamp(str(binascii.hexlify(str(pgw_record.getComponentByName("stopTime"))))))
    except error.PyAsn1Error as ep:
        stop_time = ""
    except Exception as ep:
        stop_time = ""
    ############################################################
    pDNConnectionID = int(str(pgw_record.getComponentByName("pDNConnectionID")))

    servedPDPPDNAddressExt = str(pgw_record.getComponentByName("servedPDPPDNAddressExt"))
    #################servedPDPAddress end#################"
    pGWPLMNIdentifier = get_plmn_id(binascii.hexlify(str(pgw_record.getComponentByName("p-GWPLMNIdentifier"))))

    rat_type_tmp = pgw_record.getComponentByName("rATType")
    if rat_type_tmp is not None:
        rat_type = int(str(rat_type_tmp))
    # year = '2016'
    # month = '10'
    # day = '09'

    pgw_record_item = [file_id , recordtype, servedIMSI, p_GWAddress,
                       charging_id,access_point_name_ni, serving_node_address,  pdp_type, servedPDPPDNAddress,
                       dynamic_address_flag,record_opening_time , duration, cause_for_rec_closing, diagnostics, record_sequence_num,
                       node_id, local_sequence_number, apn_selection_mode,
                       charging_characteristics,
                       ch_ch_selection_mode, servingNodePLMNIdentifier,
                       pSFurnishChargingInformation,served_imeisv, ms_time_zone,
                       userLocationInformation,listofservicedata_key,
                       servingNodeType, subscriptionIDType,subscriptionIDData, pGWPLMNIdentifier,  served_msisdn,start_time,stop_time,
                       pDNConnectionID, servedPDPPDNAddressExt,
                       rat_type,int(file_created_timestamp), year, month, day]
    print "pgw_record_item and listOfServiceData_array ######################################################"
    print pgw_record_item
    print listOfServiceData_array

    return pgw_record_item, listOfServiceData_array



def process_sgw_records(sgw_record, file_id,file_created_timestamp):

    print "starting of process FOR SGW_Record"

    recordtype = int(sgw_record.getComponentByName("recordType"))
    print recordtype
    servedIMSI = IMSI(sgw_record.getComponentByName("servedIMSI")).prettyPrint()
    print servedIMSI+"valuse of second field"
    s_GWAddress = GSNAddress(sgw_record.getComponentByName("s-GWAddress")).prettyPrint()
    print s_GWAddress
    charging_id = long(str(sgw_record.getComponentByName("chargingID")))
    print charging_id
    servingnodeaddress = GSNAddress(sgw_record.getComponentByName("servingNodeAddress")).prettyPrint()
    print servingnodeaddress
    access_point_name_ni = AccessPointNameNI(sgw_record.getComponentByName("accessPointNameNI")).prettyPrint()
    print access_point_name_ni
    pdp_type = binascii.hexlify(str(sgw_record.getComponentByName("pdpPDNType"))).replace("f", "")
    print pdp_type

    #################servedPDPAddress start#################"
    pdp_address = sgw_record.getComponentByName("servedPDPPDNAddress")
    print pdp_address
    servedPDPPDNAddressExt = sgw_record.getComponentByName("servedPDPPDNAddressExt")
    #################servedPDPAddress end#################"


    dynamic_address_flag = 0
    dynamic_address_flag_tmp = sgw_record.getComponentByName("dynamicAddressFlag")
    if dynamic_address_flag_tmp is not None:
        dynamic_address_flag = long(str(dynamic_address_flag_tmp))
        print dynamic_address_flag

    dynamic_address_flag_Ext = 0
    dynamic_address_flag_tmp_ext = sgw_record.getComponentByName("dynamicAddressFlagExt")
    if dynamic_address_flag_tmp_ext is not None:
        dynamic_address_flag_Ext = long(str(dynamic_address_flag_tmp_ext))

    ##########################list of traffic volumes ############################################
    #list_of_traffic_volumes = sgw_record.getComponentByName("listOfTrafficVolumes")
    list_of_traffic_volumes="abc"
    #################recordOpeningTime start#################
    record_opening_time_tmp = binascii.hexlify(str(sgw_record.getComponentByName("recordOpeningTime")))
    record_opening_time = append_timezone_offset(parseTimestamp(record_opening_time_tmp))
    print record_opening_time
    #################recordOpeningTime end#################"

    duration = 0
    duration_tmp = sgw_record.getComponentByName("duration")
    if duration_tmp is not None:
        duration = long(str(duration_tmp))
        print duration
    cause_for_rec_closing = long(str(sgw_record.getComponentByName("causeForRecClosing")))
    print cause_for_rec_closing
    if sgw_record.getComponentByName("diagnostics") is None:
        diagnostics = ""
    else:
        diagnostics = str(sgw_record.getComponentByName("diagnostics"))

    #################recordOpeningTime end#################"
    record_sequence_num = int(binascii.hexlify(str(sgw_record.getComponentByName("recordSequenceNumber"))))
    if record_sequence_num is not None:
        record_sequence_number = long(record_sequence_num)
    else:
        record_sequence_number = -255
        print record_sequence_num

    node_id = str(sgw_record.getComponentByName("nodeID"))
    print node_id

    local_sequence_number = int(str(sgw_record.getComponentByName("localSequenceNumber")))
    print local_sequence_number
    ################# record extension key starts ###########################################
    key = uuid.uuid4()
    record_extension_key = str(key)
    record_extension = sgw_record.getComponentByName("recordExtensions")
    identifier = ""
    ts48018BssgpCause = ""
    ts25413RanapCause = ""
    significance = ""
    cdr_rec_ext_array = []
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
                                   str(ts25413RanapCause), str(significance)]
            cdr_rec_ext_array.append(rec_extensions_item)

    ################## record extension key ends ############################################


    apn_selection_mode_tmp = APNSelectionMode(sgw_record.getComponentByName("apnSelectionMode"))
    apn_selection_mode = ""
    if apn_selection_mode_tmp == 0:
        apn_selection_mode = "mSorNetworkProvidedSubscriptionVerified"
    elif apn_selection_mode_tmp == 1:
        apn_selection_mode = "mSProvidedSubscriptionNotVerified"
    elif apn_selection_mode == 2:
        apn_selection_mode = "networkProvidedSubscriptionNotVerified"
    print apn_selection_mode

    served_msisdn = binascii.hexlify(binascii.hexlify(str(sgw_record.getComponentByName("servedMSISDN"))))
    print served_msisdn

    charging_characteristics = binascii.hexlify(str(sgw_record.getComponentByName("chargingCharacteristics")))
    print charging_characteristics


    ch_ch_selection_mode_tmp = ChChSelectionMode(sgw_record.getComponentByName("chChSelectionMode"))
    ch_ch_selection_mode = get_ch_ch_selection_mode(ch_ch_selection_mode_tmp)
    print ch_ch_selection_mode
    iMSsignalingContext=sgw_record.getComponentByName("iMSsignalingContext")
    print iMSsignalingContext
    servingNodePLMNIdentifier = binascii.hexlify(str(sgw_record.getComponentByName("servingNodePLMNIdentifier")))
    print servingNodePLMNIdentifier

    served_imei = (binascii.hexlify(str((sgw_record.getComponentByName("servedIMEISV")))))
    print served_imei

    rat_type_tmp = sgw_record.getComponentByName("rATType")
    if rat_type_tmp is not None:
        rat_type = int(str(rat_type_tmp))
        print rat_type

    ms_time_zone_tmp = (binascii.hexlify(str(sgw_record.getComponentByName("mSTimeZone"))))
    ms_time_zone = ""
    if ms_time_zone_tmp is not None:
        ms_time_zone = ms_time_zone_tmp
        print ms_time_zone


    userLocationInformation = binascii.hexlify(str(sgw_record.getComponentByName("userLocationInformation")))
    print userLocationInformation
    sGWChange=sgw_record.getComponentByName("sGWChange")
    print sGWChange
    ############## serving node type start ##########################
    servingNodeType_tmp = ServingNodeType(sgw_record.getComponentByName("servingNodeType"))
    servingNodeType = ""
    if servingNodeType_tmp == 0:
        servingNodeType = "sGSN"
    elif servingNodeType_tmp == 1:
        servingNodeType = "pMIPSGW"
    elif servingNodeType_tmp == 2:
        servingNodeType = "gTPSGW"
    elif servingNodeType_tmp == 3:
        servingNodeType = "ePDG"
    elif servingNodeType_tmp == 4:
        servingNodeType = "hSGW"
    elif servingNodeType_tmp == 5:
        servingNodeType = "mME"

    print servingNodeType
    ############## serving node type ends ###########################

    p_GWAddressUsed = GSNAddress(sgw_record.getComponentByName("p-GWAddressUsed")).prettyPrint()
    print p_GWAddressUsed

    pGWPLMNIdentifier = get_plmn_id(binascii.hexlify(str(sgw_record.getComponentByName("p-GWPLMNIdentifier"))))
    print  pGWPLMNIdentifier
    ########################starttime & stop time###############

    start_time_temp = binascii.hexlify(str(sgw_record.getComponentByName("startTime")))
    start_time = append_timezone_offset(parseTimestamp(start_time_temp))
    print start_time
    stop_time_temp = binascii.hexlify(str(sgw_record.getComponentByName("stopTime")))
    stop_time = append_timezone_offset(parseTimestamp(stop_time_temp))
    print stop_time
    ############################################################

    pDNConnectionID = int(str(sgw_record.getComponentByName("pDNConnectionID")))
    print pDNConnectionID

    userCSGInformation=UserCSGInformation(sgw_record.getComponentByName("userCSGInformation"))
    print userCSGInformation

    servedPDPPDNAddressExt = str(sgw_record.getComponentByName("servedPDPPDNAddressExt"))
    print servedPDPPDNAddressExt

    lowAccessPriorityIndicator=sgw_record.getComponentByName("lowAccessPriorityIndicator")
    print lowAccessPriorityIndicator
    #################Dynamic Address Flag ext #################"
    dynamic_address_flag_ext = 0
    dynamic_address_flag_tmp = sgw_record.getComponentByName("dynamicAddressFlagExt")
    if dynamic_address_flag_tmp is not None:
        dynamic_address_flag_ext = str(dynamic_address_flag_tmp)
        print dynamic_address_flag_ext

    sGWiPv6Address= GSNAddress(sgw_record.getComponentByName("s-GWiPv6Address")).prettyPrint()
    print sGWiPv6Address

    servingNodeiPv6Address = GSNAddress(sgw_record.getComponentByName("servingNodeiPv6Address")).prettyPrint()
    print servingNodeiPv6Address

    pGWiPv6AddressUsed=GSNAddress(sgw_record.getComponentByName("p-GWiPv6AddressUsed")).prettyPrint()
    print pGWiPv6AddressUsed
    print "end process###################"
    sgw_record_item = [file_id,recordtype,servedIMSI,s_GWAddress,charging_id,servingnodeaddress,access_point_name_ni,
                       pdp_type,pdp_address,dynamic_address_flag,list_of_traffic_volumes, record_opening_time, duration,
                       cause_for_rec_closing,diagnostics, record_sequence_number,node_id ,record_extension_key,
                       local_sequence_number, apn_selection_mode,  served_msisdn,charging_characteristics,
                       ch_ch_selection_mode, iMSsignalingContext, servingNodePLMNIdentifier,served_imei,rat_type,
                       ms_time_zone,userLocationInformation, sGWChange, servingNodeType,p_GWAddressUsed,pGWPLMNIdentifier,
                       start_time, stop_time,pDNConnectionID, userCSGInformation, servedPDPPDNAddressExt,
                       lowAccessPriorityIndicator,dynamic_address_flag_ext,sGWiPv6Address, servingNodeiPv6Address,
                       pGWiPv6AddressUsed,file_created_timestamp]


    return sgw_record_item, cdr_rec_ext_array



def parse_affirm_file(hdfs_file_name, content, iteration_id):

    print "Started parsing:" + str(hdfs_file_name)
    affirm_file = StringIO.StringIO()
    affirm_file.write(content)  # create the string object and write the content
    file_name=get_file_name(hdfs_file_name)
    print file_name
    affirm_file.seek(0)  # change the file object position

    # Getting file name details to append to records
    _absoluteFileName = hdfs_file_name.rpartition(':')[2]
    filename = _absoluteFileName.rpartition('/')[2]
    filePath = _absoluteFileName.rpartition('/')[0]

    #TODO
    #check_sum = get_file_check_sum(str(hdfs_file_name))
    check_sum = 123478987654321
    file_id = str(uuid.uuid4())
    file_created_timestamp= file_date(file_name)
    print file_name+"%%%%%%%%%%%%%%%%%%%"
    print file_created_timestamp+"$$$$$$$$$$$$$$"
    year = int(file_created_timestamp[0:-4])
    month = int(file_created_timestamp[4:-2])
    day = int(file_created_timestamp[6:])
    chunk_size = AFFIRMED_FILE_CHUNK_SIZE

    bytes_already_read = 0
    k = 0

    total_count = 0


    pgw_total_count = 0
    pgw_success_count = 0
    pgw_failed_count = 0


    pgw_lsd_count=0

    sgw_total_count = 0
    sgw_success_count = 0
    sgw_failed_count = 0

    sgw_rec_ext_success_count = 0


    sys.tracebacklimit = 0
    start_time_for_iteration = time.time()

    file_size = len(content)
    # We read the entire file in smaller chunks. The chunk most likely will not end with completed record.
    # We initialize the bytes to read with a specific configured value from constants file
    bytes_to_read = chunk_size
    print "File size:" + str(file_size)
    cdr_parsed = []
    pgw_array = []
    sgw_array = []
    pgw_list_of_service_data_array= []
    sgw_rec_ext_arrray = []



    print "Starting reading the chunks.........."
    # With each iteration the bytes already read would be updated, and we need to iterate until the end of the file size
    while bytes_already_read < file_size:

        data = affirm_file.read(bytes_to_read)
        # ### print "Before reading:" + str(fh.tell()) + ", After reading:" + str(fh.tell()) + "-" + str(len(data))
        print "records stored in the chunk "
        is_record_ended_good = True
        k += 1
        # The decoder decodes each record from the chunk until an exception is thrown due to last record
        # having in-sufficient(or lesser) length

        while data != "":
            try:
                ab, data = decoder.decode(data, asn1Spec=GPRSRecord())
                total_count += 1
                pgw_record = ab.getComponentByName("pgwRecord")
                sgw_record = ab.getComponentByName("sGWRecord")
                #print pgw_record
                #print sgw_record

                #print sgsn_pdp_record
                #print ggsn_pdp_record

                if pgw_record is not None:
                    pgw_total_count += 1
                    pgw_item,list_of_service_data_arr = process_pgw_records(pgw_record, file_id, file_created_timestamp)
                    pgw_array.append(pgw_item)

                    if list_of_service_data_arr is not None and len(list_of_service_data_arr) > 0:
                        for idx in range(len(list_of_service_data_arr)):
                            pgw_list_of_service_data_array.append(list_of_service_data_arr[idx])
                    #TODO
                # elif sgw_record is not None:
                #     sgw_total_count += 1
                #     sgw_item,rec_ext_arr = process_sgw_records(sgw_record, file_id)
                #     sgw_array.append(sgw_item)
                #
                #     if rec_ext_arr is not None and len(rec_ext_arr) > 0:
                #         for idx in range(len(rec_ext_arr)):
                #             sgw_rec_ext_arrray.append(rec_ext_arr[idx])

                # elif sgsn_pdp_record is not None:
                #     sgsn_pdp_record += 1
                #
                # elif sgsn_pdp_record is not None:
                #     ggsn_pdp_record += 1

                #     print "file under affirm process records is parsed for Sgw"
                #     sgw_item = process_sgw_records(sgw_record, file_id)
                #     sgw_array.append(sgw_item)

            # When an exception occurs with SubstrateUnderrunError, it is due to smaller record size. So we need to
            # get the remaining bytes in the chunk and reposition the pointer so that the remaining bytes will be added
            # to the next chunk and the process repeats
            except error.SubstrateUnderrunError as ep:
                is_record_ended_good = False
                # ###logging.exception('decode failure ######### :' + str(i))
                last_record_position = affirm_file.tell()
                affirm_file.seek(last_record_position - len(data))
                # print "SubstrateUnderrunError : data bytes left in this chunk:" + str(len(data)) + " , Current position in file :" \
                #       + str(last_record_position) + " , Seek for next chunk: " + str(ericsson_file.tell()) + \
                #       ", chunk number is:" + str(k)
                bytes_already_read += (bytes_to_read - len(data))
                bytes_to_read = chunk_size + len(data)
                break
            except error.PyAsn1Error as ep:
                is_record_ended_good = False
                print (str(ep.message) + "********************************************")
                # logging.exception(p.message)
                last_record_position = affirm_file.tell()
                affirm_file.seek(last_record_position - len(data))
                print "PyAsn1Error : data bytes left in this chunk:" + str(len(data)) + " , Current position in file :" \
                      + str(last_record_position) + " , Seek for next chunk: " + str(affirm_file.tell()) + \
                      ", chunk number is:" + str(k)
                bytes_already_read += (bytes_to_read - len(data))
                bytes_to_read = chunk_size + len(data)
                break
            except Exception as ep:
                print (str(ep.message) + " temp ##############################")
                # logging.exception(ep.message)

        # We also need to handle the scenario when our chunk is ending with an exact record. The len(data) would be
        # zero in that case
        if is_record_ended_good:
            last_record_position = affirm_file.tell()
            affirm_file.seek(last_record_position - len(data))
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


    pgw_success_count = len(pgw_array)
    pgw_lsd_success_count = len(pgw_list_of_service_data_array)
    sgw_success_count = len(sgw_array)
    sgw_rec_ext_success_count = len(sgw_rec_ext_arrray)

    print "total:" + str(total_count)
    print "pgw_total_count" + str(pgw_total_count)
    print "service_data_list_count" + str(pgw_lsd_success_count)
    print "sgw_total_count" + str(sgw_total_count)
    #print "sgsn_pdp_total_count" + str(sgsn_pdp_total_count)
    #print "ggsn_pdp_total_count" + str(ggsn_pdp_total_count)


    total_count = pgw_total_count + sgw_total_count

    success_count = pgw_success_count + sgw_success_count

    fail_count = pgw_failed_count + sgw_failed_count

    # iter_stats = AffirmStats(iteration_id, file_id, file_name, check_sum, total_count, success_count, fail_count,
    #
    #                        pgw_total_count, pgw_success_count, pgw_failed_count, pgw_lsd_count)

    x = [iteration_id, file_id, file_name, str(check_sum), long(total_count), long(success_count), long(fail_count),
                             long(pgw_total_count), long(pgw_success_count), long(pgw_failed_count),
                             long(pgw_lsd_count),long(0),long(0),long(0),long(0), int(file_created_timestamp), year, month, day]
    print x
    print "@@@@@@@ stats data @@@@@@@@@@"



    iter_stats = AffirmStats(iteration_id, file_id, file_name, str(check_sum), long(total_count), long(success_count), long(fail_count),
                             long(pgw_total_count), long(pgw_success_count), long(pgw_failed_count),
                             long(pgw_lsd_count),long(0),long(0),long(0),long(0), int(file_created_timestamp), year, month, day)

    cdr_parsed.append(AffirmParsedRecord(file_name, iter_stats, pgw_array,pgw_list_of_service_data_array,sgw_array, sgw_rec_ext_arrray))
    affirm_file.close()

    print "size of cdr pdpd array  :" + str(len(cdr_parsed))
    #print cdr_parsed


    return cdr_parsed


def file_date(hdfs_file_name):
  fileName=(os.path.splitext(os.path.splitext(os.path.basename(hdfs_file_name))[0])[0])
  temp=fileName.split("_")
  temp1=temp[1]
  filedate=temp1[0:-6]

  print filedate+"&&&&&&&&&&&&&&&&"

  return filedate

def main(argv=None):

    input_path = "/home/cloudera/Downloads/CDR_20161007050016_229.asn1"
   # input_path = "/Users/vn0xm03/Documents/Ramz/Personal/atni_sample/sample_files/CDR_20161007060017_230.asn1"

    iteration_id = str(get_current_time_and_day())

    with open(input_path, mode='rb') as file: # b is important -> binary
        fileContent = file.read()
        parse_affirm_file(input_path, fileContent,iteration_id)



if __name__ == "__main__":
    sys.exit(main())
