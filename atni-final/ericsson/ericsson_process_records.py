import StringIO
import logging
import socket
import sys
import time
import uuid

from atni.parsers.constants import *
from pyasn1 import error
from pyasn1.codec.ber import decoder

from common.utilities import *
from ericsson_asn_gen import *
from ericsson_asn_gen import CallEventRecord
from ericsson_parsed import EricssonParsedRecord
from ericsson_stats import EricssonStats


def process_smo_records(smo_record, file_id):


    cAMELInformationSMS = smo_record.getComponentByName("cAMELInformationSMS")

    cell_identifier = format(int(binascii.hexlify(str(smo_record.getComponentByName("cellIdentifier"))),16),'05')
    charging_characteristics = binascii.hexlify(str(smo_record.getComponentByName("chargingCharacteristics")))

    ch_ch_selection_mode_tmp = ChChSelectionMode(smo_record.getComponentByName("chChSelectionMode"))
    ch_ch_selection_mode = get_ch_ch_selection_mode(ch_ch_selection_mode_tmp)

    destination_number = get_imei_and_imsi(smo_record, "destinationNumber")

    event_timestamp_tmp = binascii.hexlify(str(smo_record.getComponentByName("eventTimeStamp")))
    event_timestamp = append_timezone_offset(parseTimestamp(event_timestamp_tmp))

    local_sequence_number = long(str(smo_record.getComponentByName("localSequenceNumber")))

    location_area_code = format(int(binascii.hexlify(str(smo_record.getComponentByName("locationArea"))),16),'05')
    message_reference = binascii.hexlify(str(smo_record.getComponentByName("messageReference")))
    ms_network_capability = binascii.hexlify(str(smo_record.getComponentByName("msNetworkCapability")))
    node_id = str(smo_record.getComponentByName("nodeID"))
    plmn_identifier = binascii.hexlify(str(smo_record.getComponentByName("pLMNIdentifier")))
    rat_type = 0
    rat_type_tmp = smo_record.getComponentByName("rATType")
    if rat_type_tmp is not None:
        rat_type = long(str(rat_type_tmp))

    recording_entity = get_imei_and_imsi(smo_record,"recordingEntity")
    record_type = 0
    record_type_tmp = smo_record.getComponentByName("recordType")
    if record_type_tmp is not None:
        record_type = long(str(record_type_tmp))

    routing_area = binascii.hexlify(str(smo_record.getComponentByName("routingArea")))
    served_imei = get_imei_and_imsi(smo_record, "servedIMEI")
    served_imsi = get_imei_and_imsi(smo_record, "servedIMSI")
    served_msisdn= get_imei_and_imsi(smo_record,"servedMSISDN")
    service_centre = get_imei_and_imsi(smo_record,"serviceCentre")

    diagnostics = smo_record.getComponentByName("smsResult")
    sms_result = 0
    if diagnostics is not None:
        sms_result = long(str(diagnostics.getComponent()))

    camel_info_sms_key = str(event_timestamp_tmp) + "_" + str(local_sequence_number)

    smo_record_item = [file_id,camel_info_sms_key,cell_identifier,charging_characteristics,ch_ch_selection_mode,
                       destination_number,event_timestamp,local_sequence_number,location_area_code,
                       message_reference,ms_network_capability,node_id,plmn_identifier,rat_type,recording_entity,
                       record_type,routing_area,served_imei,served_imsi,served_msisdn,service_centre,sms_result,
                       long(event_timestamp.strftime("%Y%m%d")), long(event_timestamp.strftime("%H"))]

    return smo_record_item


def process_smt_records(smt_record, file_id):

    cell_identifier = format(int(binascii.hexlify(str(smt_record.getComponentByName("cellIdentifier"))),16),'05')

    cell_identifier_lastsm_tmp = smt_record.getComponentByName("cellIdentifierLastSM")
    if cell_identifier_lastsm_tmp is not None:
        cell_identifier_lastsm = format(int(binascii.hexlify(str(cell_identifier_lastsm_tmp)),16),'05')
    else:
        cell_identifier_lastsm = ""

    charging_characteristics = binascii.hexlify(str(smt_record.getComponentByName("chargingCharacteristics")))

    ch_ch_selection_mode_tmp = ChChSelectionMode(smt_record.getComponentByName("chChSelectionMode"))
    ch_ch_selection_mode = get_ch_ch_selection_mode(ch_ch_selection_mode_tmp)

    str1 = binascii.hexlify(str(smt_record.getComponentByName("eventTimeStamp")))
    event_timestamp = append_timezone_offset(parseTimestamp(str1))
    local_sequence_number = long(str(smt_record.getComponentByName("localSequenceNumber")))

    location_area_code = format(int(binascii.hexlify(str(smt_record.getComponentByName("locationArea"))),16),'05')

    location_area_lastsm_tmp = smt_record.getComponentByName("locationAreaLastSM")

    if location_area_lastsm_tmp is not None:
        location_area_lastsm = format(int(binascii.hexlify(str(location_area_lastsm_tmp),16)),'05')
    else:
        location_area_lastsm = ""

    ms_network_capability = binascii.hexlify(str(smt_record.getComponentByName("msNetworkCapability")))
    node_id = str(smt_record.getComponentByName("nodeID"))
    number_of_sm = -255
    number_of_sm_tmp = smt_record.getComponentByName("numberOfSM")
    if number_of_sm_tmp is not None:
        number_of_sm = long(str(number_of_sm_tmp))

    plmn_identifier = binascii.hexlify(str(smt_record.getComponentByName("pLMNIdentifier")))
    plmn_identifier_lastsm = binascii.hexlify(str(smt_record.getComponentByName("pLMNIdentifierLastSM")))
    rat_type = -255
    rat_type_tmp = smt_record.getComponentByName("rATType")
    if rat_type_tmp is not None:
        rat_type = long(str(rat_type_tmp))

    recording_entity = get_imei_and_imsi(smt_record,"recordingEntity")
    record_type = -255
    record_type_tmp = smt_record.getComponentByName("recordType")
    if record_type_tmp is not None:
        record_type = long(str(record_type_tmp))

    routing_area = binascii.hexlify(str(smt_record.getComponentByName("routingArea")))
    routing_area_lastsm = binascii.hexlify(str(smt_record.getComponentByName("routingAreaLastSM")))
    served_imei = get_imei_and_imsi(smt_record, "servedIMEI")
    served_imsi = get_imei_and_imsi(smt_record, "servedIMSI")
    served_msisdn= get_imei_and_imsi(smt_record,"servedMSISDN")
    service_centre = get_imei_and_imsi(smt_record,"serviceCentre")

    diagnostics = smt_record.getComponentByName("smsResult")
    sms_result = 0
    if diagnostics is not None:
        sms_result = long(str(diagnostics.getComponent()))

    smt_record_item = [file_id,cell_identifier,cell_identifier_lastsm,charging_characteristics,ch_ch_selection_mode,
                       event_timestamp,local_sequence_number,location_area_code,location_area_lastsm,
                       ms_network_capability,node_id,number_of_sm,plmn_identifier,plmn_identifier_lastsm,rat_type,
                       recording_entity,record_type,routing_area,routing_area_lastsm,served_imei,served_imsi,
                       served_msisdn,service_centre,sms_result,
                       long(event_timestamp.strftime("%Y%m%d")), long(event_timestamp.strftime("%H"))]

    return smt_record_item


def process_pdp_records(pdp_record, file_id):
    # print pdpRecord

    access_point_name_ni = AccessPointNameNI(pdp_record.getComponentByName("accessPointNameNI")).prettyPrint()
    access_point_name_oi = AccessPointNameOI(pdp_record.getComponentByName("accessPointNameOI")).prettyPrint()

    apn_selection_mode_tmp = APNSelectionMode(pdp_record.getComponentByName("apnSelectionMode"))
    apn_selection_mode = ""
    if apn_selection_mode_tmp == 0:
        apn_selection_mode = "mSorNetworkProvidedSubscriptionVerified"
    elif apn_selection_mode_tmp == 1:
        apn_selection_mode = "mSProvidedSubscriptionNotVerified"
    elif apn_selection_mode == 2:
        apn_selection_mode = "networkProvidedSubscriptionNotVerified"


    cause_for_rec_closing = long(str(pdp_record.getComponentByName("causeForRecClosing")))

    cell_identifier = format(int(binascii.hexlify(str(pdp_record.getComponentByName("cellIdentifier"))),16),'05')

    charging_characteristics = binascii.hexlify(
        str(pdp_record.getComponentByName("chargingCharacteristics")))
    charging_id = long(str(pdp_record.getComponentByName("chargingID")))

    ch_ch_selection_mode_tmp = ChChSelectionMode(pdp_record.getComponentByName("chChSelectionMode"))
    ch_ch_selection_mode = get_ch_ch_selection_mode(ch_ch_selection_mode_tmp)

    if pdp_record.getComponentByName("diagnostics") is None:
        diagnostics = ""
    else:
        diagnostics = str(pdp_record.getComponentByName("diagnostics"))

    duration = 0
    duration_tmp = pdp_record.getComponentByName("duration")
    if duration_tmp is not None:
        duration = long(str(duration_tmp))

    dynamic_address_flag = 0
    dynamic_address_flag_tmp = pdp_record.getComponentByName("dynamicAddressFlag")
    if dynamic_address_flag_tmp is not None:
        dynamic_address_flag = long(str(dynamic_address_flag_tmp))

    ggsn_address_used_obj = pdp_record.getComponentByName("ggsnAddressUsed")
    ggsn_ip_binary_address = ggsn_address_used_obj.getComponentByName("iPBinaryAddress")
    ggsn_address_used = socket.inet_ntoa(
        binascii.unhexlify(binascii.hexlify(str(ggsn_ip_binary_address.getComponent()))))


    integer = 0
    local_sequence_number = long(str(pdp_record.getComponentByName("localSequenceNumber")))

    location_area_code = format(int(binascii.hexlify(str(pdp_record.getComponentByName("locationAreaCode"))),16),'05')
    ms_network_capability = binascii.hexlify(str(pdp_record.getComponentByName("msNetworkCapability")))
    # TODO
    ms_time_zone_tmp = str(pdp_record.getComponentByName("mSTimeZone"))
    ms_time_zone = ""
    if ms_time_zone_tmp is not None:
        ms_time_zone = ms_time_zone_tmp

    network_initiation = -255
    network_initiation_tmp = pdp_record.getComponentByName("networkInitiation")
    if network_initiation_tmp is not None:
        network_initiation = int(str(network_initiation_tmp))

    node_id = str(pdp_record.getComponentByName("nodeID"))
    pdp_type = binascii.hexlify(str(pdp_record.getComponentByName("pdpType"))).replace("f", "")

    plmn_identifier = get_plmn_id(binascii.hexlify(str(pdp_record.getComponentByName("pLMNIdentifier"))))
    rat_type = long(str(pdp_record.getComponentByName("rATType")))

    #################recordOpeningTime start#################
    record_opening_time_tmp = binascii.hexlify(str(pdp_record.getComponentByName("recordOpeningTime")))
    record_opening_time = append_timezone_offset(parseTimestamp(record_opening_time_tmp))
    #################recordOpeningTime end#################"

    record_sequence_num_tmp = pdp_record.getComponentByName("recordSequenceNumber")
    if record_sequence_num_tmp is not None:
        record_sequence_number = long(record_sequence_num_tmp)
    else:
        record_sequence_number = -255

    record_type = long(str(pdp_record.getComponentByName("recordType")))
    routing_area = binascii.hexlify(str(pdp_record.getComponentByName("routingArea")))
    served_imei = get_imei_and_imsi(pdp_record, "servedIMEI")
    served_imsi = get_imei_and_imsi(pdp_record, "servedIMSI")
    served_msisdn= get_imei_and_imsi(pdp_record,"servedMSISDN")

    #################servedPDPAddress start#################"
    pdp_address = pdp_record.getComponentByName("servedPDPAddress")
    ip_address = pdp_address.getComponentByName("iPAddress")
    ip_binary_address = ip_address.getComponentByName("iPBinaryAddress")
    served_pdp_address = socket.inet_ntoa(
        binascii.unhexlify(binascii.hexlify(str(ip_binary_address.getComponent()))))
    #################servedPDPAddress end#################"

    #################sgsnAddress start#################"
    sgsn_address_obj = pdp_record.getComponentByName("sgsnAddress")
    sgsn_ip_binary_address = sgsn_address_obj.getComponentByName("iPBinaryAddress")
    sgsn_address = socket.inet_ntoa(
        binascii.unhexlify(binascii.hexlify(str(sgsn_ip_binary_address.getComponent()))))
    #################sgsnAddress end#################"

    sgsn_change_tmp = pdp_record.getComponentByName("sgsnChange")
    sgsn_change = ""
    if sgsn_change_tmp is not None and int(str(sgsn_change_tmp)) == 1:
        sgsn_change = "TRUE"

    #################listOfTrafficVolumes start#################"
    list_of_traffic_volumes = pdp_record.getComponentByName("listOfTrafficVolumes")

    key = str(record_opening_time_tmp) + "_" + str(local_sequence_number)
    traffic_volumes_key = key
    cdr_traffic_volumes_array = []
    for idx in range(len(list_of_traffic_volumes)):
        change_of_char_cond = list_of_traffic_volumes.getComponentByPosition(idx)
        qos_requested = binascii.hexlify(str(change_of_char_cond.getComponentByName("qosRequested")))
        qos_negotiated = binascii.hexlify(str(change_of_char_cond.getComponentByName("qosNegotiated")))

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

        change_time = parseTimestamp(str(binascii.hexlify(str(change_of_char_cond.getComponentByName("changeTime")))))

        traffic_volume_item = [file_id,traffic_volumes_key, change_condition,change_time,data_volume_gprs_downlink,data_volume_gprs_uplink,
                               qos_negotiated,qos_requested]
        cdr_traffic_volumes_array.append(traffic_volume_item)


    #################listOfTrafficVolumes end#################"

    #################cAMELInformationPDP start #################"
    cAMELInformationPDPKey = key
    cAMELInformationPDP = pdp_record.getComponentByName("cAMELInformationPDP")
    camel_information_pdp_item = []

    if cAMELInformationPDP is not None:

        cAMELAccessPointNameNI = cAMELInformationPDP.getComponentByName("cAMELAccessPointNameNI")
        cAMELAccessPointNameOI = cAMELInformationPDP.getComponentByName("cAMELAccessPointNameOI")
        defaultTransactionHandling = int(str(cAMELInformationPDP.getComponentByName("defaultTransactionHandling")))
        fFDAppendIndicator = int(str(cAMELInformationPDP.getComponentByName("fFDAppendIndicator")))
        freeFormatData = cAMELInformationPDP.getComponentByName("freeFormatData")
        levelOfCAMELService = long(str(cAMELInformationPDP.getComponentByName("levelOfCAMELService")))
        numberOfDPEncountered = long(str(cAMELInformationPDP.getComponentByName("numberOfDPEncountered")))
        sCFAddress = cAMELInformationPDP.getComponentByName("sCFAddress")
        serviceKey = long(str(cAMELInformationPDP.getComponentByName("serviceKey")))

        camel_information_pdp_item = [file_id,cAMELInformationPDPKey,cAMELAccessPointNameNI,cAMELAccessPointNameOI,
                                      defaultTransactionHandling,fFDAppendIndicator,freeFormatData,levelOfCAMELService,
                                      numberOfDPEncountered,sCFAddress,serviceKey]

    #################cAMELInformationPDP end #################"

    record_extension_key = str(key)
    record_extension = pdp_record.getComponentByName("recordExtensions")
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



    pdp_record_item = [file_id,access_point_name_ni, access_point_name_oi, apn_selection_mode, cAMELInformationPDPKey,
                       cause_for_rec_closing, cell_identifier,
                       charging_characteristics,charging_id,ch_ch_selection_mode, duration,dynamic_address_flag,
                       ggsn_address_used,integer, local_sequence_number,location_area_code,ms_network_capability,ms_time_zone,
                       network_initiation,node_id,pdp_type,plmn_identifier,rat_type,
                       record_extension_key, record_opening_time,
                       record_sequence_number,record_type,routing_area,served_imei,
                       served_imsi, served_msisdn,served_pdp_address,sgsn_address,sgsn_change, traffic_volumes_key,
                       long(record_opening_time.strftime("%Y%m%d")), long(record_opening_time.strftime("%H"))]

    return pdp_record_item, cdr_traffic_volumes_array, camel_information_pdp_item, cdr_rec_ext_array

def parse_ericsson_file(hdfs_file_name, content, iteration_id):

    print "$$$$$$$$$ Inside parsing hdfs_file_name $$$$$$$$$$$$$$$$$"
    ericsson_file = StringIO.StringIO()
    ericsson_file.write(content)

    ericsson_file.seek(0)

    # Getting file name details to append to records
    _absoluteFileName = hdfs_file_name.rpartition(':')[2]
    file_name = _absoluteFileName.rpartition('/')[2]
    filePath = _absoluteFileName.rpartition('/')[0]

    print "####### main path:" + hdfs_file_name

    check_sum = get_file_check_sum(str(hdfs_file_name))
    #check_sum = 123478987654321
    file_id = str(uuid.uuid4())
    chunk_size = ERICSSON_FILE_CHUNK_SIZE

    bytes_already_read = 0
    k = 0

    total_count = 0
    pdp_base_total_count = 0
    pdp_base_success_count = 0
    pdp_base_failed_count = 0

    pdp_traffic_count = 0
    pdp_camel_count = 0
    pdp_rec_ext_count = 0

    smo_base_total_count = 0
    smo_base_success_count = 0
    smo_base_failed_count = 0

    smo_camel_count = 0

    smt_base_total_count = 0
    smt_base_success_count = 0
    smt_base_failed_count = 0

    sys.tracebacklimit = 0
    start_time_for_iteration = time.time()

    file_size = len(content)
    # We read the entire file in smaller chunks. The chunk most likely will not end with completed record.
    # We initialize the bytes to read with a specific configured value from constants file
    bytes_to_read = chunk_size
    print "File size:" + str(file_size)
    cdr_parsed = []
    cdr_pdp_array = []
    cdr_smo_array = []
    cdr_smt_array = []
    cdr_pdp_traffic_volume_array = []
    cdr_pdp_camel_pdp_array = []
    cdr_pdp_rec_extension_array = []
    cdr_smo_camel_sms_array = []


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
                ab, data = decoder.decode(data, asn1Spec=CallEventRecord())
                total_count +=1
                pdp_record = ab.getComponentByName("sgsnPDPRecord")
                smo_record = ab.getComponentByName("sgsnSMORecord")
                smt_record = ab.getComponentByName("sgsnSMTRecord")

                if pdp_record is not None:
                    pdp_base_total_count += 1
                    pdp_item, traffic_volumes_array,camel_info_pdp_item, rec_ext_item = process_pdp_records(pdp_record, file_id)
                    cdr_pdp_array.append(pdp_item)

                    if traffic_volumes_array is not None and len(traffic_volumes_array) > 0 :

                        for idx in range(len(traffic_volumes_array)):
                            cdr_pdp_traffic_volume_array.append(traffic_volumes_array[idx])

                    if camel_info_pdp_item is not None and len(camel_info_pdp_item) > 0:
                        cdr_pdp_camel_pdp_array.append(camel_info_pdp_item)

                    if rec_ext_item is not None and len(rec_ext_item) > 0:
                        cdr_pdp_rec_extension_array.append(rec_ext_item)

                elif smo_record is not None:
                    smo_base_total_count += 1
                    smo_item = process_smo_records(smo_record, file_id)
                    cdr_smo_array.append(smo_item)
                elif smt_record is not None:
                    smt_base_total_count += 1
                    smt_item = process_smt_records(smt_record, file_id)
                    cdr_smt_array.append(smt_item)


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

    pdp_base_success_count = len(cdr_pdp_array)
    pdp_traffic_count = len(cdr_pdp_traffic_volume_array)
    pdp_camel_count = len(cdr_pdp_camel_pdp_array)
    pdp_rec_ext_count = len(cdr_pdp_rec_extension_array)
    smo_base_success_count = len(cdr_smo_array)
    smo_camel_count = len(cdr_smo_camel_sms_array)
    smt_base_success_count = len(cdr_smt_array)

    total_count = pdp_base_total_count + smo_base_total_count + smt_base_total_count

    success_count = pdp_base_success_count + smo_base_success_count + smt_base_success_count

    fail_count = pdp_base_failed_count + smo_base_failed_count + smt_base_failed_count

    ericsson_sgsn_stats = EricssonStats(iteration_id ,file_id, file_name,check_sum ,
                                        total_count,success_count,fail_count,
                                        pdp_base_total_count, pdp_base_success_count,pdp_base_failed_count,
                                        pdp_traffic_count, pdp_camel_count,pdp_rec_ext_count,
                                        smo_base_total_count, smo_base_success_count,smo_base_failed_count,
                                        smo_camel_count,
                                        smt_base_total_count, smt_base_success_count,smt_base_failed_count)

    cdr_parsed.append(EricssonParsedRecord(file_name, ericsson_sgsn_stats, cdr_pdp_array, cdr_pdp_traffic_volume_array,
                                           cdr_pdp_camel_pdp_array, cdr_pdp_rec_extension_array,
                                           cdr_smo_array, cdr_smo_camel_sms_array, cdr_smt_array))
    ericsson_file.close()

    print "size of cdr pdpd array :" + str(len(cdr_pdp_array))
    return cdr_parsed
