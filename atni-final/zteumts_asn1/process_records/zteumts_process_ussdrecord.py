import binascii

from common.utilities import parseTimestamp, append_timezone_offset


def parseMcc(data):
    mcc = ""
    if len(data) == 6:
        mcc = data[1:2] + data[0:1] + data[3:4]
        mcc = mcc.replace("f", "")

    return mcc


def parseMnc(data):
    mnc = ""
    if len(data) == 6:
        mnc = data[5:6] + data[4:5] + data[2:3]
        mnc = mnc.replace("f", "")

    return mnc


class ProcessZTEumtsUSSDRecord:

    def __init__(self):
        pass

    @staticmethod
    def process_ussd_records(ussd_record, file_id, file_name):

        if ussd_record.getComponentByName("callReference") is None:
            call_reference = ""
        else:
            call_reference = binascii.hexlify(str(ussd_record.getComponentByName("callReference")))

        end_time_tmp = binascii.hexlify(str(ussd_record.getComponentByName("endTime")))
        end_time = append_timezone_offset(parseTimestamp(end_time_tmp))

        if ussd_record.getComponentByName("errorCode") is None:
            error_code = ""
        else:
            error_code = ussd_record.getComponentByName("errorCode").getName()

        if ussd_record.getComponentByName("exchangeIdentity") is None:
            exchange_identity = ""
        else:
            exchange_identity = str(ussd_record.getComponentByName("exchangeIdentity"))

        if ussd_record.getComponentByName("hotBillingTag") is None:
            hot_billing_tag = -255
        else:
            hot_billing_tag = long(str(ussd_record.getComponentByName("hotBillingTag")))

        if ussd_record.getComponentByName("hotBillingTag2") is None:
            hot_billing_tag2 = -255
        else:
            hot_billing_tag2 = long(str(ussd_record.getComponentByName("hotBillingTag2")))

        if ussd_record.getComponentByName("location") is not None:
            location = ussd_record.getComponentByName("location")
            location_area = binascii.hexlify(str(location.getComponentByName("locationArea")))
            location_area_str = str(location_area)

            location_cellid_binascii = location_area_str[10:14]
            format_location_cellid_binascii = int(location_cellid_binascii, 16)
            location_cellid = format(format_location_cellid_binascii, '05')

            location_area_code_binascii = location_area_str[6:10]
            format_location_area_code_binascii = int(location_area_code_binascii, 16)
            location_area_code = format(format_location_area_code_binascii, '05')

            location_plmn = parseMcc(location_area_str[0:6]) + parseMnc(location_area_str[0:6])

            location_sac = location_area_str[-4:]
        else:
            location_cellid = ""
            location_area_code = ""
            location_plmn = ""
            location_sac = ""

        if ussd_record.getComponentByName("msClassmark") is None:
            ms_classmark = ""
        else:
            ms_classmark = binascii.hexlify(str(ussd_record.getComponentByName("msClassmark")))

        if ussd_record.getComponentByName("operatorId") is None:
            operator_id = -255
        else:
            operator_id = long(str(ussd_record.getComponentByName("operatorId")))

        if ussd_record.getComponentByName("recordSequenceNumber") is None:
            record_sequence_number = ""
        else:
            record_sequence_number = binascii.hexlify(str(ussd_record.getComponentByName("recordSequenceNumber")))

        if ussd_record.getComponentByName("recordType") is None:
            record_type = -255
        else:
            record_type = long(str(ussd_record.getComponentByName("recordType")))

        if ussd_record.getComponentByName("recordingEntity") is None:
            recording_entity = ""
        else:
            recording_entity_binascii = binascii.hexlify(str(ussd_record.getComponentByName("recordingEntity")))
            recording_entity_list = []
            recording_entity_data = list(recording_entity_binascii)
            for l in xrange(0, len(recording_entity_data) - 1, 2):
                recording_entity_list.append(recording_entity_data[l+1] + "" + recording_entity_data[l])
            recording_entity_concatenated = ''.join(recording_entity_list)
            recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
            recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        if ussd_record.getComponentByName("servedIMEI") is None:
            served_imei = ""
        else:
            served_imei_binascii = binascii.hexlify(str(ussd_record.getComponentByName("servedIMEI")))
            served_imei_list = []
            served_imei_data = list(served_imei_binascii)
            for i in xrange(0, len(served_imei_data) - 1, 2):
                served_imei_list.append(served_imei_data[i + 1] + "" + served_imei_data[i])
            served_imei_concatenated = ''.join(served_imei_list)
            served_imei = served_imei_concatenated.replace("f", "")

        if ussd_record.getComponentByName("servedIMSI") is None:
            served_imsi = ""
        else:
            served_imsi_binascii = binascii.hexlify(str(ussd_record.getComponentByName("servedIMSI")))
            served_imsi_list = []
            served_imsi_data = list(served_imsi_binascii)
            for j in xrange(0, len(served_imsi_data) - 1, 2):
                served_imsi_list.append(served_imsi_data[j+1] + "" + served_imsi_data[j])
            served_imsi_concatenated = ''.join(served_imsi_list)
            served_imsi = served_imsi_concatenated.replace("f", "")

        if ussd_record.getComponentByName("servedMSISDN") is None:
            served_msisdn = ""
        else:
            served_msisdn_binascii = binascii.hexlify(str(ussd_record.getComponentByName("servedMSISDN")))
            served_msisdn_list = []
            served_msisdn_data = list(served_msisdn_binascii)
            for k in xrange(0, len(served_msisdn_data) - 1, 2):
                served_msisdn_list.append(served_msisdn_data[k+1] + "" + served_msisdn_data[k])
            served_msisdn_concatenated = ''.join(served_msisdn_list)
            served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
            served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        start_time_tmp = binascii.hexlify(str(ussd_record.getComponentByName("startTime")))
        start_time = append_timezone_offset(parseTimestamp(start_time_tmp))

        if ussd_record.getComponentByName("systemType") is None:
            system_type = ""
        else:
            system_type_tmp = long(str(ussd_record.getComponentByName("systemType")))
            system_type = ""
            if system_type_tmp == 0:
                system_type = "unknown"
            elif system_type_tmp == 1:
                system_type = "iuUTRAN"
            elif system_type_tmp == 2:
                system_type = "gERAN"

        if ussd_record.getComponentByName("uSSDDataCodingScheme") is None:
            ussd_datacoding_scheme = ""
        else:
            ussd_datacoding_scheme = binascii.hexlify(str(ussd_record.getComponentByName("uSSDDataCodingScheme")))

        if ussd_record.getComponentByName("uSSDInteractionCount") is None:
            ussd_interaction_code = -255
        else:
            ussd_interaction_code = long(str(ussd_record.getComponentByName("uSSDInteractionCount")))

        if ussd_record.getComponentByName("uSSDOperationCode") is None:
            ussd_operation_code = ""
        else:
            ussd_operation_code_tmp = long(str(ussd_record.getComponentByName("uSSDOperationCode")))
            ussd_operation_code = ""
            if ussd_operation_code_tmp == 19:
                ussd_operation_code = "processUnstructuredSS-Data"
            elif ussd_operation_code_tmp == 59:
                ussd_operation_code = "processUnstructuredSS-Request"
            elif ussd_operation_code_tmp == 60:
                ussd_operation_code = "unstructuredSS-Request"
            elif ussd_operation_code_tmp == 61:
                ussd_operation_code = "unstructuredSS-Notify"

        if ussd_record.getComponentByName("uSSDServiceCode") is None:
            ussd_service_code = -255
        else:
            ussd_service_code = long(str(ussd_record.getComponentByName("uSSDServiceCode")))

        if ussd_record.getComponentByName("uSSDUnstructuredData") is None:
            ussd_unstructured_data = ""
        else:
            ussd_unstructured_data = binascii.hexlify(str(ussd_record.getComponentByName("uSSDUnstructuredData")))

        ussd_record = [call_reference, end_time, error_code, exchange_identity, file_id, hot_billing_tag,
                       hot_billing_tag2, location_cellid, location_area_code, location_plmn,
                       location_sac, ms_classmark, operator_id, record_sequence_number,
                       record_type, recording_entity, served_imei, served_imsi, served_msisdn, start_time, system_type,
                       ussd_datacoding_scheme, ussd_interaction_code, ussd_operation_code, ussd_service_code,
                       ussd_unstructured_data, long(start_time.strftime("%Y%m%d")),
                       long(start_time.strftime("%H"))]

        return ussd_record

