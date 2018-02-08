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


class ProcessZTEumtsSSActionRecord:

    def __init__(self):
        pass

    @staticmethod
    def process_ssaction_records(ssaction_record, file_id, file_name):

        if ssaction_record.getComponentByName("callReference") is None:
            call_reference = ""
        else:
            call_reference = binascii.hexlify(str(ssaction_record.getComponentByName("callReference")))

        if ssaction_record.getComponentByName("exchangeIdentity") is None:
            exchange_identity = ""
        else:
            exchange_identity = str(ssaction_record.getComponentByName("exchangeIdentity"))

        if ssaction_record.getComponentByName("forwardedSubNumber") is None:
            forwarded_subnumber = ""
        else:
            forwarded_subnumber = str(ssaction_record.getComponentByName("forwardedSubNumber"))

        if ssaction_record.getComponentByName("globalCallReference") is None:
            global_call_reference = ""
        else:
            global_call_reference = str(ssaction_record.getComponentByName("globalCallReference"))

        if ssaction_record.getComponentByName("hotBillingTag2") is None:
            hot_billing_tag2 = -255
        else:
            hot_billing_tag2 = long(str(ssaction_record.getComponentByName("hotBillingTag2")))

        if ssaction_record.getComponentByName("location") is not None:
            location = ssaction_record.getComponentByName("location")
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

        if ssaction_record.getComponentByName("msClassmark") is None:
            ms_classmark = ""
        else:
            ms_classmark = binascii.hexlify(str(ssaction_record.getComponentByName("msClassmark")))

        if ssaction_record.getComponentByName("mscSPC14") is None:
            mscspc14 = ""
        else:
            mscspc14 = binascii.hexlify(str(ssaction_record.getComponentByName("mscSPC14")))

        if ssaction_record.getComponentByName("mscSPC24") is None:
            mscspc24 = ""
        else:
            mscspc24 = binascii.hexlify(str(ssaction_record.getComponentByName("mscSPC24")))

        if ssaction_record.getComponentByName("noReplyCondTime") is None:
            no_reply_condtime = -255
        else:
            no_reply_condtime = long(str(ssaction_record.getComponentByName("noReplyCondTime")))

        if ssaction_record.getComponentByName("operatorId") is None:
            operator_id = -255
        else:
            operator_id = long(str(ssaction_record.getComponentByName("operatorId")))

        if ssaction_record.getComponentByName("recordSequenceNumber") is None:
            record_sequence_number = ""
        else:
            record_sequence_number = binascii.hexlify(str(ssaction_record.getComponentByName("recordSequenceNumber")))

        if ssaction_record.getComponentByName("recordType") is None:
            record_type = -255
        else:
            record_type = long(str(ssaction_record.getComponentByName("recordType")))

        if ssaction_record.getComponentByName("recordingEntity") is None:
            recording_entity = ""
        else:
            recording_entity_binascii = binascii.hexlify(str(ssaction_record.getComponentByName("recordingEntity")))
            recording_entity_list = []
            recording_entity_data = list(recording_entity_binascii)
            for l in xrange(0, len(recording_entity_data) - 1, 2):
                recording_entity_list.append(recording_entity_data[l+1] + "" + recording_entity_data[l])
            recording_entity_concatenated = ''.join(recording_entity_list)
            recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
            recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        if ssaction_record.getComponentByName("servedIMEI") is None:
            served_imei = ""
        else:
            served_imei_binascii = binascii.hexlify(str(ssaction_record.getComponentByName("servedIMEI")))
            served_imei_list = []
            served_imei_data = list(served_imei_binascii)
            for i in xrange(0, len(served_imei_data) - 1, 2):
                served_imei_list.append(served_imei_data[i + 1] + "" + served_imei_data[i])
            served_imei_concatenated = ''.join(served_imei_list)
            served_imei = served_imei_concatenated.replace("f", "")

        if ssaction_record.getComponentByName("servedIMSI") is None:
            served_imsi = ""
        else:
            served_imsi_binascii = binascii.hexlify(str(ssaction_record.getComponentByName("servedIMSI")))
            served_imsi_list = []
            served_imsi_data = list(served_imsi_binascii)
            for j in xrange(0, len(served_imsi_data) - 1, 2):
                served_imsi_list.append(served_imsi_data[j+1] + "" + served_imsi_data[j])
            served_imsi_concatenated = ''.join(served_imsi_list)
            served_imsi = served_imsi_concatenated.replace("f", "")

        if ssaction_record.getComponentByName("servedMSISDN") is None:
            served_msisdn = ""
        else:
            served_msisdn_binascii = binascii.hexlify(str(ssaction_record.getComponentByName("servedMSISDN")))
            served_msisdn_list = []
            served_msisdn_data = list(served_msisdn_binascii)
            for k in xrange(0, len(served_msisdn_data) - 1, 2):
                served_msisdn_list.append(served_msisdn_data[k+1] + "" + served_msisdn_data[k])
            served_msisdn_concatenated = ''.join(served_msisdn_list)
            served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
            served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        if ssaction_record.getComponentByName("ssAction") is None:
            ssaction = ""
        else:
            ssaction_tmp = long(str(ssaction_record.getComponentByName("ssAction")))
            ssaction = ""
            if ssaction_tmp == 0:
                ssaction = "registration"
            elif ssaction_tmp == 1:
                ssaction = "erasure"
            elif ssaction_tmp == 2:
                ssaction = "activation"
            elif ssaction_tmp == 3:
                ssaction = "deactivation"
            elif ssaction_tmp == 4:
                ssaction = "interrogation"
            elif ssaction_tmp == 5:
                ssaction = "invocation"
            elif ssaction_tmp == 6:
                ssaction = "passwordRegistration"

        if ssaction_record.getComponentByName("ssActionResult") is None:
            ssaction_result = ""
        else:
            ssaction_result = ssaction_record.getComponentByName("ssActionResult").getName()

        ssaction_time_tmp = binascii.hexlify(str(ssaction_record.getComponentByName("ssActionTime")))
        ssaction_time = append_timezone_offset(parseTimestamp(ssaction_time_tmp))

        if ssaction_record.getComponentByName("ssParameters") is None:
            ss_parameters = ""
        else:
            ss_parameters = str(ssaction_record.getComponentByName("ssParameters"))

        if ssaction_record.getComponentByName("supplService") is None:
            suppl_service = ""
        else:
            suppl_service = binascii.hexlify(str(ssaction_record.getComponentByName("supplService")))

        if ssaction_record.getComponentByName("systemType") is None:
            system_type = ""
        else:
            system_type_tmp = long(str(ssaction_record.getComponentByName("systemType")))
            system_type = ""
            if system_type_tmp == 0:
                system_type = "unknown"
            elif system_type_tmp == 1:
                system_type = "iuUTRAN"
            elif system_type_tmp == 2:
                system_type = "gERAN"

        if ssaction_record.getComponentByName("transactionIdentification") is None:
            transaction_identification = ""
        else:
            transaction_identification = str(ssaction_record.getComponentByName("transactionIdentification"))

        ssaction_record = [call_reference, exchange_identity, file_id, forwarded_subnumber, global_call_reference,
                           hot_billing_tag2, location_cellid, location_area_code, location_plmn, location_sac,
                           ms_classmark, mscspc14, mscspc24, no_reply_condtime, operator_id,
                           record_sequence_number, record_type, recording_entity, served_imei, served_imsi,
                           served_msisdn, ssaction, ssaction_result, ssaction_time, ss_parameters, suppl_service,
                           system_type, transaction_identification, long(ssaction_time.strftime("%Y%m%d")),
                           long(ssaction_time.strftime("%H"))]

        return ssaction_record


