import binascii

from common.constants import *

from common.utilities import parseTimestamp


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


class ProcessZTEumtsMSCSRVCCRecord:
    def __init__(self):
        pass

    @staticmethod
    def process_mscsrvcc_records(mscsrvcc_record, file_name):

        answer_time_tmp = binascii.hexlify(str(mscsrvcc_record.getComponentByName("answerTime")))
        answer_time = parseTimestamp(answer_time_tmp)

        if mscsrvcc_record.getComponentByName("basicService") is None:
            basic_service = ""
        else:
            basic_service_choice = mscsrvcc_record.getComponentByName("basicService")
            basic_service_choice_hexvalue = basic_service_choice.getComponent().prettyPrint()
            if basic_service_choice_hexvalue == "0x11":
                basic_service = "teleservice"
            else:
                basic_service = "bearerservice"

        if mscsrvcc_record.getComponentByName("callDuration") is None:
            call_duration = 0
        else:
            call_duration = long(str(mscsrvcc_record.getComponentByName("callDuration")))

        called_number_binascii = binascii.hexlify(str(mscsrvcc_record.getComponentByName("calledNumber")))
        called_number_list = []
        called_number_data = list(called_number_binascii)
        for i in xrange(0, len(called_number_data) - 1, 2):
            called_number_list.append(called_number_data[i + 1] + "" + called_number_data[i])
        called_number_concatenated = ''.join(called_number_list)
        called_number = called_number_concatenated[2:]

        call_reference = binascii.hexlify(str(mscsrvcc_record.getComponentByName("callReference")))

        if mscsrvcc_record.getComponentByName("causeForTerm") is None:
            cause_for_termination = ""
        else:
            cause_for_termination_tmp = long(str(mscsrvcc_record.getComponentByName("causeForTerm")))
            cause_for_termination = ""
            if cause_for_termination_tmp == 0:
                cause_for_termination = "normalRelease"
            elif cause_for_termination_tmp == 1:
                cause_for_termination = "partialRecord"
            elif cause_for_termination_tmp == 2:
                cause_for_termination = "partialRecordCallReestablishment"
            elif cause_for_termination_tmp == 3:
                cause_for_termination = "unsuccessfulCallAttempt"
            elif cause_for_termination_tmp == 4:
                cause_for_termination = "stableCallAbnormalTermination"
            elif cause_for_termination_tmp == 5:
                cause_for_termination = "cAMELInitCallRelease"
            elif cause_for_termination_tmp == 52:
                cause_for_termination = "unauthorizedRequestingNetwork"
            elif cause_for_termination_tmp == 53:
                cause_for_termination = "unauthorizedLCSClient"
            elif cause_for_termination_tmp == 54:
                cause_for_termination = "positionMethodFailure"
            elif cause_for_termination_tmp == 58:
                cause_for_termination = "unknownOrUnreachableLCSClient"
            elif cause_for_termination_tmp == 101:
                cause_for_termination = "cAMELPlayTone"
            elif cause_for_termination_tmp == 102:
                cause_for_termination = "changeOfConfDueToCPH"
            elif cause_for_termination_tmp == 103:
                cause_for_termination = "falseAnswerCharge"
            elif cause_for_termination_tmp == 104:
                cause_for_termination = "failPlayTone"
            elif cause_for_termination_tmp == 105:
                cause_for_termination = "releaseForPreemption"

        if mscsrvcc_record.getComponentByName("diagnostics") is None:
            diagnostics = ""
        else:
            diagnostics = str(mscsrvcc_record.getComponentByName("diagnostics"))

        exchange_identity = str(mscsrvcc_record.getComponentByName("exchangeIdentity"))

        file_name = file_name

        ics_i2_active_flag = str(mscsrvcc_record.getComponentByName("iCSI2ActiveFlag"))

        ims_charging_identifier = str(mscsrvcc_record.getComponentByName("iMS-Charging-Identifier"))

        is_emergency_call = str(mscsrvcc_record.getComponentByName("isEmergencyCall"))

        last_long_part_ind = str(mscsrvcc_record.getComponentByName("lastLongPartInd"))

        if mscsrvcc_record.getComponentByName("location") is not None:
            location = mscsrvcc_record.getComponentByName("location")
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

        if mscsrvcc_record.getComponentByName("millisecDuration") is None:
            milli_sec_duration = 0
        else:
            milli_sec_duration = long(str(mscsrvcc_record.getComponentByName("millisecDuration")))

        ms_classmark = binascii.hexlify(str(mscsrvcc_record.getComponentByName("msClassmark")))

        if mscsrvcc_record.getComponentByName("mscOutgoingTKGP") is None:
            msc_outgoing_tkgp = 0
        else:
            msc_outgoing_tkgp = binascii.hexlify(str(mscsrvcc_record.getComponentByName("mscOutgoingTKGP")))

        if mscsrvcc_record.getComponentByName("partialRecordType") is None:
            partial_record_type = ""
        else:
            partial_record_type_tmp = long(str(mscsrvcc_record.getComponentByName("partialRecordType")))
            partial_record_type = ""
            if partial_record_type_tmp == 1:
                partial_record_type = "serviceChange"

        recording_entity_binascii = binascii.hexlify(str(mscsrvcc_record.getComponentByName("recordingEntity")))
        recording_entity_list = []
        recording_entity_data = list(recording_entity_binascii)
        for l in xrange(0, len(recording_entity_data) - 1, 2):
            recording_entity_list.append(recording_entity_data[l + 1] + "" + recording_entity_data[l])
        recording_entity_concatenated = ''.join(recording_entity_list)
        recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
        recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        record_sequence_number = binascii.hexlify(str(mscsrvcc_record.getComponentByName("recordSequenceNumber")))

        if mscsrvcc_record.getComponentByName("recordType") is None:
            record_type = 0
        else:
            record_type = long(str(mscsrvcc_record.getComponentByName("recordType")))

        related_icid = str(mscsrvcc_record.getComponentByName("relatedICID"))

        release_time_tmp = binascii.hexlify(str(mscsrvcc_record.getComponentByName("releaseTime")))
        release_time = parseTimestamp(release_time_tmp)

        seizure_time_tmp = binascii.hexlify(str(mscsrvcc_record.getComponentByName("seizureTime")))
        seizure_time = parseTimestamp(seizure_time_tmp)

        if mscsrvcc_record.getComponentByName("sequenceNumber") is None:
            sequence_number = ""
        else:
            sequence_number = long(str(mscsrvcc_record.getComponentByName("sequenceNumber")))

        served_imei_binascii = binascii.hexlify(str(mscsrvcc_record.getComponentByName("servedIMEI")))
        served_imei_list = []
        served_imei_data = list(served_imei_binascii)
        for i in xrange(0, len(served_imei_data) - 1, 2):
            served_imei_list.append(served_imei_data[i+1] + "" + served_imei_data[i])
        served_imei_concatenated = ''.join(served_imei_list)
        served_imei = served_imei_concatenated.replace("f", "")

        served_imsi_binascii = binascii.hexlify(str(mscsrvcc_record.getComponentByName("servedIMSI")))
        served_imsi_list = []
        served_imsi_data = list(served_imsi_binascii)
        for j in xrange(0, len(served_imsi_data) - 1, 2):
            served_imsi_list.append(served_imsi_data[j+1] + "" + served_imsi_data[j])
        served_imsi_concatenated = ''.join(served_imsi_list)
        served_imsi = served_imsi_concatenated.replace("f", "")

        served_msisdn_binascii = binascii.hexlify(str(mscsrvcc_record.getComponentByName("servedMSISDN")))
        served_msisdn_list = []
        served_msisdn_data = list(served_msisdn_binascii)
        for k in xrange(0, len(served_msisdn_data) - 1, 2):
            served_msisdn_list.append(served_msisdn_data[k+1] + "" + served_msisdn_data[k])
        served_msisdn_concatenated = ''.join(served_msisdn_list)
        served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
        served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        if mscsrvcc_record.getComponentByName("systemType") is None:
            system_type = ""
        else:
            system_type_tmp = long(str(mscsrvcc_record.getComponentByName("systemType")))
            system_type = ""
            if system_type_tmp == 0:
                system_type = "unknown"
            elif system_type_tmp == 1:
                system_type = "iuUTRAN"
            elif system_type_tmp == 2:
                system_type = "gERAN"

        if mscsrvcc_record.getComponentByName("transactionIdentification") is None:
            transaction_identification = 0
        else:
            transaction_identification = long(str(mscsrvcc_record.getComponentByName("transactionIdentification")))

        mscsrvcc_record = [answer_time, basic_service, call_duration, called_number, call_reference,
                           cause_for_termination, diagnostics, exchange_identity, file_name, ics_i2_active_flag,
                           ims_charging_identifier, is_emergency_call, last_long_part_ind, location_cellid,
                           location_area_code, location_plmn, location_sac, milli_sec_duration, ms_classmark,
                           msc_outgoing_tkgp, partial_record_type, recording_entity, record_sequence_number, record_type
                           , related_icid, release_time, seizure_time, sequence_number, served_imei, served_imsi,
                           served_msisdn, system_type, transaction_identification]
        return mscsrvcc_record




