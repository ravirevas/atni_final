import binascii

from common.constants import *

from common.utilities import parseTimestamp


class ProcessZTEumtsMTRFRecord:
    def __init__(self):
        pass

    @staticmethod
    def process_mtrf_records(mtrf_record, file_name):

        answer_time_tmp = binascii.hexlify(str(mtrf_record.getComponentByName("answerTime")))
        answer_time = parseTimestamp(answer_time_tmp)

        if mtrf_record.getComponentByName("basicService") is None:
            basic_service = ""
        else:
            basic_service_choice = mtrf_record.getComponentByName("basicService")
            basic_service_choice_hexvalue = basic_service_choice.getComponent().prettyPrint()
            if basic_service_choice_hexvalue == "0x11":
                basic_service = "teleservice"
            else:
                basic_service = "bearerservice"

        if mtrf_record.getComponentByName("callDuration") is None:
            call_duration = 0
        else:
            call_duration = long(str(mtrf_record.getComponentByName("callDuration")))

        calling_number_binascii = binascii.hexlify(
                str(mtrf_record.getComponentByName("callingNumber")))
        calling_number_list = []
        calling_number_data = list(calling_number_binascii)
        for i in xrange(0, len(calling_number_data) - 1, 2):
            calling_number_list.append(calling_number_data[i + 1] + "" + calling_number_data[i])
        calling_number_concatenated = ''.join(calling_number_list)
        calling_number = calling_number_concatenated[2:]

        call_reference = binascii.hexlify(str(mtrf_record.getComponentByName("callReference")))

        if mtrf_record.getComponentByName("causeForTerm") is None:
            cause_for_termination = ""
        else:
            cause_for_termination_tmp = long(str(mtrf_record.getComponentByName("causeForTerm")))
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

        if mtrf_record.getComponentByName("diagnostics") is None:
            diagnostics = ""
        else:
            diagnostics = str(mtrf_record.getComponentByName("diagnostics"))

        exchange_identity = str(mtrf_record.getComponentByName("exchangeIdentity"))

        file_name = file_name

        last_long_part_ind = str(mtrf_record.getComponentByName("lastLongPartInd"))

        if mtrf_record.getComponentByName("mscIncomingTKGP") is not None:
            msc_incoming_tkgp = binascii.hexlify(str(mtrf_record.getComponentByName("mscIncomingTKGP")))
        else:
            msc_incoming_tkgp = ""

        if mtrf_record.getComponentByName("mscOutgoingTKGP") is not None:
            msc_outgoing_tkgp = binascii.hexlify(str(mtrf_record.getComponentByName("mscOutgoingTKGP")))
        else:
            msc_outgoing_tkgp = ""

        if mtrf_record.getComponentByName("partialRecordType") is None:
            partial_record_type = ""
        else:
            partial_record_type_tmp = long(str(mtrf_record.getComponentByName("partialRecordType")))
            partial_record_type = ""
            if partial_record_type_tmp == 1:
                partial_record_type = "serviceChange"

        recording_entity_binascii = binascii.hexlify(str(mtrf_record.getComponentByName("recordingEntity")))
        recording_entity_list = []
        recording_entity_data = list(recording_entity_binascii)
        for l in xrange(0, len(recording_entity_data) - 1, 2):
            recording_entity_list.append(recording_entity_data[l + 1] + "" + recording_entity_data[l])
        recording_entity_concatenated = ''.join(recording_entity_list)
        recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
        recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        record_sequence_number = binascii.hexlify(str(mtrf_record.getComponentByName("recordSequenceNumber")))

        if mtrf_record.getComponentByName("recordType") is None:
            record_type = 0
        else:
            record_type = long(str(mtrf_record.getComponentByName("recordType")))

        release_time_tmp = binascii.hexlify(str(mtrf_record.getComponentByName("releaseTime")))
        release_time = parseTimestamp(release_time_tmp)

        roaming_number_binascii = binascii.hexlify(str(mtrf_record.getComponentByName("roamingNumber")))
        roaming_number_list = []
        roaming_number_data = list(roaming_number_binascii)
        for i in xrange(0, len(roaming_number_data) - 1, 2):
            roaming_number_list.append(roaming_number_data[i + 1] + "" + roaming_number_data[i])
        roaming_number_concatenated = ''.join(roaming_number_list)
        roaming_number = roaming_number_concatenated[2:]

        seizure_time_tmp = binascii.hexlify(str(mtrf_record.getComponentByName("seizureTime")))
        seizure_time = parseTimestamp(seizure_time_tmp)

        if mtrf_record.getComponentByName("sequenceNumber") is None:
            sequence_number = 0
        else:
            sequence_number = long(str(mtrf_record.getComponentByName("sequenceNumber")))

        served_imei_binascii = binascii.hexlify(str(mtrf_record.getComponentByName("servedIMEI")))
        served_imei_list = []
        served_imei_data = list(served_imei_binascii)
        for i in xrange(0, len(served_imei_data) - 1, 2):
            served_imei_list.append(served_imei_data[i+1] + "" + served_imei_data[i])
        served_imei_concatenated = ''.join(served_imei_list)
        served_imei = served_imei_concatenated.replace("f", "")

        served_imsi_binascii = binascii.hexlify(str(mtrf_record.getComponentByName("servedIMSI")))
        served_imsi_list = []
        served_imsi_data = list(served_imsi_binascii)
        for j in xrange(0, len(served_imsi_data) - 1, 2):
            served_imsi_list.append(served_imsi_data[j+1] + "" + served_imsi_data[j])
        served_imsi_concatenated = ''.join(served_imsi_list)
        served_imsi = served_imsi_concatenated.replace("f", "")

        served_msisdn_binascii = binascii.hexlify(str(mtrf_record.getComponentByName("servedMSISDN")))
        served_msisdn_list = []
        served_msisdn_data = list(served_msisdn_binascii)
        for k in xrange(0, len(served_msisdn_data) - 1, 2):
            served_msisdn_list.append(served_msisdn_data[k+1] + "" + served_msisdn_data[k])
        served_msisdn_concatenated = ''.join(served_msisdn_list)
        served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
        served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        mtrf_record = [answer_time, basic_service, call_duration, calling_number, call_reference, cause_for_termination,
                       diagnostics, exchange_identity, file_name, last_long_part_ind, msc_incoming_tkgp,
                       msc_outgoing_tkgp,   partial_record_type, recording_entity, record_sequence_number,
                       record_type, release_time, roaming_number, seizure_time, sequence_number, served_imei,
                       served_imsi, served_msisdn]

        return mtrf_record

