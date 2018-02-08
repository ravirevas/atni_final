import binascii

from common.constants import *

from common.utilities import parseTimestamp


class ProcessZTEumtsTCICPHRecord:
    def __init__(self):
        pass

    @staticmethod
    def process_tcicph_records(tcicph_record, file_name):

        answer_time_tmp = binascii.hexlify(str(tcicph_record.getComponentByName("answerTime")))
        answer_time = parseTimestamp(answer_time_tmp)

        if tcicph_record.getComponentByName("callDuration") is None:
            call_duration = 0
        else:
            call_duration = long(str(tcicph_record.getComponentByName("callDuration")))

        called_number_binascii = binascii.hexlify(str(tcicph_record.getComponentByName("calledNumber")))
        called_number_list = []
        called_number_data = list(called_number_binascii)
        for i in xrange(0, len(called_number_data) - 1, 2):
            called_number_list.append(called_number_data[i + 1] + "" + called_number_data[i])
        called_number_concatenated = ''.join(called_number_list)
        called_number = called_number_concatenated[2:]

        calling_number_binascii = binascii.hexlify(
                str(tcicph_record.getComponentByName("callingNumber")))
        calling_number_list = []
        calling_number_data = list(calling_number_binascii)
        for i in xrange(0, len(calling_number_data) - 1, 2):
            calling_number_list.append(calling_number_data[i + 1] + "" + calling_number_data[i])
        calling_number_concatenated = ''.join(calling_number_list)
        calling_number = calling_number_concatenated[2:]

        call_reference = binascii.hexlify(str(tcicph_record.getComponentByName("callReference")))

        call_segment_id = long(str(tcicph_record.getComponentByName("callSegmentId")))

        if tcicph_record.getComponentByName("causeForTerm") is None:
            cause_for_termination = ""
        else:
            cause_for_termination_tmp = long(str(tcicph_record.getComponentByName("causeForTerm")))
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

        if tcicph_record.getComponentByName("defaultCallHandling") is None:
            default_call_handling = ""
        else:
            default_call_handling_tmp = long(str(tcicph_record.getComponentByName("defaultCallHandling")))
            default_call_handling = ""
            if default_call_handling_tmp == 0:
                default_call_handling = "continueCall"
            elif default_call_handling_tmp == 1:
                default_call_handling = "releaseCall"

        if tcicph_record.getComponentByName("diagnostics") is None:
            diagnostics = ""
        else:
            diagnostics = str(tcicph_record.getComponentByName("diagnostics"))

        file_name = file_name

        free_format_data = str(tcicph_record.getComponentByName("freeFormatData"))

        free_format_data_append = str(tcicph_record.getComponentByName("freeFormatDataAppend"))

        global_call_reference = str(tcicph_record.getComponentByName("globalCallReference"))

        if tcicph_record.getComponentByName("gsm-SCFAddress") is not None:
            gsm_scfaddress_binascii = binascii.hexlify(str(tcicph_record.getComponentByName("gsm-SCFAddress")))
            gsm_scfaddress_list = []
            gsm_scfaddress_data = list(gsm_scfaddress_binascii)
            for i in xrange(0, len(gsm_scfaddress_data) - 1, 2):
                gsm_scfaddress_list.append(gsm_scfaddress_data[i + 1] + "" + gsm_scfaddress_data[i])
                gsm_scfaddress_concatenated = ''.join(gsm_scfaddress_list)
                gsm_scfaddress = gsm_scfaddress_concatenated[2:].replace("f", "")
        else:
            gsm_scfaddress = ""

        interrogation_time_tmp = binascii.hexlify(str(tcicph_record.getComponentByName("interrogationTime")))
        interrogation_time = parseTimestamp(interrogation_time_tmp)

        last_long_part_ind = str(tcicph_record.getComponentByName("lastLongPartInd"))

        level_of_camel_service = str(tcicph_record.getComponentByName("levelOfCAMELService"))

        if tcicph_record.getComponentByName("millisecDuration") is None:
            milli_sec_duration = 0
        else:
            milli_sec_duration = long(str(tcicph_record.getComponentByName("millisecDuration")))

        msc_address = str(tcicph_record.getComponentByName("mSCAddress"))

        msc_incoming_tkgp = binascii.hexlify(str(tcicph_record.getComponentByName("mscIncomingTKGP")))

        if tcicph_record.getComponentByName("networkCallReference") is None:
            network_call_reference = ""
        else:
            network_call_reference = binascii.hexlify(str(tcicph_record.getComponentByName("networkCallReference")))

        number_of_dp_encountered = str(tcicph_record.getComponentByName("numberOfDPEncountered"))

        if tcicph_record.getComponentByName("partialRecordType") is None:
            partial_record_type = ""
        else:
            partial_record_type_tmp = long(str(tcicph_record.getComponentByName("partialRecordType")))
            partial_record_type = ""
            if partial_record_type_tmp == 1:
                partial_record_type = "serviceChange"

        if tcicph_record.getComponentByName("partSequenceNumber") is None:
            part_sequence_number = 0
        else:
            part_sequence_number = long(str(tcicph_record.getComponentByName("partSequenceNumber")))

        recording_entity_binascii = binascii.hexlify(str(tcicph_record.getComponentByName("recordingEntity")))
        recording_entity_list = []
        recording_entity_data = list(recording_entity_binascii)
        for l in xrange(0, len(recording_entity_data) - 1, 2):
            recording_entity_list.append(recording_entity_data[l + 1] + "" + recording_entity_data[l])
        recording_entity_concatenated = ''.join(recording_entity_list)
        recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
        recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        record_sequence_number = binascii.hexlify(str(tcicph_record.getComponentByName("recordSequenceNumber")))

        if tcicph_record.getComponentByName("recordType") is None:
            record_type = 0
        else:
            record_type = long(str(tcicph_record.getComponentByName("recordType")))

        release_time_tmp = binascii.hexlify(str(tcicph_record.getComponentByName("releaseTime")))
        release_time = parseTimestamp(release_time_tmp)

        seizure_time_tmp = binascii.hexlify(str(tcicph_record.getComponentByName("seizureTime")))
        seizure_time = parseTimestamp(seizure_time_tmp)

        served_imsi_binascii = binascii.hexlify(str(tcicph_record.getComponentByName("servedIMSI")))
        served_imsi_list = []
        served_imsi_data = list(served_imsi_binascii)
        for j in xrange(0, len(served_imsi_data) - 1, 2):
            served_imsi_list.append(served_imsi_data[j + 1] + "" + served_imsi_data[j])
        served_imsi_concatenated = ''.join(served_imsi_list)
        served_imsi = served_imsi_concatenated.replace("f", "")

        served_msisdn_binascii = binascii.hexlify(str(tcicph_record.getComponentByName("servedMSISDN")))
        served_msisdn_list = []
        served_msisdn_data = list(served_msisdn_binascii)
        for k in xrange(0, len(served_msisdn_data) - 1, 2):
            served_msisdn_list.append(served_msisdn_data[k + 1] + "" + served_msisdn_data[k])
        served_msisdn_concatenated = ''.join(served_msisdn_list)
        served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
        served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        if tcicph_record.getComponentByName("serviceKey") is None:
            service_key = ""
        else:
            service_key = str(tcicph_record.getComponentByName("serviceKey"))

        if tcicph_record.getComponentByName("termCallType") is None:
            term_call_type = ""
        else:
            term_call_type_tmp = long(str(tcicph_record.getComponentByName("termCallType")))
            term_call_type = ""
            if term_call_type_tmp == 0:
                term_call_type = "terminatingCAMELCall"
            elif term_call_type_tmp == 1:
                term_call_type = "visitedTerminatingCAMELCall"

        tcicph_record = [answer_time, call_duration, called_number, calling_number, call_reference, call_segment_id,
                         cause_for_termination, default_call_handling, diagnostics, file_name, free_format_data,
                         free_format_data_append, global_call_reference, gsm_scfaddress, interrogation_time,
                         last_long_part_ind, level_of_camel_service, milli_sec_duration, msc_address,
                         msc_incoming_tkgp, network_call_reference, number_of_dp_encountered, partial_record_type,
                         part_sequence_number, recording_entity, record_sequence_number, record_type, release_time,
                         seizure_time, served_imsi, served_msisdn, service_key, term_call_type]
        return tcicph_record
