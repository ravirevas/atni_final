import binascii

from common.constants import *

from common.utilities import parseTimestamp


class ProcessZTEumtsMCFCPHRecord:

    def __init__(self):
        pass

    @staticmethod
    def process_mcfcph_records(mcfcph_record, file_name):

        additional_chg_info = mcfcph_record.getComponentByName("additionalChgInfo")

        if additional_chg_info is None:
            charge_indicator = ""
        else:
            charge_indicator = str(additional_chg_info.getComponentByName("chargeIndicator"))

        if additional_chg_info is None:
            charged_party = ""
        else:
            charged_party_tmp = long(str(additional_chg_info.getComponentByName("chargedParty")))
            charged_party = ""
            if charged_party_tmp == 0:
                charged_party = "callingAsChargedParty"
            elif charged_party_tmp == 1:
                charged_party = "calledAsChargedParty"
            elif charged_party_tmp == 2:
                charged_party = "noCharging"
            elif charged_party_tmp == 3:
                charged_party = "anotherNumAsChargedParty"

        answer_time_tmp = binascii.hexlify(str(mcfcph_record.getComponentByName("answerTime")))
        answer_time = parseTimestamp(answer_time_tmp)

        if mcfcph_record.getComponentByName("basicService") is None:
            basic_service = ""
        else:
            basic_service_choice = mcfcph_record.getComponentByName("basicService")
            basic_service_choice_hexvalue = basic_service_choice.getComponent().prettyPrint()
            if basic_service_choice_hexvalue == "0x11":
                basic_service = "teleservice"
            else:
                basic_service = "bearerservice"

        if mcfcph_record.getComponentByName("callDuration") is None:
            call_duration = 0
        else:
            call_duration = long(str(mcfcph_record.getComponentByName("callDuration")))

        called_number_binascii = binascii.hexlify(str(mcfcph_record.getComponentByName("calledNumber")))
        called_number_list = []
        called_number_data = list(called_number_binascii)
        for i in xrange(0, len(called_number_data) - 1, 2):
            called_number_list.append(called_number_data[i + 1] + "" + called_number_data[i])
        called_number_concatenated = ''.join(called_number_list)
        called_number = called_number_concatenated[2:]

        calling_number_binascii = binascii.hexlify(
                str(mcfcph_record.getComponentByName("callingNumber")))
        calling_number_list = []
        calling_number_data = list(calling_number_binascii)
        for i in xrange(0, len(calling_number_data) - 1, 2):
            calling_number_list.append(calling_number_data[i + 1] + "" + calling_number_data[i])
        calling_number_concatenated = ''.join(calling_number_list)
        calling_number = calling_number_concatenated[2:]

        call_reference = binascii.hexlify(str(mcfcph_record.getComponentByName("callReference")))

        call_segment_id = long(str(mcfcph_record.getComponentByName("callSegmentId")))

        if mcfcph_record.getComponentByName("cAMELInitCFIndicator") is None:
            camel_initcf_indicator = ""
        else:
            camel_initcf_indicator_tmp = long(str(mcfcph_record.getComponentByName("cAMELInitCFIndicator")))
            camel_initcf_indicator = ""
            if camel_initcf_indicator_tmp == 0:
                camel_initcf_indicator = "noCAMELCallForwarding"
            if camel_initcf_indicator_tmp == 1:
                camel_initcf_indicator = "cAMELCallForwarding"

        if mcfcph_record.getComponentByName("causeForTerm") is None:
            cause_for_termination = ""
        else:
            cause_for_termination_tmp = long(str(mcfcph_record.getComponentByName("causeForTerm")))
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

        if mcfcph_record.getComponentByName("defaultCallHandling") is None:
            default_call_handling = ""
        else:
            default_call_handling_tmp = long(str(mcfcph_record.getComponentByName("defaultCallHandling")))
            default_call_handling = ""
            if default_call_handling_tmp == 0:
                default_call_handling = "continueCall"
            elif default_call_handling_tmp == 1:
                default_call_handling = "releaseCall"

        if mcfcph_record.getComponentByName("diagnostics") is None:
            diagnostics = ""
        else:
            diagnostics = str(mcfcph_record.getComponentByName("diagnostics"))

        file_name = os.path.split(ZTEUMTS_IN_PATH)[1]

        free_format_data = str(mcfcph_record.getComponentByName("freeFormatData"))

        free_format_data_append = str(mcfcph_record.getComponentByName("freeFormatDataAppend"))

        global_call_reference = str(mcfcph_record.getComponentByName("globalCallReference"))

        if mcfcph_record.getComponentByName("gsm-SCFAddress") is not None:
            gsm_scfaddress_binascii = binascii.hexlify(str(mcfcph_record.getComponentByName("gsm-SCFAddress")))
            gsm_scfaddress_list = []
            gsm_scfaddress_data = list(gsm_scfaddress_binascii)
            for i in xrange(0, len(gsm_scfaddress_data) - 1, 2):
                gsm_scfaddress_list.append(gsm_scfaddress_data[i + 1] + "" + gsm_scfaddress_data[i])
                gsm_scfaddress_concatenated = ''.join(gsm_scfaddress_list)
                gsm_scfaddress = gsm_scfaddress_concatenated[2:].replace("f", "")
        else:
            gsm_scfaddress = ""

        if mcfcph_record.getComponentByName("hotBillingTag") is None:
            hot_billing_tag = 0
        else:
            hot_billing_tag = long(str(mcfcph_record.getComponentByName("hotBillingTag")))

        is_camel_call = str(mcfcph_record.getComponentByName("isCAMELCall"))

        last_long_part_ind = str(mcfcph_record.getComponentByName("lastLongPartInd"))

        level_of_camel_service = str(mcfcph_record.getComponentByName("levelOfCAMELService"))

        if mcfcph_record.getComponentByName("mcfType") is None:
            mcf_type = ""
        else:
            mcf_type_tmp = long(str(mcfcph_record.getComponentByName("mcfType")))
            mcf_type = ""
            if mcf_type_tmp == 0:
                mcf_type = "unconditional"
            elif mcf_type_tmp == 1:
                mcf_type = "busy"
            elif mcf_type_tmp == 2:
                mcf_type = "notReply"
            elif mcf_type_tmp == 3:
                mcf_type = "notReachable"
            elif mcf_type_tmp == 254:
                mcf_type = "callBack"

        if mcfcph_record.getComponentByName("millisecDuration") is None:
            milli_sec_duration = 0
        else:
            milli_sec_duration = long(str(mcfcph_record.getComponentByName("millisecDuration")))

        msc_address = str(mcfcph_record.getComponentByName("mSCAddress"))

        msc_incoming_tkgp = binascii.hexlify(str(mcfcph_record.getComponentByName("mscIncomingTKGP")))

        if mcfcph_record.getComponentByName("networkCallReference") is None:
            network_call_reference = ""
        else:
            network_call_reference = binascii.hexlify(str(mcfcph_record.getComponentByName("networkCallReference")))

        number_of_dp_encountered = str(mcfcph_record.getComponentByName("numberOfDPEncountered"))

        if mcfcph_record.getComponentByName("partialRecordType") is None:
            partial_record_type = ""
        else:
            partial_record_type_tmp = long(str(mcfcph_record.getComponentByName("partialRecordType")))
            partial_record_type = ""
            if partial_record_type_tmp == 1:
                partial_record_type = "serviceChange"

        if mcfcph_record.getComponentByName("partSequenceNumber") is None:
            part_sequence_number = 0
        else:
            part_sequence_number = long(str(mcfcph_record.getComponentByName("partSequenceNumber")))

        recording_entity_binascii = binascii.hexlify(str(mcfcph_record.getComponentByName("recordingEntity")))
        recording_entity_list = []
        recording_entity_data = list(recording_entity_binascii)
        for l in xrange(0, len(recording_entity_data) - 1, 2):
            recording_entity_list.append(recording_entity_data[l + 1] + "" + recording_entity_data[l])
        recording_entity_concatenated = ''.join(recording_entity_list)
        recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
        recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        record_sequence_number = binascii.hexlify(str(mcfcph_record.getComponentByName("recordSequenceNumber")))

        if mcfcph_record.getComponentByName("recordType") is None:
            record_type = 0
        else:
            record_type = long(str(mcfcph_record.getComponentByName("recordType")))

        release_time_tmp = binascii.hexlify(str(mcfcph_record.getComponentByName("releaseTime")))
        release_time = parseTimestamp(release_time_tmp)

        seizure_time_tmp = binascii.hexlify(str(mcfcph_record.getComponentByName("seizureTime")))
        seizure_time = parseTimestamp(seizure_time_tmp)

        served_imei_binascii = binascii.hexlify(str(mcfcph_record.getComponentByName("servedIMEI")))
        served_imei_list = []
        served_imei_data = list(served_imei_binascii)
        for i in xrange(0, len(served_imei_data) - 1, 2):
            served_imei_list.append(served_imei_data[i+1] + "" + served_imei_data[i])
        served_imei_concatenated = ''.join(served_imei_list)
        served_imei = served_imei_concatenated.replace("f", "")

        served_imsi_binascii = binascii.hexlify(str(mcfcph_record.getComponentByName("servedIMSI")))
        served_imsi_list = []
        served_imsi_data = list(served_imsi_binascii)
        for j in xrange(0, len(served_imsi_data) - 1, 2):
            served_imsi_list.append(served_imsi_data[j+1] + "" + served_imsi_data[j])
        served_imsi_concatenated = ''.join(served_imsi_list)
        served_imsi = served_imsi_concatenated.replace("f", "")

        served_msisdn_binascii = binascii.hexlify(str(mcfcph_record.getComponentByName("servedMSISDN")))
        served_msisdn_list = []
        served_msisdn_data = list(served_msisdn_binascii)
        for k in xrange(0, len(served_msisdn_data) - 1, 2):
            served_msisdn_list.append(served_msisdn_data[k+1] + "" + served_msisdn_data[k])
        served_msisdn_concatenated = ''.join(served_msisdn_list)
        served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
        served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        if mcfcph_record.getComponentByName("serviceKey") is None:
            service_key = ""
        else:
            service_key = str(mcfcph_record.getComponentByName("serviceKey"))

        subscriber_category = binascii.hexlify(str(mcfcph_record.getComponentByName("subscriberCategory")))

        if mcfcph_record.getComponentByName("systemType") is None:
            system_type = ""
        else:
            system_type_tmp = long(str(mcfcph_record.getComponentByName("systemType")))
            system_type = ""
            if system_type_tmp == 0:
                system_type = "unknown"
            elif system_type_tmp == 1:
                system_type = "iuUTRAN"
            elif system_type_tmp == 2:
                system_type = "gERAN"

        mcfcph_record = [charged_party, charge_indicator, answer_time, basic_service, call_duration, called_number,
                         calling_number, call_reference, call_segment_id, camel_initcf_indicator, cause_for_termination,
                         default_call_handling, diagnostics, file_name, free_format_data, free_format_data_append,
                         global_call_reference, gsm_scfaddress, hot_billing_tag, is_camel_call, last_long_part_ind,
                         level_of_camel_service, mcf_type, milli_sec_duration, msc_address, msc_incoming_tkgp,
                         network_call_reference, number_of_dp_encountered, partial_record_type, part_sequence_number,
                         recording_entity, record_sequence_number, record_type, release_time, seizure_time, served_imei,
                         served_imsi, served_msisdn, service_key, subscriber_category, system_type]

        return mcfcph_record

