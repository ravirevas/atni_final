import binascii

from common.constants import *

from common.utilities import parseTimestamp


class ProcessZTEumtsTermCAMELIntRecord:
    def __init__(self):
        pass

    @staticmethod
    def process_termcamelint_records(termcamelint_record, file_name):

        answer_time_tmp = binascii.hexlify(str(termcamelint_record.getComponentByName("answerTime")))
        answer_time = parseTimestamp(answer_time_tmp)

        if termcamelint_record.getComponentByName("byPassFlag2") is None:
            by_pass_flag2 = 0
        else:
            by_pass_flag2 = long(str(termcamelint_record.getComponentByName("byPassFlag2")))

        if termcamelint_record.getComponentByName("callDuration") is None:
            call_duration = 0
        else:
            call_duration = long(str(termcamelint_record.getComponentByName("callDuration")))

        called_number_binascii = binascii.hexlify(str(termcamelint_record.getComponentByName("calledNumber")))
        called_number_list = []
        called_number_data = list(called_number_binascii)
        for i in xrange(0, len(called_number_data) - 1, 2):
            called_number_list.append(called_number_data[i + 1] + "" + called_number_data[i])
        called_number_concatenated = ''.join(called_number_list)
        called_number = called_number_concatenated[2:]

        calling_number_binascii = binascii.hexlify(
                str(termcamelint_record.getComponentByName("callingNumber")))
        calling_number_list = []
        calling_number_data = list(calling_number_binascii)
        for i in xrange(0, len(calling_number_data) - 1, 2):
            calling_number_list.append(calling_number_data[i + 1] + "" + calling_number_data[i])
        calling_number_concatenated = ''.join(calling_number_list)
        calling_number = calling_number_concatenated[2:]

        call_reference = binascii.hexlify(str(termcamelint_record.getComponentByName("callReference")))

        if termcamelint_record.getComponentByName("causeForTerm") is None:
            cause_for_termination = ""
        else:
            cause_for_termination_tmp = long(str(termcamelint_record.getComponentByName("causeForTerm")))
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

        if termcamelint_record.getComponentByName("dataVolume") is None:
            data_volume = 0
        else:
            data_volume = long(str(termcamelint_record.getComponentByName("dataVolume")))

        if termcamelint_record.getComponentByName("defaultCallHandling") is None:
            default_call_handling = ""
        else:
            default_call_handling_tmp = long(str(termcamelint_record.getComponentByName("defaultCallHandling")))
            default_call_handling = ""
            if default_call_handling_tmp == 0:
                default_call_handling = "continueCall"
            elif default_call_handling_tmp == 1:
                default_call_handling = "releaseCall"

        if termcamelint_record.getComponentByName("defaultCallHandling_2") is None:
            default_call_handling2 = ""
        else:
            default_call_handling2_tmp = str(termcamelint_record.getComponentByName("defaultCallHandling_2"))
            default_call_handling2 = ""
            if default_call_handling2_tmp == 0:
                default_call_handling2 = "continueCall"
            elif default_call_handling2_tmp == 1:
                default_call_handling2 = "releaseCall"

        destination_routing_address = str(termcamelint_record.getComponentByName("destinationRoutingAddress"))

        if termcamelint_record.getComponentByName("diagnostics") is None:
            diagnostics = ""
        else:
            diagnostics = str(termcamelint_record.getComponentByName("diagnostics"))

        exchange_identity = str(termcamelint_record.getComponentByName("exchangeIdentity"))

        file_name = file_name

        free_format_data = str(termcamelint_record.getComponentByName("freeFormatData"))

        free_format_data_append = str(termcamelint_record.getComponentByName("freeFormatDataAppend"))

        free_format_data_append_2 = str(termcamelint_record.getComponentByName("freeFormatDataAppend_2"))

        free_format_data_2 = str(termcamelint_record.getComponentByName("freeFormatData_2"))

        global_call_reference = str(termcamelint_record.getComponentByName("globalCallReference"))

        if termcamelint_record.getComponentByName("gsm-SCFAddress") is not None:
            gsm_scfaddress_binascii = binascii.hexlify(str(termcamelint_record.getComponentByName("gsm-SCFAddress")))
            gsm_scfaddress_list = []
            gsm_scfaddress_data = list(gsm_scfaddress_binascii)
            for i in xrange(0, len(gsm_scfaddress_data) - 1, 2):
                gsm_scfaddress_list.append(gsm_scfaddress_data[i + 1] + "" + gsm_scfaddress_data[i])
                gsm_scfaddress_concatenated = ''.join(gsm_scfaddress_list)
                gsm_scfaddress = gsm_scfaddress_concatenated[2:].replace("f", "")
        else:
            gsm_scfaddress = ""

        if termcamelint_record.getComponentByName("gsm-SCFAddress_2") is not None:
            gsm_scfaddress_2_binascii = binascii.hexlify(
                    str(termcamelint_record.getComponentByName("gsm-SCFAddress_2")))
            gsm_scfaddress_2_list = []
            gsm_scfaddress_2_data = list(gsm_scfaddress_2_binascii)
            for j in xrange(0, len(gsm_scfaddress_2_data) - 1, 2):
                gsm_scfaddress_2_list.append(gsm_scfaddress_2_data[j + 1] + "" + gsm_scfaddress_2_data[j])
                gsm_scfaddress_2_concatenated = ''.join(gsm_scfaddress_2_list)
                gsm_scfaddress_2 = gsm_scfaddress_2_concatenated[2:].replace("f", "")
        else:
            gsm_scfaddress_2 = ""

        if termcamelint_record.getComponentByName("hotBillingTag2") is None:
            hot_billing_tag2 = 0
        else:
            hot_billing_tag2 = long(str(termcamelint_record.getComponentByName("hotBillingTag2")))

        incoming_cic = termcamelint_record.getComponentByName("incomingCic")

        if incoming_cic is None:
            incoming_cic_channel = 0
        else:
            incoming_cic_channel = long(str(incoming_cic.getComponentByName("channel")))

        if incoming_cic is None:
            incoming_cic_pcmunit = 0
        else:
            incoming_cic_pcmunit = long(str(incoming_cic.getComponentByName("pcmUnit")))

        incoming_tkgpname = str(termcamelint_record.getComponentByName("incomingTKGPName"))

        if termcamelint_record.getComponentByName("incomingTrafficType") is None:
            incoming_traffic_type = ""
        else:
            incoming_traffic_type_tmp = long(str(termcamelint_record.getComponentByName("incomingTrafficType")))
            incoming_traffic_type = ""
            if incoming_traffic_type_tmp == 0:
                incoming_traffic_type = "unknown"
            elif incoming_traffic_type_tmp == 1:
                incoming_traffic_type = "localNetworkMobile"
            elif incoming_traffic_type_tmp == 2:
                incoming_traffic_type = "localNetworkFixed"
            elif incoming_traffic_type_tmp == 3:
                incoming_traffic_type = "externalNetwork"

        interrogation_time_tmp = binascii.hexlify(str(termcamelint_record.getComponentByName("interrogationTime")))
        interrogation_time = parseTimestamp(interrogation_time_tmp)

        last_long_part_ind = str(termcamelint_record.getComponentByName("lastLongPartInd"))

        level_of_camel_service = str(termcamelint_record.getComponentByName("levelOfCAMELService"))

        if termcamelint_record.getComponentByName("millisecDuration") is None:
            milli_sec_duration = 0
        else:
            milli_sec_duration = long(str(termcamelint_record.getComponentByName("millisecDuration")))

        msc_address = str(termcamelint_record.getComponentByName("mSCAddress"))

        msc_incoming_tkgp = binascii.hexlify(str(termcamelint_record.getComponentByName("mscIncomingTKGP")))

        if termcamelint_record.getComponentByName("mscOutgoingTKGP") is None:
            msc_outgoing_tkgp = 0
        else:
            msc_outgoing_tkgp = binascii.hexlify(str(termcamelint_record.getComponentByName("mscOutgoingTKGP")))

        msc_server_indication = str(termcamelint_record.getComponentByName("mscServerIndication"))

        mscspc14_tmp = binascii.hexlify(str(termcamelint_record.getComponentByName("mscSPC14")))

        mscspc24_tmp = binascii.hexlify(str(termcamelint_record.getComponentByName("mscSPC24")))

        if termcamelint_record.getComponentByName("networkCallReference") is None:
            network_call_reference = ""
        else:
            network_call_reference = binascii.hexlify(
                    str(termcamelint_record.getComponentByName("networkCallReference")))

        if termcamelint_record.getComponentByName("numberOfDPEncountered") is None:
            number_of_dpencountered = 0
        else:
            number_of_dpencountered = long(str(termcamelint_record.getComponentByName("numberOfDPEncountered")))

        if termcamelint_record.getComponentByName("operatorId") is None:
            operator_id = 0
        else:
            operator_id = long(str(termcamelint_record.getComponentByName("operatorId")))

        outgoing_cic = termcamelint_record.getComponentByName("outgoingCic")

        if outgoing_cic is None:
            outgoing_cic_channel = 0
        else:
            outgoing_cic_channel = long(str(outgoing_cic.getComponentByName("channel")))

        if outgoing_cic is None:
            outgoing_cic_pcmunit = 0
        else:
            outgoing_cic_pcmunit = long(str(outgoing_cic.getComponentByName("pcmUnit")))

        outgoing_tkgpname = str(termcamelint_record.getComponentByName("outgoingTKGPName"))

        if termcamelint_record.getComponentByName("outgoingTrafficType") is None:
            outgoing_traffic_type = ""
        else:
            outgoing_traffic_type_tmp = long(str(termcamelint_record.getComponentByName("outgoingTrafficType")))
            outgoing_traffic_type = ""
            if outgoing_traffic_type_tmp == 0:
                outgoing_traffic_type = "unknown"
            elif outgoing_traffic_type_tmp == 1:
                outgoing_traffic_type = "localNetworkMobile"
            elif outgoing_traffic_type_tmp == 2:
                outgoing_traffic_type = "localNetworkFixed"
            elif outgoing_traffic_type_tmp == 3:
                outgoing_traffic_type = "externalNetwork"

        recording_entity_binascii = binascii.hexlify(str(termcamelint_record.getComponentByName("recordingEntity")))
        recording_entity_list = []
        recording_entity_data = list(recording_entity_binascii)
        for l in xrange(0, len(recording_entity_data) - 1, 2):
            recording_entity_list.append(recording_entity_data[l + 1] + "" + recording_entity_data[l])
        recording_entity_concatenated = ''.join(recording_entity_list)
        recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
        recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        record_sequence_number = binascii.hexlify(str(termcamelint_record.getComponentByName("recordSequenceNumber")))

        record_type = long(str(termcamelint_record.getComponentByName("recordType")))

        release_time_tmp = binascii.hexlify(str(termcamelint_record.getComponentByName("releaseTime")))
        release_time = parseTimestamp(release_time_tmp)

        seizure_time_tmp = binascii.hexlify(str(termcamelint_record.getComponentByName("seizureTime")))
        seizure_time = parseTimestamp(seizure_time_tmp)

        if termcamelint_record.getComponentByName("sequenceNumber") is None:
            sequence_number = 0
        else:
            sequence_number = long(str(termcamelint_record.getComponentByName("sequenceNumber")))

        served_imsi_binascii = binascii.hexlify(str(termcamelint_record.getComponentByName("servedIMSI")))
        served_imsi_list = []
        served_imsi_data = list(served_imsi_binascii)
        for j in xrange(0, len(served_imsi_data) - 1, 2):
            served_imsi_list.append(served_imsi_data[j + 1] + "" + served_imsi_data[j])
        served_imsi_concatenated = ''.join(served_imsi_list)
        served_imsi = served_imsi_concatenated.replace("f", "")

        served_msisdn_binascii = binascii.hexlify(str(termcamelint_record.getComponentByName("servedMSISDN")))
        served_msisdn_list = []
        served_msisdn_data = list(served_msisdn_binascii)
        for k in xrange(0, len(served_msisdn_data) - 1, 2):
            served_msisdn_list.append(served_msisdn_data[k + 1] + "" + served_msisdn_data[k])
        served_msisdn_concatenated = ''.join(served_msisdn_list)
        served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
        served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        if termcamelint_record.getComponentByName("serviceKey") is None:
            service_key = 0
        else:
            service_key = long(str(termcamelint_record.getComponentByName("serviceKey")))

        if termcamelint_record.getComponentByName("serviceKey_2") is None:
            service_key_2 = 0
        else:
            service_key_2 = long(str(termcamelint_record.getComponentByName("serviceKey_2")))

        start_time = parseTimestamp('1606151210002d0700')

        termcamelint_record = [answer_time, by_pass_flag2, call_duration, called_number, calling_number, call_reference,
                               cause_for_termination, data_volume, default_call_handling, default_call_handling2,
                               destination_routing_address, diagnostics, exchange_identity, file_name, free_format_data,
                               free_format_data_2, free_format_data_append, free_format_data_append_2,
                               global_call_reference, gsm_scfaddress, gsm_scfaddress_2, hot_billing_tag2,
                               incoming_cic_channel, incoming_cic_pcmunit, incoming_tkgpname, incoming_traffic_type,
                               interrogation_time, last_long_part_ind, level_of_camel_service, milli_sec_duration,
                               msc_address, msc_incoming_tkgp, msc_outgoing_tkgp, msc_server_indication, mscspc14_tmp,
                               mscspc24_tmp, network_call_reference, number_of_dpencountered,
                               operator_id, outgoing_cic_channel,
                               outgoing_cic_pcmunit, outgoing_tkgpname, outgoing_traffic_type, recording_entity,
                               record_sequence_number, record_type, release_time, seizure_time, sequence_number,
                               served_imsi, served_msisdn, service_key, service_key_2, start_time]
        return termcamelint_record
