import binascii

from common.utilities import parseTimestamp, append_timezone_offset, boolean_value, parse_start_time


class ProcessZTEumtsOutGatewayRecord:
    def __init__(self):
        pass

    @staticmethod
    def process_outgateway_records(outgateway_record, file_id, file_name):

        additional_chg_info = outgateway_record.getComponentByName("additionalChgInfo")

        if additional_chg_info is None:
            charge_indicator = -255
        else:
            charge_indicator = long(str(additional_chg_info.getComponentByName("chargeIndicator")))

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

        answer_time_tmp = binascii.hexlify(str(outgateway_record.getComponentByName("answerTime")))
        answer_time = append_timezone_offset(parseTimestamp(answer_time_tmp))

        if outgateway_record.getComponentByName("callDuration") is None:
            call_duration = -255
        else:
            call_duration = long(str(outgateway_record.getComponentByName("callDuration")))

        if outgateway_record.getComponentByName("callReference") is None:
            call_reference = ""
        else:
            call_reference = binascii.hexlify(str(outgateway_record.getComponentByName("callReference")))

        if outgateway_record.getComponentByName("calledNumber") is None:
            called_number = ""
        else:
            called_number_binascii = binascii.hexlify(str(outgateway_record.getComponentByName("calledNumber")))
            called_number_list = []
            called_number_data = list(called_number_binascii)
            for i in xrange(0, len(called_number_data) - 1, 2):
                called_number_list.append(called_number_data[i + 1] + "" + called_number_data[i])
            called_number_concatenated = ''.join(called_number_list)
            called_number = called_number_concatenated[2:]

        if outgateway_record.getComponentByName("callingNumber") is None:
            calling_number = ""
        else:
            calling_number_binascii = binascii.hexlify(
                    str(outgateway_record.getComponentByName("callingNumber")))
            calling_number_list = []
            calling_number_data = list(calling_number_binascii)
            for i in xrange(0, len(calling_number_data) - 1, 2):
                calling_number_list.append(calling_number_data[i + 1] + "" + calling_number_data[i])
            calling_number_concatenated = ''.join(calling_number_list)
            calling_number = calling_number_concatenated[2:]

        if outgateway_record.getComponentByName("causeForTerm") is None:
            cause_for_termination = ""
        else:
            cause_for_termination_tmp = long(str(outgateway_record.getComponentByName("causeForTerm")))
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

        if outgateway_record.getComponentByName("connectedNumber") is None:
            connected_number = ""
        else:
            connected_number_binascii = binascii.hexlify(str(outgateway_record.getComponentByName("connectedNumber")))
            connected_number_list = []
            connected_number_data = list(connected_number_binascii)
            for i in xrange(0, len(connected_number_data) - 1, 2):
                connected_number_list.append(connected_number_data[i + 1] + "" + connected_number_data[i])
            connected_number_concatenated = ''.join(connected_number_list)
            connected_number = connected_number_concatenated[2:]

        if outgateway_record.getComponentByName("dataVolume") is None:
            data_volume = -255
        else:
            data_volume = long(str(outgateway_record.getComponentByName("dataVolume")))

        if outgateway_record.getComponentByName("dDCFlag") is None:
            ddc_flag = -255
        else:
            ddc_flag = long(str(outgateway_record.getComponentByName("dDCFlag")))

        if outgateway_record.getComponentByName("diagnostics") is None:
            diagnostics = ""
        else:
            diagnostics = outgateway_record.getComponentByName("diagnostics").getName()

        if outgateway_record.getComponentByName("exchangeIdentity") is None:
            exchange_identity = ""
        else:
            exchange_identity = str(outgateway_record.getComponentByName("exchangeIdentity"))

        if outgateway_record.getComponentByName("forwardedToNumber") is None:
            forwarded_to_number = ""
        else:
            forwarded_to_number = str(outgateway_record.getComponentByName("forwardedToNumber"))

        if outgateway_record.getComponentByName("gatewayRecordType") is None:
            gateway_record_type = ""
        else:
            gateway_record_type_tmp = long(str(outgateway_record.getComponentByName("gatewayRecordType")))
            gateway_record_type = ""
            if gateway_record_type_tmp == 0:
                gateway_record_type = "gatewayRecord"
            elif gateway_record_type_tmp == 1:
                gateway_record_type = "trunkRecord"
            elif gateway_record_type_tmp == 2:
                gateway_record_type = "sipRecord"

        if outgateway_record.getComponentByName("globalCallReference") is None:
            global_call_reference = ""
        else:
            global_call_reference = str(outgateway_record.getComponentByName("globalCallReference"))

        if outgateway_record.getComponentByName("iMS-Charging-Identifier") is None:
            ims_charging_identifier = ""
        else:
            ims_charging_identifier = str(outgateway_record.getComponentByName("iMS-Charging-Identifier"))

        incoming_cic = outgateway_record.getComponentByName("incomingCic")

        if incoming_cic is None:
            incoming_cic_channel = -255
        else:
            incoming_cic_channel = long(str(incoming_cic.getComponentByName("channel")))

        if incoming_cic is None:
            incoming_cic_pcmunit = -255
        else:
            incoming_cic_pcmunit = long(str(incoming_cic.getComponentByName("pcmUnit")))

        if outgateway_record.getComponentByName("incomingTKGPName") is None:
            incoming_tkgpname = ""
        else:
            incoming_tkgpname = str(outgateway_record.getComponentByName("incomingTKGPName"))

        if outgateway_record.getComponentByName("incomingTrafficType") is None:
            incoming_traffic_type = ""
        else:
            incoming_traffic_type_tmp = long(str(outgateway_record.getComponentByName("incomingTrafficType")))
            incoming_traffic_type = ""
            if incoming_traffic_type_tmp == 0:
                incoming_traffic_type = "unknown"
            elif incoming_traffic_type_tmp == 1:
                incoming_traffic_type = "localNetworkMobile"
            elif incoming_traffic_type_tmp == 2:
                incoming_traffic_type = "localNetworkFixed"
            elif incoming_traffic_type_tmp == 3:
                incoming_traffic_type = "externalNetwork"

        if outgateway_record.getComponentByName("initialCalledNumber") is None:
            initial_called_number = ""
        else:
            initial_called_number = str(outgateway_record.getComponentByName("initialCalledNumber"))

        if outgateway_record.getComponentByName("isdnBasicService") is None:
            isdn_basicservice = ""
        else:
            isdn_basicservice_choice = outgateway_record.getComponentByName("isdnBasicService")
            isdn_basicservice_choice_hexvalue = isdn_basicservice_choice.getComponent().prettyPrint()
            if isdn_basicservice_choice_hexvalue == "0x11":
                isdn_basicservice = "teleservice"
            else:
                isdn_basicservice = "bearerservice"

        is_multimedia_call_get_value = outgateway_record.getComponentByName("isMultiMediaCall")
        is_multimedia_call = boolean_value(is_multimedia_call_get_value)

        last_longpart_ind_get_value = outgateway_record.getComponentByName("lastLongPartInd")
        last_longpart_ind = boolean_value(last_longpart_ind_get_value)

        if outgateway_record.getComponentByName("localOfficeForward") is None:
            local_office_forward = ""
        else:
            local_office_forward = str(outgateway_record.getComponentByName("localOfficeForward"))

        if outgateway_record.getComponentByName("millisecDuration") is None:
            milli_sec_duration = -255
        else:
            milli_sec_duration = long(str(outgateway_record.getComponentByName("millisecDuration")))

        if outgateway_record.getComponentByName("mNPNumber") is None:
            mnpnumber = ""
        else:
            mnpnumber = str(outgateway_record.getComponentByName("mNPNumber"))

        if outgateway_record.getComponentByName("mscIncomingTKGP") is None:
            msc_incoming_tkgp = ""
        else:
            msc_incoming_tkgp = binascii.hexlify(str(outgateway_record.getComponentByName("mscIncomingTKGP")))

        if outgateway_record.getComponentByName("mscOutgoingTKGP") is None:
            msc_outgoing_tkgp = ""
        else:
            msc_outgoing_tkgp = binascii.hexlify(str(outgateway_record.getComponentByName("mscOutgoingTKGP")))

        if outgateway_record.getComponentByName("mscSPC14") is None:
            mscspc14 = ""
        else:
            mscspc14 = binascii.hexlify(str(outgateway_record.getComponentByName("mscSPC14")))

        if outgateway_record.getComponentByName("mscSPC24") is None:
            mscspc24 = ""
        else:
            mscspc24 = binascii.hexlify(str(outgateway_record.getComponentByName("mscSPC24")))

        if outgateway_record.getComponentByName("operatorId") is None:
            operator_id = -255
        else:
            operator_id = long(str(outgateway_record.getComponentByName("operatorId")))

        if outgateway_record.getComponentByName("orig-ioi") is None:
            orig_ioi = ""
        else:
            orig_ioi = str(outgateway_record.getComponentByName("orig-ioi"))

        outgoing_cic = outgateway_record.getComponentByName("outgoingCic")

        if outgoing_cic is None:
            outgoing_cic_channel = -255
        else:
            outgoing_cic_channel = long(str(outgoing_cic.getComponentByName("channel")))

        if outgoing_cic is None:
            outgoing_cic_pcmunit = -255
        else:
            outgoing_cic_pcmunit = long(str(outgoing_cic.getComponentByName("pcmUnit")))

        if outgateway_record.getComponentByName("outgoingTKGPName") is None:
            outgoing_tkgpname = ""
        else:
            outgoing_tkgpname = str(outgateway_record.getComponentByName("outgoingTKGPName"))

        if outgateway_record.getComponentByName("outgoingTrafficType") is None:
            outgoing_traffic_type = ""
        else:
            outgoing_traffic_type_tmp = long(str(outgateway_record.getComponentByName("outgoingTrafficType")))
            outgoing_traffic_type = ""
            if outgoing_traffic_type_tmp == 0:
                outgoing_traffic_type = "unknown"
            elif outgoing_traffic_type_tmp == 1:
                outgoing_traffic_type = "localNetworkMobile"
            elif outgoing_traffic_type_tmp == 2:
                outgoing_traffic_type = "localNetworkFixed"
            elif outgoing_traffic_type_tmp == 3:
                outgoing_traffic_type = "externalNetwork"

        if outgateway_record.getComponentByName("partialRecordType") is None:
            partial_record_type = ""
        else:
            partial_record_type_tmp = long(str(outgateway_record.getComponentByName("partialRecordType")))
            partial_record_type = ""
            if partial_record_type_tmp == 1:
                partial_record_type = "serviceChange"

        if outgateway_record.getComponentByName("pbrt") is None:
            pbrt = -255
        else:
            pbrt = long(str(outgateway_record.getComponentByName("pbrt")))

        if outgateway_record.getComponentByName("reasonForServiceChange") is None:
            reason_for_service_change = ""
        else:
            reason_for_service_change_tmp = long(str(outgateway_record.getComponentByName("reasonForServiceChange")))
            reason_for_service_change = ""
            if reason_for_service_change_tmp == 0:
                reason_for_service_change = "msubInitiate"
            elif reason_for_service_change_tmp == 1:
                reason_for_service_change = "mscInitiated"
            elif reason_for_service_change_tmp == 2:
                reason_for_service_change = "callSetupFallBack"
            elif reason_for_service_change_tmp == 3:
                reason_for_service_change = "callSetupChangeOrder"

        if outgateway_record.getComponentByName("recordingEntity") is None:
            recording_entity = ""
        else:
            recording_entity_binascii = binascii.hexlify(str(outgateway_record.getComponentByName("recordingEntity")))
            recording_entity_list = []
            recording_entity_data = list(recording_entity_binascii)
            for l in xrange(0, len(recording_entity_data) - 1, 2):
                recording_entity_list.append(recording_entity_data[l+1] + "" + recording_entity_data[l])
            recording_entity_concatenated = ''.join(recording_entity_list)
            recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
            recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        if outgateway_record.getComponentByName("recordSequenceNumber") is None:
            record_sequence_number = ""
        else:
            record_sequence_number = binascii.hexlify(str(outgateway_record.getComponentByName("recordSequenceNumber")))

        if outgateway_record.getComponentByName("recordType") is None:
            record_type = -255
        else:
            record_type = long(str(outgateway_record.getComponentByName("recordType")))

        if outgateway_record.getComponentByName("redirectNumber") is None:
            redirecting_number = ""
        else:
            redirecting_number_binascii = binascii.hexlify(str(outgateway_record.getComponentByName("redirectNumber")))
            redirecting_number_list = []
            redirecting_number_data = list(redirecting_number_binascii)
            for i in xrange(0, len(redirecting_number_data) - 1, 2):
                redirecting_number_list.append(redirecting_number_data[i + 1] + "" + redirecting_number_data[i])
            redirecting_number_concatenated = ''.join(redirecting_number_list)
            redirecting_number = redirecting_number_concatenated[2:].replace("f", "")

        release_time_tmp = binascii.hexlify(str(outgateway_record.getComponentByName("releaseTime")))
        release_time = append_timezone_offset(parseTimestamp(release_time_tmp))

        rn_plus_b_get_value = outgateway_record.getComponentByName("rnPlusB")
        rn_plus_b = boolean_value(rn_plus_b_get_value)

        if outgateway_record.getComponentByName("roamingNumber") is None:
            roaming_number = ""
        else:
            roaming_number_binascii = binascii.hexlify(str(outgateway_record.getComponentByName("roamingNumber")))
            roaming_number_list = []
            roaming_number_data = list(roaming_number_binascii)
            for i in xrange(0, len(roaming_number_data) - 1, 2):
                roaming_number_list.append(roaming_number_data[i + 1] + "" + roaming_number_data[i])
            roaming_number_concatenated = ''.join(roaming_number_list)
            roaming_number = roaming_number_concatenated[2:].replace("f", "")

        seizure_time_tmp = binascii.hexlify(str(outgateway_record.getComponentByName("seizureTime")))
        seizure_time = append_timezone_offset(parseTimestamp(seizure_time_tmp))

        if outgateway_record.getComponentByName("sequenceNumber") is None:
            sequence_number = -255
        else:
            sequence_number = long(str(outgateway_record.getComponentByName("sequenceNumber")))

        if outgateway_record.getComponentByName("serviceChangeInitiator") is None:
            service_change_initiator = -255
        else:
            service_change_initiator = long(str(outgateway_record.getComponentByName("serviceChangeInitiator")))

        if outgateway_record.getComponentByName("term-ioi") is None:
            term_ioi = ""
        else:
            term_ioi = str(outgateway_record.getComponentByName("term-ioi"))

        if outgateway_record.getComponentByName("trunkLine") is None:
            trunk_line = ""
        else:
            trunk_line = binascii.hexlify(str(outgateway_record.getComponentByName("trunkLine")))

        if outgateway_record.getComponentByName("typeOfServiceChange") is None:
            types_of_service_change = ""
        else:
            types_of_service_change_tmp = long(str(outgateway_record.getComponentByName("typeOfServiceChange")))
            types_of_service_change = ""
            if types_of_service_change_tmp == 0:
                types_of_service_change = "changeToSpeech"
            elif types_of_service_change_tmp == 1:
                types_of_service_change = "changeToMultimedia"

        if outgateway_record.getComponentByName("startTime") is None:
            start_time = parse_start_time(file_name)
        else:
            start_time_tmp = binascii.hexlify(str(outgateway_record.getComponentByName("startTime")))
            start_time = append_timezone_offset(parseTimestamp(start_time_tmp))

        outgateway_record = [charge_indicator, charged_party,
                             answer_time, call_duration,
                             call_reference, called_number,
                             calling_number, cause_for_termination, connected_number, ddc_flag, data_volume,
                             diagnostics, exchange_identity, file_id, forwarded_to_number, gateway_record_type,
                             global_call_reference, ims_charging_identifier, incoming_cic_channel,
                             incoming_cic_pcmunit,
                             incoming_tkgpname, incoming_traffic_type, initial_called_number, is_multimedia_call,
                             isdn_basicservice, last_longpart_ind, local_office_forward, mnpnumber,
                             milli_sec_duration, msc_incoming_tkgp, msc_outgoing_tkgp, mscspc14, mscspc24,
                             operator_id, orig_ioi, outgoing_cic_channel, outgoing_cic_pcmunit,
                             outgoing_tkgpname,
                             outgoing_traffic_type, partial_record_type, pbrt, reason_for_service_change,
                             record_sequence_number, record_type, recording_entity, redirecting_number,
                             release_time, rn_plus_b, roaming_number, seizure_time, sequence_number,
                             service_change_initiator,
                             start_time, term_ioi, trunk_line,
                             types_of_service_change, long(seizure_time.strftime("%Y%m%d")),
                             long(seizure_time.strftime("%H"))]

        return outgateway_record

