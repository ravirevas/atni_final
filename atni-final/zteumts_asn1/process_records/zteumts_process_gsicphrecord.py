import binascii


class ProcessZTEumtsGSICPHRecord:

    def __init__(self):
        pass

    @staticmethod
    def process_gsicph_records(gsicph_record, file_name):

        additional_chg_info = gsicph_record.getComponentByName("additionalChgInfo")

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

        if gsicph_record.getComponentByName("basicService") is None:
            basic_service = ""
        else:
            basic_service = str(gsicph_record.getComponentByName("basicService").prettyPrint())

        called_number_binascii = binascii.hexlify(str(gsicph_record.getComponentByName("calledNumber")))
        called_number_list = []
        called_number_data = list(called_number_binascii)
        for i in xrange(0, len(called_number_data) - 1, 2):
            called_number_list.append(called_number_data[i + 1] + "" + called_number_data[i])
        called_number_concatenated = ''.join(called_number_list)
        called_number = called_number_concatenated[2:]

        call_reference = binascii.hexlify(str(gsicph_record.getComponentByName("callReference")))

        call_segment_id = long(str(gsicph_record.getComponentByName("callSegmentId")))

        if gsicph_record.getComponentByName("causeForTerm") is None:
            cause_for_termination = ""
        else:
            cause_for_termination_tmp = long(str(gsicph_record.getComponentByName("causeForTerm")))
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

        if gsicph_record.getComponentByName("defaultCallHandling") is None:
            default_call_handling = ""
        else:
            default_call_handling_tmp = long(str(gsicph_record.getComponentByName("defaultCallHandling")))
            default_call_handling = ""
            if default_call_handling_tmp == 0:
                default_call_handling = "continueCall"
            elif default_call_handling_tmp == 1:
                default_call_handling = "releaseCall"

        if gsicph_record.getComponentByName("diagnostics") is None:
            diagnostics = ""
        else:
            diagnostics = str(gsicph_record.getComponentByName("diagnostics"))

        file_name = file_name

        global_call_reference = str(gsicph_record.getComponentByName("globalCallReference"))

        if gsicph_record.getComponentByName("gsm-SCFAddress") is not None:
            gsm_scfaddress_binascii = binascii.hexlify(str(gsicph_record.getComponentByName("gsm-SCFAddress")))
            gsm_scfaddress_list = []
            gsm_scfaddress_data = list(gsm_scfaddress_binascii)
            for i in xrange(0, len(gsm_scfaddress_data) - 1, 2):
                gsm_scfaddress_list.append(gsm_scfaddress_data[i + 1] + "" + gsm_scfaddress_data[i])
                gsm_scfaddress_concatenated = ''.join(gsm_scfaddress_list)
                gsm_scfaddress = gsm_scfaddress_concatenated[2:].replace("f", "")
        else:
            gsm_scfaddress = ""

        is_camel_call = str(gsicph_record.getComponentByName("isCAMELCall"))

        last_long_part_ind = str(gsicph_record.getComponentByName("lastLongPartInd"))

        level_of_camel_service = str(gsicph_record.getComponentByName("levelOfCAMELService"))

        msc_address = str(gsicph_record.getComponentByName("mSCAddress"))

        if gsicph_record.getComponentByName("networkCallReference") is None:
            network_call_reference = ""
        else:
            network_call_reference = binascii.hexlify(
                str(gsicph_record.getComponentByName("networkCallReference")))

        number_of_dp_encountered = str(gsicph_record.getComponentByName("numberOfDPEncountered"))

        if gsicph_record.getComponentByName("partialRecordType") is None:
            partial_record_type = ""
        else:
            partial_record_type_tmp = long(str(gsicph_record.getComponentByName("partialRecordType")))
            partial_record_type = ""
            if partial_record_type_tmp == 1:
                partial_record_type = "serviceChange"

        if gsicph_record.getComponentByName("partSequenceNumber") is None:
            part_sequence_number = 0
        else:
            part_sequence_number = long(str(gsicph_record.getComponentByName("partSequenceNumber")))

        recording_entity_binascii = binascii.hexlify(str(gsicph_record.getComponentByName("recordingEntity")))
        recording_entity_list = []
        recording_entity_data = list(recording_entity_binascii)
        for l in xrange(0, len(recording_entity_data) - 1, 2):
            recording_entity_list.append(recording_entity_data[l + 1] + "" + recording_entity_data[l])
        recording_entity_concatenated = ''.join(recording_entity_list)
        recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
        recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        record_sequence_number = binascii.hexlify(str(gsicph_record.getComponentByName("recordSequenceNumber")))

        if gsicph_record.getComponentByName("recordType") is None:
            record_type = 0
        else:
            record_type = long(str(gsicph_record.getComponentByName("recordType")))

        served_msisdn_binascii = binascii.hexlify(str(gsicph_record.getComponentByName("servedMSISDN")))
        served_msisdn_list = []
        served_msisdn_data = list(served_msisdn_binascii)
        for k in xrange(0, len(served_msisdn_data) - 1, 2):
            served_msisdn_list.append(served_msisdn_data[k + 1] + "" + served_msisdn_data[k])
        served_msisdn_concatenated = ''.join(served_msisdn_list)
        served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
        served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        gsicph_record = [charged_party, charge_indicator, basic_service, called_number, call_reference, call_segment_id,
                         cause_for_termination, default_call_handling, diagnostics, file_name, global_call_reference,
                         gsm_scfaddress, is_camel_call, last_long_part_ind, level_of_camel_service, msc_address,
                         network_call_reference, number_of_dp_encountered, partial_record_type, part_sequence_number,
                         recording_entity, record_sequence_number, record_type, served_msisdn]

        return gsicph_record





