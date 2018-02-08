import binascii

from common.constants import *

from common.utilities import parseTimestamp, boolean_value


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


class ProcessZTEumtsMOECallRecord:
    def __init__(self):
        pass

    @staticmethod
    def process_moecall_records(moecallrecord_record, file_name):

        answer_time_tmp = binascii.hexlify(str(moecallrecord_record.getComponentByName("answerTime")))
        answer_time = parseTimestamp(answer_time_tmp)

        if moecallrecord_record.getComponentByName("aocParameters") is not None:
            aoc_parameter = str(moecallrecord_record.getComponentByName("aocParameters"))
            aoc_parametere1 = str(aoc_parameter.getComponentByName("aocParameters"))
            aoc_parametere2 = str(aoc_parameter.getComponentByName("aocParameters"))
            aoc_parametere3 = str(aoc_parameter.getComponentByName("aocParameters"))
            aoc_parametere4 = str(aoc_parameter.getComponentByName("aocParameters"))
            aoc_parametere5 = str(aoc_parameter.getComponentByName("aocParameters"))
            aoc_parametere6 = str(aoc_parameter.getComponentByName("aocParameters"))
            aoc_parametere7 = str(aoc_parameter.getComponentByName("aocParameters"))
        else:
            aoc_parametere1 = ""
            aoc_parametere2 = ""
            aoc_parametere3 = ""
            aoc_parametere4 = ""
            aoc_parametere5 = ""
            aoc_parametere6 = ""
            aoc_parametere7 = ""

        if moecallrecord_record.getComponentByName("aReleaseCause") is None:
            a_release_cause = 0
        else:
            a_release_cause = long(str(moecallrecord_record.getComponentByName("aReleaseCause")))

        if moecallrecord_record.getComponentByName("basicService") is None:
            basic_service = ""
        else:
            basic_service_choice = moecallrecord_record.getComponentByName("basicService")
            basic_service_choice_hexvalue = basic_service_choice.getComponent().prettyPrint()
            if basic_service_choice_hexvalue == "0x11":
                basic_service = "teleservice"
            else:
                basic_service = "bearerservice"

        if moecallrecord_record.getComponentByName("bSCIdentification") is None:
            bsdidentification = ""
        else:
            bsdidentification = binascii.hexlify(str(moecallrecord_record.getComponentByName("bSCIdentification")))

        if moecallrecord_record.getComponentByName("bscIdentification24") is None:
            bsdidentification_24 = ""
        else:
            bsdidentification_24 = binascii.hexlify(str(moecallrecord_record.getComponentByName("bscIdentification24")))

        if moecallrecord_record.getComponentByName("callDuration") is None:
            call_duration = 0
        else:
            call_duration = long(str(moecallrecord_record.getComponentByName("callDuration")))

        called_number_binascii = binascii.hexlify(str(moecallrecord_record.getComponentByName("calledNumber")))
        called_number_list = []
        called_number_data = list(called_number_binascii)
        for i in xrange(0, len(called_number_data) - 1, 2):
            called_number_list.append(called_number_data[i + 1] + "" + called_number_data[i])
        called_number_concatenated = ''.join(called_number_list)
        called_number = called_number_concatenated[2:]

        call_reference = binascii.hexlify(str(moecallrecord_record.getComponentByName("callReference")))

        if moecallrecord_record.getComponentByName("carp") is None:
            carp = 0
        else:
            carp = long(str(moecallrecord_record.getComponentByName("carp")))

        if moecallrecord_record.getComponentByName("causeForTerm") is None:
            cause_for_termination = ""
        else:
            cause_for_termination_tmp = long(str(moecallrecord_record.getComponentByName("causeForTerm")))
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

        if moecallrecord_record.getComponentByName("chanCodingUsed") is None:
            chan_coding_used = ""
        else:
            chan_coding_used_tmp = long(str(moecallrecord_record.getComponentByName("chanCodingUsed")))
            chan_coding_used = ""
            if chan_coding_used_tmp == 1:
                chan_coding_used = "tchF4800"
            elif chan_coding_used_tmp == 2:
                chan_coding_used = "tchF9600"
            elif chan_coding_used_tmp == 3:
                chan_coding_used = "tchF14400"

        if moecallrecord_record.getComponentByName("changeOfRadioChan") is not None:
            change_of_radio_chan = moecallrecord_record.getComponentByName("changeOfRadioChan")
            change_time_tmp = binascii.hexlify(str(change_of_radio_chan.getComponentByName("changeTime")))
            change_time = parseTimestamp(change_time_tmp)
            radio_channel = ""
            # if change_of_radio_chan.getComponentByName("radioChannel") is None:
            #     radio_channel = ""
            # else:
            #     radio_channel_tmp = long(str(mocallrecord_record.getComponentByName("radioChannel")))
            #     radio_channel = ""
            #     if radio_channel_tmp == 1:
            #         radio_channel = "tchF4800"
            #     elif radio_channel_tmp == 2:
            #         radio_channel = "tchF9600"
            changeofradiochan_speech_version_used = str(change_of_radio_chan.getComponentByName("speechVersionUsed"))

        else:
            radio_channel = ""
            change_time = parseTimestamp('1606151209292d0700')
            changeofradiochan_speech_version_used = ""

        csfbmo_indicator_get_value = moecallrecord_record.getComponentByName("cSFBMOIndicator")
        csfbmo_indicator = boolean_value(csfbmo_indicator_get_value)

        if moecallrecord_record.getComponentByName("defaultCallHandling") is None:
            default_call_handling = ""
        else:
            default_call_handling_tmp = long(str(moecallrecord_record.getComponentByName("defaultCallHandling")))
            default_call_handling = ""
            if default_call_handling_tmp == 0:
                default_call_handling = "continueCall"
            elif default_call_handling_tmp == 1:
                default_call_handling = "releaseCall"

        if moecallrecord_record.getComponentByName("diagnostics") is None:
            diagnostics = ""
        else:
            diagnostics = str(moecallrecord_record.getComponentByName("diagnostics"))

        dp_number = long(str(moecallrecord_record.getComponentByName("dPNumber")))

        if moecallrecord_record.getComponentByName("eMLPPPriorityLevel") is None:
            emlpp_priority_level = 0
        else:
            emlpp_priority_level = long(str(moecallrecord_record.getComponentByName("eMLPPPriorityLevel")))

        if moecallrecord_record.getComponentByName("eMLPPRequestPriorityLevel") is None:
            emlpp_request_priority_level = 0
        else:
            emlpp_request_priority_level = long(
                    str(moecallrecord_record.getComponentByName("eMLPPRequestPriorityLevel")))

        exchange_identity = str(moecallrecord_record.getComponentByName("exchangeIdentity"))

        file_name = file_name

        global_call_reference = str(moecallrecord_record.getComponentByName("globalCallReference"))

        if moecallrecord_record.getComponentByName("gsm-SCFAddress") is not None:
            gsm_scfaddress_binascii = binascii.hexlify(str(moecallrecord_record.getComponentByName("gsm-SCFAddress")))
            gsm_scfaddress_list = []
            gsm_scfaddress_data = list(gsm_scfaddress_binascii)
            for i in xrange(0, len(gsm_scfaddress_data) - 1, 2):
                gsm_scfaddress_list.append(gsm_scfaddress_data[i + 1] + "" + gsm_scfaddress_data[i])
                gsm_scfaddress_concatenated = ''.join(gsm_scfaddress_list)
                gsm_scfaddress = gsm_scfaddress_concatenated[2:].replace("f", "")
        else:
            gsm_scfaddress = ""

        if moecallrecord_record.getComponentByName("hotBillingTag") is None:
            hot_billing_tag = 0
        else:
            hot_billing_tag = long(str(moecallrecord_record.getComponentByName("hotBillingTag")))

        if moecallrecord_record.getComponentByName("hotBillingTag2") is None:
            hot_billing_tag2 = 0
        else:
            hot_billing_tag2 = long(str(moecallrecord_record.getComponentByName("hotBillingTag2")))

        incoming_cic = moecallrecord_record.getComponentByName("incomingCic")

        if incoming_cic is None:
            incoming_cic_channel = 0
        else:
            incoming_cic_channel = long(str(incoming_cic.getComponentByName("channel")))

        if incoming_cic is None:
            incoming_cic_pcmunit = 0
        else:
            incoming_cic_pcmunit = long(str(incoming_cic.getComponentByName("pcmUnit")))

        incoming_tkgpname = str(moecallrecord_record.getComponentByName("incomingTKGPName"))

        is_camel_call = str(moecallrecord_record.getComponentByName("isCAMELCall"))

        if moecallrecord_record.getComponentByName("iuReleaseCause") is None:
            iu_release_cause = 0
        else:
            iu_release_cause = long(str(moecallrecord_record.getComponentByName("iuReleaseCause")))

        last_long_part_ind = str(moecallrecord_record.getComponentByName("lastLongPartInd"))

        level_of_camel_service = str(moecallrecord_record.getComponentByName("levelOfCAMELService"))

        location = moecallrecord_record.getComponentByName("location")
        if location is not None:
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
            location_cellid = ''
            location_area_code = ''
            location_plmn = ''
            location_sac = ''

        if moecallrecord_record.getComponentByName("millisecDuration") is None:
            milli_sec_duration = 0
        else:
            milli_sec_duration = long(str(moecallrecord_record.getComponentByName("millisecDuration")))

        msc_address = str(moecallrecord_record.getComponentByName("mSCAddress"))

        msc_incoming_tkgp = binascii.hexlify(str(moecallrecord_record.getComponentByName("mscIncomingTKGP")))

        ms_classmark = binascii.hexlify(str(moecallrecord_record.getComponentByName("msClassmark")))

        if moecallrecord_record.getComponentByName("mscOutgoingTKGP") is None:
            msc_outgoing_tkgp = 0
        else:
            msc_outgoing_tkgp = binascii.hexlify(str(moecallrecord_record.getComponentByName("mscOutgoingTKGP")))

        mscspc14_tmp = binascii.hexlify(str(moecallrecord_record.getComponentByName("mscSPC14")))

        mscspc24_tmp = binascii.hexlify(str(moecallrecord_record.getComponentByName("mscSPC24")))

        if moecallrecord_record.getComponentByName("networkCallReference") is None:
            network_call_reference = ""
        else:
            network_call_reference = binascii.hexlify(
                    str(moecallrecord_record.getComponentByName("networkCallReference")))

        if moecallrecord_record.getComponentByName("operatorId") is None:
            operator_id = 0
        else:
            operator_id = long(str(moecallrecord_record.getComponentByName("operatorId")))

        outgoing_cic = moecallrecord_record.getComponentByName("outgoingCic")

        if outgoing_cic is None:
            outgoing_cic_channel = 0
        else:
            outgoing_cic_channel = long(str(outgoing_cic.getComponentByName("channel")))

        if outgoing_cic is None:
            outgoing_cic_pcmunit = 0
        else:
            outgoing_cic_pcmunit = long(str(outgoing_cic.getComponentByName("pcmUnit")))

        outgoing_tkgpname = str(moecallrecord_record.getComponentByName("outgoingTKGPName"))

        if moecallrecord_record.getComponentByName("radioChanRequested") is None:
            radio_chan_requested = ""
        else:
            radio_chan_requested_tmp = long(str(moecallrecord_record.getComponentByName("radioChanRequested")))
            radio_chan_requested = ""
            if radio_chan_requested_tmp == 0:
                radio_chan_requested = "halfRateChannel"
            elif radio_chan_requested_tmp == 1:
                radio_chan_requested = "fullRateChannel"
            elif radio_chan_requested_tmp == 2:
                radio_chan_requested = "dualHalfRatePreferred"
            elif radio_chan_requested_tmp == 3:
                radio_chan_requested = "dualFullRatePreferred"

        if moecallrecord_record.getComponentByName("radioChanUsed") is None:
            radio_chan_used = ""
        else:
            radio_chan_used_tmp = long(str(moecallrecord_record.getComponentByName("radioChanUsed")))
            radio_chan_used = ""
            if radio_chan_used_tmp == 0:
                radio_chan_used = "fullRate"
            elif radio_chan_used_tmp == 1:
                radio_chan_used = "halfRate"

        recording_entity_binascii = binascii.hexlify(str(moecallrecord_record.getComponentByName("recordingEntity")))
        recording_entity_list = []
        recording_entity_data = list(recording_entity_binascii)
        for l in xrange(0, len(recording_entity_data) - 1, 2):
            recording_entity_list.append(recording_entity_data[l + 1] + "" + recording_entity_data[l])
        recording_entity_concatenated = ''.join(recording_entity_list)
        recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
        recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        record_sequence_number = binascii.hexlify(str(moecallrecord_record.getComponentByName("recordSequenceNumber")))

        record_type = long(str(moecallrecord_record.getComponentByName("recordType")))

        release_time_tmp = binascii.hexlify(str(moecallrecord_record.getComponentByName("releaseTime")))
        release_time = parseTimestamp(release_time_tmp)

        if moecallrecord_record.getComponentByName("roamingNumber") is None:
            roaming_number = ""
        else:
            roaming_number_binascii = binascii.hexlify(str(moecallrecord_record.getComponentByName("roamingNumber")))
            roaming_number_list = []
            roaming_number_data = list(roaming_number_binascii)
            for i in xrange(0, len(roaming_number_data) - 1, 2):
                roaming_number_list.append(roaming_number_data[i + 1] + "" + roaming_number_data[i])
            roaming_number_concatenated = ''.join(roaming_number_list)
            roaming_number = roaming_number_concatenated[2:]

        routing_category = str(moecallrecord_record.getComponentByName("routingCategory"))

        seizure_time_tmp = binascii.hexlify(str(moecallrecord_record.getComponentByName("seizureTime")))
        seizure_time = parseTimestamp(seizure_time_tmp)

        if moecallrecord_record.getComponentByName("sequenceNumber") is None:
            sequence_number = 0
        else:
            sequence_number = long(str(moecallrecord_record.getComponentByName("sequenceNumber")))

        served_imei_binascii = binascii.hexlify(str(moecallrecord_record.getComponentByName("servedIMEI")))
        served_imei_list = []
        served_imei_data = list(served_imei_binascii)
        for i in xrange(0, len(served_imei_data) - 1, 2):
            served_imei_list.append(served_imei_data[i + 1] + "" + served_imei_data[i])
        served_imei_concatenated = ''.join(served_imei_list)
        served_imei = served_imei_concatenated.replace("f", "")

        served_imsi_binascii = binascii.hexlify(str(moecallrecord_record.getComponentByName("servedIMSI")))
        served_imsi_list = []
        served_imsi_data = list(served_imsi_binascii)
        for j in xrange(0, len(served_imsi_data) - 1, 2):
            served_imsi_list.append(served_imsi_data[j + 1] + "" + served_imsi_data[j])
        served_imsi_concatenated = ''.join(served_imsi_list)
        served_imsi = served_imsi_concatenated.replace("f", "")

        served_msisdn_binascii = binascii.hexlify(str(moecallrecord_record.getComponentByName("servedMSISDN")))
        served_msisdn_list = []
        served_msisdn_data = list(served_msisdn_binascii)
        for k in xrange(0, len(served_msisdn_data) - 1, 2):
            served_msisdn_list.append(served_msisdn_data[k + 1] + "" + served_msisdn_data[k])
        served_msisdn_concatenated = ''.join(served_msisdn_list)
        served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
        served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        if moecallrecord_record.getComponentByName("serviceKey") is None:
            service_key = 0
        else:
            service_key = long(str(moecallrecord_record.getComponentByName("serviceKey")))

        subscriber_category = binascii.hexlify(str(moecallrecord_record.getComponentByName("subscriberCategory")))

        suppress_camel_ind = str(moecallrecord_record.getComponentByName("suppressCAMELInd"))

        if moecallrecord_record.getComponentByName("systemType") is None:
            system_type = ""
        else:
            system_type_tmp = long(str(moecallrecord_record.getComponentByName("systemType")))
            system_type = ""
            if system_type_tmp == 0:
                system_type = "unknown"
            elif system_type_tmp == 1:
                system_type = "iuUTRAN"
            elif system_type_tmp == 2:
                system_type = "gERAN"

        if moecallrecord_record.getComponentByName("transactionIdentification") is None:
            transaction_identification = 0
        else:
            transaction_identification = long(str(moecallrecord_record.getComponentByName("transactionIdentification")))

        translated_number_binascii = binascii.hexlify(str(moecallrecord_record.getComponentByName("translatedNumber")))
        translated_number_list = []
        translated_number_data = list(translated_number_binascii)
        for l in xrange(0, len(translated_number_data) - 1, 2):
            translated_number_list.append(translated_number_data[l + 1] + "" + translated_number_data[l])
        translated_number_concatenated = ''.join(translated_number_list)
        translated_number = translated_number_concatenated[2:]

        moecallrecord_record = [answer_time, aoc_parametere1, aoc_parametere2, aoc_parametere3, aoc_parametere4,
                                aoc_parametere5, aoc_parametere6, aoc_parametere7, a_release_cause, basic_service,
                                bsdidentification, bsdidentification_24, call_duration, called_number, call_reference,
                                carp, cause_for_termination, change_time, radio_channel,
                                changeofradiochan_speech_version_used, csfbmo_indicator, default_call_handling,
                                diagnostics, dp_number, emlpp_priority_level, emlpp_request_priority_level,
                                exchange_identity, file_name, global_call_reference, gsm_scfaddress, hot_billing_tag,
                                hot_billing_tag2, incoming_cic_channel, incoming_cic_pcmunit, incoming_tkgpname,
                                is_camel_call, iu_release_cause, last_long_part_ind, level_of_camel_service,
                                location_cellid, location_area_code, location_plmn, location_sac, milli_sec_duration,
                                msc_address, msc_incoming_tkgp, ms_classmark, msc_outgoing_tkgp, mscspc14_tmp,
                                mscspc24_tmp, network_call_reference, operator_id, outgoing_cic_channel,
                                outgoing_cic_pcmunit, outgoing_tkgpname, radio_chan_requested, radio_chan_used,
                                recording_entity, record_sequence_number, record_type, release_time, roaming_number,
                                routing_category, seizure_time, sequence_number, served_imei, served_imsi,
                                served_msisdn, service_key, subscriber_category, suppress_camel_ind, system_type,
                                transaction_identification, translated_number]
        return moecallrecord_record
