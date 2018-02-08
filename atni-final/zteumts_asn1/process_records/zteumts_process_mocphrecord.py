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


class ProcessZTEumtsMOCPHRecord:
    def __init__(self):
        pass

    @staticmethod
    def process_mocph_records(mocphrecord_record, file_name):

        additional_chg_info = mocphrecord_record.getComponentByName("additionalChgInfo")

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

        answer_time_tmp = binascii.hexlify(str(mocphrecord_record.getComponentByName("answerTime")))
        answer_time = parseTimestamp(answer_time_tmp)

        if mocphrecord_record.getComponentByName("aocParameters") is not None:
            aoc_parameter = str(mocphrecord_record.getComponentByName("aocParameters"))
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

        if mocphrecord_record.getComponentByName("basicService") is None:
            basic_service = ""
        else:
            basic_service_choice = mocphrecord_record.getComponentByName("basicService")
            basic_service_choice_hexvalue = basic_service_choice.getComponent().prettyPrint()
            if basic_service_choice_hexvalue == "0x11":
                basic_service = "teleservice"
            else:
                basic_service = "bearerservice"

        if mocphrecord_record.getComponentByName("callDuration") is None:
            call_duration = 0
        else:
            call_duration = long(str(mocphrecord_record.getComponentByName("callDuration")))

        called_number_binascii = binascii.hexlify(str(mocphrecord_record.getComponentByName("calledNumber")))
        called_number_list = []
        called_number_data = list(called_number_binascii)
        for i in xrange(0, len(called_number_data) - 1, 2):
            called_number_list.append(called_number_data[i + 1] + "" + called_number_data[i])
        called_number_concatenated = ''.join(called_number_list)
        called_number = called_number_concatenated[2:]

        call_reference = binascii.hexlify(str(mocphrecord_record.getComponentByName("callReference")))

        call_segment_id = long(str(mocphrecord_record.getComponentByName("callSegmentId")))

        if mocphrecord_record.getComponentByName("causeForTerm") is None:
            cause_for_termination = ""
        else:
            cause_for_termination_tmp = long(str(mocphrecord_record.getComponentByName("causeForTerm")))
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

        if mocphrecord_record.getComponentByName("changeOfRadioChan") is not None:
            change_of_radio_chan = mocphrecord_record.getComponentByName("changeOfRadioChan")
            change_time_tmp = binascii.hexlify(str(change_of_radio_chan.getComponentByName("changeTime")))
            change_time = parseTimestamp(change_time_tmp)
            radio_channel = ""
            # if change_of_radio_chan.getComponentByName("radioChannel") is None:
            #     radio_channel = ""
            # else:
            #     radio_channel_tmp = long(str(mtcall_record.getComponentByName("radioChannel")))
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

        if mocphrecord_record.getComponentByName("defaultCallHandling") is None:
            default_call_handling = ""
        else:
            default_call_handling_tmp = long(str(mocphrecord_record.getComponentByName("defaultCallHandling")))
            default_call_handling = ""
            if default_call_handling_tmp == 0:
                default_call_handling = "continueCall"
            elif default_call_handling_tmp == 1:
                default_call_handling = "releaseCall"

        if mocphrecord_record.getComponentByName("diagnostics") is None:
            diagnostics = ""
        else:
            diagnostics = str(mocphrecord_record.getComponentByName("diagnostics"))

        file_name = file_name

        free_format_data = str(mocphrecord_record.getComponentByName("freeFormatData"))

        free_format_data_append = str(mocphrecord_record.getComponentByName("freeFormatDataAppend"))

        global_call_reference = str(mocphrecord_record.getComponentByName("globalCallReference"))

        if mocphrecord_record.getComponentByName("gsm-SCFAddress") is not None:
            gsm_scfaddress_binascii = binascii.hexlify(str(mocphrecord_record.getComponentByName("gsm-SCFAddress")))
            gsm_scfaddress_list = []
            gsm_scfaddress_data = list(gsm_scfaddress_binascii)
            for i in xrange(0, len(gsm_scfaddress_data) - 1, 2):
                gsm_scfaddress_list.append(gsm_scfaddress_data[i + 1] + "" + gsm_scfaddress_data[i])
                gsm_scfaddress_concatenated = ''.join(gsm_scfaddress_list)
                gsm_scfaddress = gsm_scfaddress_concatenated[2:].replace("f", "")
        else:
            gsm_scfaddress = ""

        if mocphrecord_record.getComponentByName("guaranteedBitRate") is None:
            guaranteed_bit_rate = ""
        else:
            guaranteed_bit_rate_tmp = long(str(mocphrecord_record.getComponentByName("guaranteedBitRate")))
            guaranteed_bit_rate = ""
            if guaranteed_bit_rate_tmp == 1:
                guaranteed_bit_rate = "gBR14400BitsPerSecond"
            elif guaranteed_bit_rate_tmp == 2:
                guaranteed_bit_rate = "gBR28800BitsPerSecond"
            elif guaranteed_bit_rate_tmp == 3:
                guaranteed_bit_rate = "gBR32000BitsPerSecond"
            elif guaranteed_bit_rate_tmp == 4:
                guaranteed_bit_rate = "gBR33600BitsPerSecond"
            elif guaranteed_bit_rate_tmp == 5:
                guaranteed_bit_rate = "gBR56000BitsPerSecond"
            elif guaranteed_bit_rate_tmp == 6:
                guaranteed_bit_rate = "gBR57600BitsPerSecond"
            elif guaranteed_bit_rate_tmp == 7:
                guaranteed_bit_rate = "gBR64000BitsPerSecond"
            elif guaranteed_bit_rate_tmp == 8:
                guaranteed_bit_rate = "gBR31200BitsPerSecond"
            elif guaranteed_bit_rate_tmp == 9:
                guaranteed_bit_rate = "gBR38400BitsPerSecond"

        if mocphrecord_record.getComponentByName("hotBillingTag") is None:
            hot_billing_tag = 0
        else:
            hot_billing_tag = long(str(mocphrecord_record.getComponentByName("hotBillingTag")))

        is_camel_call = str(mocphrecord_record.getComponentByName("isCAMELCall"))

        last_long_part_ind = str(mocphrecord_record.getComponentByName("lastLongPartInd"))

        level_of_camel_service = str(mocphrecord_record.getComponentByName("levelOfCAMELService"))

        if mocphrecord_record.getComponentByName("location") is not None:
            location = mocphrecord_record.getComponentByName("location")
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

        if mocphrecord_record.getComponentByName("maximumBitRate") is None:
            maximumbitrate = ""
        else:
            maximumbitrate_tmp = long(str(mocphrecord_record.getComponentByName("maximumBitRate")))
            maximumbitrate = ""
            if maximumbitrate_tmp == 1:
                maximumbitrate = "mBR14400BitsPerSecond"
            elif maximumbitrate_tmp == 2:
                maximumbitrate = "mBR28800BitsPerSecond"
            elif maximumbitrate_tmp == 3:
                maximumbitrate = "mBR32000BitsPerSecond"
            elif maximumbitrate_tmp == 4:
                maximumbitrate = "mBR33600BitsPerSecond"
            elif maximumbitrate_tmp == 5:
                maximumbitrate = "mBR56000BitsPerSecond"
            elif maximumbitrate_tmp == 6:
                maximumbitrate = "mBR57600BitsPerSecond"
            elif maximumbitrate_tmp == 7:
                maximumbitrate = "mBR64000BitsPerSecond"
            elif maximumbitrate_tmp == 8:
                maximumbitrate = "mBR67200BitsPerSecond"

        if mocphrecord_record.getComponentByName("millisecDuration") is None:
            milli_sec_duration = 0
        else:
            milli_sec_duration = long(str(mocphrecord_record.getComponentByName("millisecDuration")))

        msc_address = str(mocphrecord_record.getComponentByName("mSCAddress"))

        msc_incoming_tkgp = binascii.hexlify(str(mocphrecord_record.getComponentByName("mscIncomingTKGP")))

        ms_classmark = binascii.hexlify(str(mocphrecord_record.getComponentByName("msClassmark")))

        if mocphrecord_record.getComponentByName("networkCallReference") is None:
            network_call_reference = ""
        else:
            network_call_reference = binascii.hexlify(
                    str(mocphrecord_record.getComponentByName("networkCallReference")))

        if mocphrecord_record.getComponentByName("numberOfDPEncountered") is None:
            number_of_dpencountered = 0
        else:
            number_of_dpencountered = long(str(mocphrecord_record.getComponentByName("numberOfDPEncountered")))

        if mocphrecord_record.getComponentByName("partialRecordType") is None:
            partial_record_type = ""
        else:
            partial_record_type_tmp = long(str(mocphrecord_record.getComponentByName("partialRecordType")))
            partial_record_type = ""
            if partial_record_type_tmp == 1:
                partial_record_type = "serviceChange"

        if mocphrecord_record.getComponentByName("partSequenceNumber") is None:
            part_sequence_number = 0
        else:
            part_sequence_number = long(str(mocphrecord_record.getComponentByName("partSequenceNumber")))

        if mocphrecord_record.getComponentByName("radioChanRequested") is None:
            radio_chan_requested = ""
        else:

            radio_chan_requested_tmp = long(str(mocphrecord_record.getComponentByName("radioChanRequested")))
            radio_chan_requested = ""
            if radio_chan_requested_tmp == 0:
                radio_chan_requested = "halfRateChannel"
            elif radio_chan_requested_tmp == 1:
                radio_chan_requested = "fullRateChannel"
            elif radio_chan_requested_tmp == 2:
                radio_chan_requested = "dualHalfRatePreferred"
            elif radio_chan_requested_tmp == 3:
                radio_chan_requested = "dualFullRatePreferred"

        if mocphrecord_record.getComponentByName("radioChanUsed") is None:
            radio_chan_used = ""
        else:
            radio_chan_used_tmp = long(str(mocphrecord_record.getComponentByName("radioChanUsed")))
            radio_chan_used = ""
            if radio_chan_used_tmp == 0:
                radio_chan_used = "fullRate"
            elif radio_chan_used_tmp == 1:
                radio_chan_used = "halfRate"

        recording_entity_binascii = binascii.hexlify(str(mocphrecord_record.getComponentByName("recordingEntity")))
        recording_entity_list = []
        recording_entity_data = list(recording_entity_binascii)
        for l in xrange(0, len(recording_entity_data) - 1, 2):
            recording_entity_list.append(recording_entity_data[l + 1] + "" + recording_entity_data[l])
        recording_entity_concatenated = ''.join(recording_entity_list)
        recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
        recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        record_sequence_number = binascii.hexlify(str(mocphrecord_record.getComponentByName("recordSequenceNumber")))

        record_type = long(str(mocphrecord_record.getComponentByName("recordType")))

        release_time_tmp = binascii.hexlify(str(mocphrecord_record.getComponentByName("releaseTime")))
        release_time = parseTimestamp(release_time_tmp)

        roaming_number_binascii = binascii.hexlify(str(mocphrecord_record.getComponentByName("roamingNumber")))
        roaming_number_list = []
        roaming_number_data = list(roaming_number_binascii)
        for i in xrange(0, len(roaming_number_data) - 1, 2):
            roaming_number_list.append(roaming_number_data[i + 1] + "" + roaming_number_data[i])
        roaming_number_concatenated = ''.join(roaming_number_list)
        roaming_number = roaming_number_concatenated[2:]

        seizure_time_tmp = binascii.hexlify(str(mocphrecord_record.getComponentByName("seizureTime")))
        seizure_time = parseTimestamp(seizure_time_tmp)

        served_imei_binascii = binascii.hexlify(str(mocphrecord_record.getComponentByName("servedIMEI")))
        served_imei_list = []
        served_imei_data = list(served_imei_binascii)
        for i in xrange(0, len(served_imei_data) - 1, 2):
            served_imei_list.append(served_imei_data[i + 1] + "" + served_imei_data[i])
        served_imei_concatenated = ''.join(served_imei_list)
        served_imei = served_imei_concatenated.replace("f", "")

        served_imsi_binascii = binascii.hexlify(str(mocphrecord_record.getComponentByName("servedIMSI")))
        served_imsi_list = []
        served_imsi_data = list(served_imsi_binascii)
        for j in xrange(0, len(served_imsi_data) - 1, 2):
            served_imsi_list.append(served_imsi_data[j + 1] + "" + served_imsi_data[j])
        served_imsi_concatenated = ''.join(served_imsi_list)
        served_imsi = served_imsi_concatenated.replace("f", "")

        served_msisdn_binascii = binascii.hexlify(str(mocphrecord_record.getComponentByName("servedMSISDN")))
        served_msisdn_list = []
        served_msisdn_data = list(served_msisdn_binascii)
        for k in xrange(0, len(served_msisdn_data) - 1, 2):
            served_msisdn_list.append(served_msisdn_data[k + 1] + "" + served_msisdn_data[k])
        served_msisdn_concatenated = ''.join(served_msisdn_list)
        served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
        served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        if mocphrecord_record.getComponentByName("serviceKey") is None:
            service_key = ""
        else:
            service_key = str(mocphrecord_record.getComponentByName("serviceKey"))

        speech_version_supported = str(mocphrecord_record.getComponentByName("speechVersionSupported"))

        speech_version_used = str(mocphrecord_record.getComponentByName("speechVersionUsed"))

        subscriber_category = binascii.hexlify(str(mocphrecord_record.getComponentByName("subscriberCategory")))

        if mocphrecord_record.getComponentByName("systemType") is None:
            system_type = ""
        else:
            system_type_tmp = long(str(mocphrecord_record.getComponentByName("systemType")))
            system_type = ""
            if system_type_tmp == 0:
                system_type = "unknown"
            elif system_type_tmp == 1:
                system_type = "iuUTRAN"
            elif system_type_tmp == 2:
                system_type = "gERAN"

        mocphrecord_record = [charged_party, charge_indicator, answer_time, aoc_parametere1, aoc_parametere2,
                              aoc_parametere3, aoc_parametere4, aoc_parametere5, aoc_parametere6, aoc_parametere7,
                              basic_service, call_duration, called_number, call_reference, call_segment_id,
                              cause_for_termination, change_time, radio_channel, changeofradiochan_speech_version_used,
                              default_call_handling, diagnostics, file_name, free_format_data, free_format_data_append,
                              global_call_reference, gsm_scfaddress, guaranteed_bit_rate, hot_billing_tag, is_camel_call
            , last_long_part_ind, level_of_camel_service, location_cellid, location_area_code,
                              location_plmn, location_sac, maximumbitrate, milli_sec_duration, msc_address,
                              msc_incoming_tkgp, ms_classmark, network_call_reference, number_of_dpencountered,
                              partial_record_type, part_sequence_number, radio_chan_requested, radio_chan_used,
                              recording_entity, record_sequence_number, record_type, release_time, roaming_number,
                              seizure_time, served_imei, served_imsi, served_msisdn, service_key,
                              speech_version_supported, speech_version_used,
                              subscriber_category, system_type]
        return mocphrecord_record
