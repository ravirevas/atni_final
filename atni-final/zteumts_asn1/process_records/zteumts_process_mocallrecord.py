import binascii

from common.utilities import parseTimestamp, append_timezone_offset, parse_start_time, boolean_value


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


class ProcessZTEumtsMOCallRecord:
    def __init__(self):
        pass

    @staticmethod
    def process_mocall_records(mocallrecord_record, file_id, file_name):

        if mocallrecord_record.getComponentByName("recordType") is None:
            record_type = -255
        else:
            record_type = long(str(mocallrecord_record.getComponentByName("recordType")))

        additional_chg_info = mocallrecord_record.getComponentByName("additionalChgInfo")

        if additional_chg_info is None:
            charge_indicator = ""
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

        if mocallrecord_record.getComponentByName("aocParameters") is not None:
            aoc_parameter = str(mocallrecord_record.getComponentByName("aocParameters"))
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

        if mocallrecord_record.getComponentByName("aiurRequested") is None:
            aiur_requested = ""
        else:
            aiur_requested_tmp = long(str(mocallrecord_record.getComponentByName("aiurRequested")))
            aiur_requested = ""
            if aiur_requested_tmp == 1:
                aiur_requested = "aiur09600BitsPerSecond"
            elif aiur_requested_tmp == 2:
                aiur_requested = "aiur14400BitsPerSecond"
            elif aiur_requested_tmp == 3:
                aiur_requested = "aiur19200BitsPerSecond"
            elif aiur_requested_tmp == 5:
                aiur_requested = "aiur28800BitsPerSecond"
            elif aiur_requested_tmp == 6:
                aiur_requested = "aiur38400BitsPerSecond"
            elif aiur_requested_tmp == 7:
                aiur_requested = "aiur43200BitsPerSecond"
            elif aiur_requested_tmp == 8:
                aiur_requested = "aiur57600BitsPerSecond"
            elif aiur_requested_tmp == 9:
                aiur_requested = "aiur38400BitsPerSecond1"
            elif aiur_requested_tmp == 10:
                aiur_requested = "aiur38400BitsPerSecond2"
            elif aiur_requested_tmp == 11:
                aiur_requested = "aiur38400BitsPerSecond3"
            elif aiur_requested_tmp == 12:
                aiur_requested = "aiur38400BitsPerSecond4"

        cug_category = mocallrecord_record.getComponentByName("cUGCategory")

        if cug_category is None:
            cug_category_calltype = -255
        else:
            cug_category_calltype = long(str(cug_category.getComponentByName("callType")))

        if cug_category is None:
            cug_category_usertype = -255
        else:
            cug_category_usertype = long(str(cug_category.getComponentByName("userType")))

        cug_outgoing_accessind_get_value = mocallrecord_record.getComponentByName("cUGOutgoingAccessInd")
        cug_outgoing_accessind = boolean_value(cug_outgoing_accessind_get_value)

        cug_outgoing_accessused_get_value = mocallrecord_record.getComponentByName("cUGOutgoingAccessUsed")
        cug_outgoing_accessused = boolean_value(cug_outgoing_accessused_get_value)

        if mocallrecord_record.getComponentByName("servedIMEI") is None:
            served_imei = ""
        else:
            served_imei_binascii = binascii.hexlify(str(mocallrecord_record.getComponentByName("servedIMEI")))
            served_imei_list = []
            served_imei_data = list(served_imei_binascii)
            for i in xrange(0, len(served_imei_data) - 1, 2):
                served_imei_list.append(served_imei_data[i + 1] + "" + served_imei_data[i])
            served_imei_concatenated = ''.join(served_imei_list)
            served_imei = served_imei_concatenated.replace("f", "")

        if mocallrecord_record.getComponentByName("servedIMSI") is None:
            served_imsi = ""
        else:
            served_imsi_binascii = binascii.hexlify(str(mocallrecord_record.getComponentByName("servedIMSI")))
            served_imsi_list = []
            served_imsi_data = list(served_imsi_binascii)
            for j in xrange(0, len(served_imsi_data) - 1, 2):
                served_imsi_list.append(served_imsi_data[j+1] + "" + served_imsi_data[j])
            served_imsi_concatenated = ''.join(served_imsi_list)
            served_imsi = served_imsi_concatenated.replace("f", "")

        if mocallrecord_record.getComponentByName("servedMSISDN") is None:
            served_msisdn = ""
        else:
            served_msisdn_binascii = binascii.hexlify(str(mocallrecord_record.getComponentByName("servedMSISDN")))
            served_msisdn_list = []
            served_msisdn_data = list(served_msisdn_binascii)
            for k in xrange(0, len(served_msisdn_data) - 1, 2):
                served_msisdn_list.append(served_msisdn_data[k+1] + "" + served_msisdn_data[k])
            served_msisdn_concatenated = ''.join(served_msisdn_list)
            served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
            served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        if mocallrecord_record.getComponentByName("calledNumber") is None:
            called_number = ""
        else:
            called_number_binascii = binascii.hexlify(str(mocallrecord_record.getComponentByName("calledNumber")))
            called_number_list = []
            called_number_data = list(called_number_binascii)
            for i in xrange(0, len(called_number_data) - 1, 2):
                called_number_list.append(called_number_data[i + 1] + "" + called_number_data[i])
            called_number_concatenated = ''.join(called_number_list)
            called_number = called_number_concatenated[2:]

        if mocallrecord_record.getComponentByName("translatedNumber") is None:
            translated_number = ""
        else:
            translated_number_binascii = binascii.hexlify(str(mocallrecord_record.getComponentByName("translatedNumber")))
            translated_number_list = []
            translated_number_data = list(translated_number_binascii)
            for l in xrange(0, len(translated_number_data) - 1, 2):
                translated_number_list.append(translated_number_data[l + 1] + "" + translated_number_data[l])
            translated_number_concatenated = ''.join(translated_number_list)
            translated_number = translated_number_concatenated[2:]

        if mocallrecord_record.getComponentByName("connectedNumber") is None:
            connected_number = ""
        else:
            connected_number_binascii = binascii.hexlify(str(mocallrecord_record.getComponentByName("connectedNumber")))
            connected_number_list = []
            connected_number_data = list(connected_number_binascii)
            for i in xrange(0, len(connected_number_data) - 1, 2):
                connected_number_list.append(connected_number_data[i + 1] + "" + connected_number_data[i])
            connected_number_concatenated = ''.join(connected_number_list)
            connected_number = connected_number_concatenated[2:]

        if mocallrecord_record.getComponentByName("roamingNumber") is None:
            roaming_number = ""
        else:
            roaming_number_binascii = binascii.hexlify(str(mocallrecord_record.getComponentByName("roamingNumber")))
            roaming_number_list = []
            roaming_number_data = list(roaming_number_binascii)
            for i in xrange(0, len(roaming_number_data) - 1, 2):
                roaming_number_list.append(roaming_number_data[i + 1] + "" + roaming_number_data[i])
            roaming_number_concatenated = ''.join(roaming_number_list)
            roaming_number = roaming_number_concatenated[2:]

        if mocallrecord_record.getComponentByName("recordingEntity") is None:
            recording_entity = ""
        else:
            recording_entity_binascii = binascii.hexlify(str(mocallrecord_record.getComponentByName("recordingEntity")))
            recording_entity_list = []
            recording_entity_data = list(recording_entity_binascii)
            for l in xrange(0, len(recording_entity_data) - 1, 2):
                recording_entity_list.append(recording_entity_data[l+1] + "" + recording_entity_data[l])
            recording_entity_concatenated = ''.join(recording_entity_list)
            recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
            recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        if mocallrecord_record.getComponentByName("mscIncomingTKGP") is None:
            msc_incoming_tkgp = ""
        else:
            msc_incoming_tkgp = binascii.hexlify(str(mocallrecord_record.getComponentByName("mscIncomingTKGP")))

        if mocallrecord_record.getComponentByName("calledLocation") is not None:
            called_location = mocallrecord_record.getComponentByName("calledLocation")
            called_location_area = binascii.hexlify(str(called_location.getComponentByName("locationArea")))
            called_location_area_str = str(called_location_area)

            called_location_cellid_binascii = called_location_area_str[10:14]
            called_format_location_cellid_binascii = int(called_location_cellid_binascii, 16)
            called_location_cellid = format(called_format_location_cellid_binascii, '05')

            called_location_area_code_binascii = called_location_area_str[6:10]
            called_format_location_area_code_binascii = int(called_location_area_code_binascii, 16)
            called_location_area_code = format(called_format_location_area_code_binascii, '05')

            called_location_plmn = parseMcc(called_location_area_str[0:6]) + parseMnc(called_location_area_str[0:6])

            called_location_sac = called_location_area_str[-4:]
        else:
            called_location_cellid = ""
            called_location_area_code = ""
            called_location_plmn = ""
            called_location_sac = ""

        if mocallrecord_record.getComponentByName("basicService") is None:
            basic_service = ""
        else:
            basic_service_choice = mocallrecord_record.getComponentByName("basicService")
            basic_service_choice_hexvalue = basic_service_choice.getComponent().prettyPrint()
            if basic_service_choice_hexvalue == "0x11":
                basic_service = "teleservice"
            else:
                basic_service = "bearerservice"

        if mocallrecord_record.getComponentByName("transparencyIndicator") is None:
            transparency_indicator = ""
        else:
            transparency_indicator_tmp = long(str(mocallrecord_record.getComponentByName("transparencyIndicator")))
            transparency_indicator = ""
            if transparency_indicator_tmp == 0:
                transparency_indicator = "transparent"
            elif transparency_indicator_tmp == 1:
                transparency_indicator = "nonTransparent"

        if mocallrecord_record.getComponentByName("mscOutgoingTKGP") is None:
            msc_outgoing_tkgp = -255
        else:
            msc_outgoing_tkgp = binascii.hexlify(str(mocallrecord_record.getComponentByName("mscOutgoingTKGP")))

        seizure_time_tmp = binascii.hexlify(str(mocallrecord_record.getComponentByName("seizureTime")))
        seizure_time = append_timezone_offset(parseTimestamp(seizure_time_tmp))

        answer_time_tmp = binascii.hexlify(str(mocallrecord_record.getComponentByName("answerTime")))
        answer_time = append_timezone_offset(parseTimestamp(answer_time_tmp))

        release_time_tmp = binascii.hexlify(str(mocallrecord_record.getComponentByName("releaseTime")))
        release_time = append_timezone_offset(parseTimestamp(release_time_tmp))

        if mocallrecord_record.getComponentByName("callDuration") is None:
            call_duration = -255
        else:
            call_duration = long(str(mocallrecord_record.getComponentByName("callDuration")))

        if mocallrecord_record.getComponentByName("dataVolume") is None:
            data_volume = -255
        else:
            data_volume = long(str(mocallrecord_record.getComponentByName("dataVolume")))

        if mocallrecord_record.getComponentByName("radioChanRequested") is None:
            radio_chan_requested = ""
        else:
            radio_chan_requested_tmp = long(str(mocallrecord_record.getComponentByName("radioChanRequested")))
            radio_chan_requested = ""
            if radio_chan_requested_tmp == 0:
                radio_chan_requested = "halfRateChannel"
            elif radio_chan_requested_tmp == 1:
                radio_chan_requested = "fullRateChannel"
            elif radio_chan_requested_tmp == 2:
                radio_chan_requested = "dualHalfRatePreferred"
            elif radio_chan_requested_tmp == 3:
                radio_chan_requested = "dualFullRatePreferred"

        if mocallrecord_record.getComponentByName("radioChanUsed") is None:
            radio_chan_used = ""
        else:
            radio_chan_used_tmp = long(str(mocallrecord_record.getComponentByName("radioChanUsed")))
            radio_chan_used = ""
            if radio_chan_used_tmp == 0:
                radio_chan_used = "fullRate"
            elif radio_chan_used_tmp == 1:
                radio_chan_used = "halfRate"

        if mocallrecord_record.getComponentByName("changeOfRadioChan") is not None:
            change_of_radio_chan = mocallrecord_record.getComponentByName("changeOfRadioChan")
            if change_of_radio_chan.getComponentByName("changeTime") is None:
                change_time = parse_start_time(file_name)
            else:
                change_time_tmp = binascii.hexlify(str(change_of_radio_chan.getComponentByName("changeTime")))
                change_time = append_timezone_offset(parseTimestamp(change_time_tmp))
            if change_of_radio_chan.getComponentByName("radioChannel") is None:
                radio_channel = ""
            else:
                radio_channel_tmp = long(str(change_of_radio_chan.getComponentByName("radioChannel")))
                radio_channel = ""
                if radio_channel_tmp == 0:
                    radio_channel = "fullRate"
                elif radio_channel_tmp == 1:
                    radio_channel = "halfRate"
            if change_of_radio_chan.getComponentByName("speechVersionUsed") is None:
                changeofradiochan_speech_version_used = ""
            else:
                changeofradiochan_speech_version_used = str(change_of_radio_chan.getComponentByName("speechVersionUsed"))
        else:
            radio_channel = ""
            change_time = parse_start_time(file_name)
            changeofradiochan_speech_version_used = ""

        if mocallrecord_record.getComponentByName("channelType") is None:
            channel_type = ""
        else:
            channel_type = str(mocallrecord_record.getComponentByName("channelType"))

        if mocallrecord_record.getComponentByName("causeForTerm") is None:
            cause_for_termination = ""
        else:
            cause_for_termination_tmp = long(str(mocallrecord_record.getComponentByName("causeForTerm")))
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

        if mocallrecord_record.getComponentByName("chanCodingUsed") is None:
            chan_coding_used = ""
        else:
            chan_coding_used_tmp = long(str(mocallrecord_record.getComponentByName("chanCodingUsed")))
            chan_coding_used = ""
            if chan_coding_used_tmp == 1:
                chan_coding_used = "tchF4800"
            elif chan_coding_used_tmp == 2:
                chan_coding_used = "tchF9600"
            elif chan_coding_used_tmp == 3:
                chan_coding_used = "tchF14400"

        if mocallrecord_record.getComponentByName("diagnostics") is None:
            diagnostics = ""
        else:
            diagnostics = mocallrecord_record.getComponentByName("diagnostics").getName()

        if mocallrecord_record.getComponentByName("callReference") is None:
            call_reference = ""
        else:
            call_reference = binascii.hexlify(str(mocallrecord_record.getComponentByName("callReference")))

        if mocallrecord_record.getComponentByName("sequenceNumber") is None:
            sequence_number = -255
        else:
            sequence_number = long(str(mocallrecord_record.getComponentByName("sequenceNumber")))

        last_long_part_ind_get_value = mocallrecord_record.getComponentByName("lastLongPartInd")
        last_long_part_ind = boolean_value(last_long_part_ind_get_value)

        is_camel_call_get_value = mocallrecord_record.getComponentByName("isCAMELCall")
        is_camel_call = boolean_value(is_camel_call_get_value)

        if mocallrecord_record.getComponentByName("gsm-SCFAddress") is not None:
            gsm_scfaddress_binascii = binascii.hexlify(str(mocallrecord_record.getComponentByName("gsm-SCFAddress")))
            gsm_scfaddress_list = []
            gsm_scfaddress_data = list(gsm_scfaddress_binascii)
            for i in xrange(0, len(gsm_scfaddress_data) - 1, 2):
                gsm_scfaddress_list.append(gsm_scfaddress_data[i + 1] + "" + gsm_scfaddress_data[i])
                gsm_scfaddress_concatenated = ''.join(gsm_scfaddress_list)
                gsm_scfaddress = gsm_scfaddress_concatenated[2:].replace("f", "")
        else:
            gsm_scfaddress = ""

        if mocallrecord_record.getComponentByName("serviceKey") is None:
            service_key = -255
        else:
            service_key = long(str(mocallrecord_record.getComponentByName("serviceKey")))

        if mocallrecord_record.getComponentByName("networkCallReference") is None:
            network_call_reference = ""
        else:
            network_call_reference = binascii.hexlify(
                str(mocallrecord_record.getComponentByName("networkCallReference")))

        if mocallrecord_record.getComponentByName("mSCAddress") is None:
            msc_address = ""
        else:
            msc_address_binascii = binascii.hexlify(str(mocallrecord_record.getComponentByName("mSCAddress")))
            msc_address_list = []
            msc_address_data = list(msc_address_binascii)
            for i in xrange(0, len(msc_address_data) - 1, 2):
                msc_address_list.append(msc_address_data[i + 1] + "" + msc_address_data[i])
                msc_address_concatenated = ''.join(msc_address_list)
                msc_address = msc_address_concatenated[2:].replace("f", "")

        if mocallrecord_record.getComponentByName("defaultCallHandling") is None:
            default_call_handling = ""
        else:
            default_call_handling_tmp = long(str(mocallrecord_record.getComponentByName("defaultCallHandling")))
            default_call_handling = ""
            if default_call_handling_tmp == 0:
                default_call_handling = "continueCall"
            elif default_call_handling_tmp == 1:
                default_call_handling = "releaseCall"

        is_hscsd_used_get_value = mocallrecord_record.getComponentByName("isHSCSDUsed")
        is_hscsd_used = boolean_value(is_hscsd_used_get_value)

        if mocallrecord_record.getComponentByName("hSCSDChanRequested") is None:
            hscsd_chan_requested = -255
        else:
            hscsd_chan_requested = long(str(mocallrecord_record.getComponentByName("hSCSDChanRequested")))

        if mocallrecord_record.getComponentByName("hSCSDChanAllocated") is None:
            hscsd_chan_allocated = -255
        else:
            hscsd_chan_allocated = long(str(mocallrecord_record.getComponentByName("hSCSDChanAllocated")))

        if mocallrecord_record.getComponentByName("fnur") is None:
            fnur = ""
        else:
            fnur_tmp = long(str(mocallrecord_record.getComponentByName("fnur")))
            if fnur_tmp == 0:
                fnur = "fnur9600-BitsPerSecond"
            elif fnur_tmp == 1:
                fnur = "fnur14400BitsPerSecond"
            elif fnur_tmp == 2:
                fnur = "fnur19200BitsPerSecond"
            elif fnur_tmp == 3:
                fnur = "fnur14400BitsPerSecond"
            elif fnur_tmp == 4:
                fnur = "fnur28800BitsPerSecond"
            elif fnur_tmp == 5:
                fnur = "fnur38400BitsPerSecond"
            elif fnur_tmp == 6:
                fnur = "fnur48000BitsPerSecond"
            elif fnur_tmp == 7:
                fnur = "fnur56000BitsPerSecond"
            elif fnur_tmp == 8:
                fnur = "fnur64000BitsPerSecond"
            elif fnur_tmp == 9:
                fnur = "fnur33600BitsPerSecond"
            elif fnur_tmp == 10:
                fnur = "fnur32000BitsPerSecond"

        incoming_cic = mocallrecord_record.getComponentByName("incomingCic")

        if incoming_cic is None:
            incoming_cic_channel = -255
        else:
            incoming_cic_channel = long(str(incoming_cic.getComponentByName("channel")))

        if incoming_cic is None:
            incoming_cic_pcmunit = -255
        else:
            incoming_cic_pcmunit = long(str(incoming_cic.getComponentByName("pcmUnit")))

        outgoing_cic = mocallrecord_record.getComponentByName("outgoingCic")

        if outgoing_cic is None:
            outgoing_cic_channel = -255
        else:
            outgoing_cic_channel = long(str(outgoing_cic.getComponentByName("channel")))

        if outgoing_cic is None:
            outgoing_cic_pcmunit = -255
        else:
            outgoing_cic_pcmunit = long(str(outgoing_cic.getComponentByName("pcmUnit")))

        if mocallrecord_record.getComponentByName("typeOfServiceChange") is None:
            types_of_service_change = ""
        else:
            types_of_service_change_tmp = long(str(mocallrecord_record.getComponentByName("typeOfServiceChange")))
            types_of_service_change = ""
            if types_of_service_change_tmp == 0:
                types_of_service_change = "changeToSpeech"
            elif types_of_service_change_tmp == 1:
                types_of_service_change = "changeToMultimedia"

        if mocallrecord_record.getComponentByName("numberOfDPEncountered") is None:
            number_of_dp_encountered = -255
        else:
            number_of_dp_encountered = long(str(mocallrecord_record.getComponentByName("numberOfDPEncountered")))

        if mocallrecord_record.getComponentByName("levelOfCAMELService") is None:
            level_of_camel_service = ""
        else:
            level_of_camel_service_tmp = mocallrecord_record.getComponentByName("levelOfCAMELService").prettyPrint()
            level_of_camel_service = level_of_camel_service_tmp[2:5]

        if mocallrecord_record.getComponentByName("freeFormatData") is None:
            free_format_data = ""
        else:
            free_format_data = str(mocallrecord_record.getComponentByName("freeFormatData"))

        if mocallrecord_record.getComponentByName("freeFormatDataAppend") is None:
            free_format_data_append = ""
        else:
            free_format_data_append = str(mocallrecord_record.getComponentByName("freeFormatDataAppend"))

        if mocallrecord_record.getComponentByName("defaultCallHandling_2") is None:
            default_call_handling2 = ""
        else:
            default_call_handling2_tmp = str(mocallrecord_record.getComponentByName("defaultCallHandling_2"))
            default_call_handling2 = ""
            if default_call_handling2_tmp == 0:
                default_call_handling2 = "continueCall"
            elif default_call_handling2_tmp == 1:
                default_call_handling2 = "releaseCall"

        if mocallrecord_record.getComponentByName("gsm-SCFAddress_2") is not None:
            gsm_scfaddress_2_binascii = binascii.hexlify(
                str(mocallrecord_record.getComponentByName("gsm-SCFAddress_2")))
            gsm_scfaddress_2_list = []
            gsm_scfaddress_2_data = list(gsm_scfaddress_2_binascii)
            for i in xrange(0, len(gsm_scfaddress_2_data) - 1, 2):
                gsm_scfaddress_2_list.append(gsm_scfaddress_2_data[i + 1] + "" + gsm_scfaddress_2_data[i])
                gsm_scfaddress_2_concatenated = ''.join(gsm_scfaddress_2_list)
                gsm_scfaddress_2 = gsm_scfaddress_2_concatenated[2:].replace("f", "")
        else:
            gsm_scfaddress_2 = ""

        if mocallrecord_record.getComponentByName("serviceKey_2") is None:
            service_key_2 = -255
        else:
            service_key_2 = long(str(mocallrecord_record.getComponentByName("serviceKey_2")))

        if mocallrecord_record.getComponentByName("freeFormatData_2") is None:
            free_format_data_2 = ""
        else:
            free_format_data_2 = str(mocallrecord_record.getComponentByName("freeFormatData_2"))

        if mocallrecord_record.getComponentByName("freeFormatDataAppend_2") is None:
            free_format_data_append_2 = ""
        else:
            free_format_data_append_2 = str(mocallrecord_record.getComponentByName("freeFormatDataAppend_2"))

        if mocallrecord_record.getComponentByName("systemType") is None:
            system_type = ""
        else:
            system_type_tmp = long(str(mocallrecord_record.getComponentByName("systemType")))
            system_type = ""
            if system_type_tmp == 0:
                system_type = "unknown"
            elif system_type_tmp == 1:
                system_type = "iuUTRAN"
            elif system_type_tmp == 2:
                system_type = "gERAN"

        if mocallrecord_record.getComponentByName("exchangeIdentity") is None:
            exchange_identity = ""
        else:
            exchange_identity = str(mocallrecord_record.getComponentByName("exchangeIdentity"))

        if mocallrecord_record.getComponentByName("dialledNumber") is None:
            dialled_number = ""
        else:
            dialled_number_binascii = binascii.hexlify(str(mocallrecord_record.getComponentByName("dialledNumber")))
            dialled_number_list = []
            dialled_number_data = list(dialled_number_binascii)
            for i in xrange(0, len(dialled_number_data) - 1, 2):
                dialled_number_list.append(dialled_number_data[i + 1] + "" + dialled_number_data[i])
            dialled_number_concatenated = ''.join(dialled_number_list)
            dialled_number = dialled_number_concatenated[2:].replace("f", "")

        if mocallrecord_record.getComponentByName("subscriberCategory") is None:
            subscriber_category = ""
        else:
            subscriber_category = binascii.hexlify(str(mocallrecord_record.getComponentByName("subscriberCategory")))

        if mocallrecord_record.getComponentByName("eMLPPPriorityLevel") is None:
            emlpp_priority_level = -255
        else:
            emlpp_priority_level = long(str(mocallrecord_record.getComponentByName("eMLPPPriorityLevel")))

        if mocallrecord_record.getComponentByName("eMLPPRequestPriorityLevel") is None:
            emlpp_request_priority_level = -255
        else:
            emlpp_request_priority_level = long(
                str(mocallrecord_record.getComponentByName("eMLPPRequestPriorityLevel")))

        if mocallrecord_record.getComponentByName("hotBillingTag") is None:
            hot_billing_tag = -255
        else:
            hot_billing_tag = long(str(mocallrecord_record.getComponentByName("hotBillingTag")))

        is_cug_used_get_value = mocallrecord_record.getComponentByName("isCUGUsed")
        is_cug_used = boolean_value(is_cug_used_get_value)

        if mocallrecord_record.getComponentByName("cUGInterLockCode") is None:
            cug_interlock_code = ""
        else:
            cug_interlock_code = str(mocallrecord_record.getComponentByName("cUGInterLockCode"))

        if mocallrecord_record.getComponentByName("dataRate") is None:
            data_rate = -255
        else:
            data_rate = long(str(mocallrecord_record.getComponentByName("dataRate")))

        is_multi_media_call_get_value = mocallrecord_record.getComponentByName("isMultiMediaCall")
        is_multi_media_call = boolean_value(is_multi_media_call_get_value)

        if mocallrecord_record.getComponentByName("rateIndication") is None:
            rate_indication = ""
        else:
            rate_indication = str(mocallrecord_record.getComponentByName("rateIndication"))

        if mocallrecord_record.getComponentByName("guaranteedBitRate") is None:
            guaranteed_bit_rate = ""
        else:
            guaranteed_bit_rate_tmp = long(str(mocallrecord_record.getComponentByName("guaranteedBitRate")))
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

        if mocallrecord_record.getComponentByName("maximumBitRate") is None:
            maximumbitrate = ""
        else:
            maximumbitrate_tmp = long(str(mocallrecord_record.getComponentByName("maximumBitRate")))
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

        if mocallrecord_record.getComponentByName("pbrt") is None:
            pbrt = -255
        else:
            pbrt = long(str(mocallrecord_record.getComponentByName("pbrt")))

        if mocallrecord_record.getComponentByName("carp") is None:
            carp = -255
        else:
            carp = long(str(mocallrecord_record.getComponentByName("carp")))

        if cug_category is None:
            cug_index = -255
        else:
            cug_index = long(str(cug_category.getComponentByName("cUGIndex")))

        if mocallrecord_record.getComponentByName("hotBillingTag2") is None:
            hot_billing_tag2 = -255
        else:
            hot_billing_tag2 = long(str(mocallrecord_record.getComponentByName("hotBillingTag2")))

        if mocallrecord_record.getComponentByName("dDCFlag") is None:
            ddcflag = -255
        else:
            ddcflag = long(str(mocallrecord_record.getComponentByName("dDCFlag")))

        if mocallrecord_record.getComponentByName("bSCIdentification") is None:
            bsdidentification = ""
        else:
            bsdidentification = binascii.hexlify(str(mocallrecord_record.getComponentByName("bSCIdentification")))

        if mocallrecord_record.getComponentByName("callingPartyRoamingInd") is None:
            calling_party_roamingind = ""
        else:
            calling_party_roamingind_int = str(mocallrecord_record.getComponentByName("callingPartyRoamingInd"))
            if calling_party_roamingind_int == "1":
                calling_party_roamingind = "TRUE"
            else:
                calling_party_roamingind = "FALSE"

        if mocallrecord_record.getComponentByName("serviceCategory") is None:
            service_category = ""
        else:
            service_category = binascii.hexlify(str(mocallrecord_record.getComponentByName("serviceCategory")))

        if mocallrecord_record.getComponentByName("transactionIdentification") is None:
            transaction_identification = -255
        else:
            transaction_identification = long(str(mocallrecord_record.getComponentByName("transactionIdentification")))

        if mocallrecord_record.getComponentByName("mscSPC14") is None:
            mscspc14 = ""
        else:
            mscspc14 = binascii.hexlify(str(mocallrecord_record.getComponentByName("mscSPC14")))

        if mocallrecord_record.getComponentByName("mscSPC24") is None:
            mscspc24 = ""
        else:
            mscspc24 = binascii.hexlify(str(mocallrecord_record.getComponentByName("mscSPC24")))

        if mocallrecord_record.getComponentByName("incomingTKGPName") is None:
            incoming_tkgpname = ""
        else:
            incoming_tkgpname = str(mocallrecord_record.getComponentByName("incomingTKGPName"))

        if mocallrecord_record.getComponentByName("outgoingTKGPName") is None:
            outgoing_tkgpname = ""
        else:
            outgoing_tkgpname = str(mocallrecord_record.getComponentByName("outgoingTKGPName"))

        if mocallrecord_record.getComponentByName("partialRecordType") is None:
            partial_record_type = ""
        else:
            partial_record_type_tmp = long(str(mocallrecord_record.getComponentByName("partialRecordType")))
            partial_record_type = ""
            if partial_record_type_tmp == 1:
                partial_record_type = "serviceChange"

        if mocallrecord_record.getComponentByName("serviceChangeInitiator") is None:
            service_change_initiator = -255
        else:
            service_change_initiator = long(str(mocallrecord_record.getComponentByName("serviceChangeInitiator")))

        if mocallrecord_record.getComponentByName("reasonForServiceChange") is None:
            reason_for_service_change = -255
        else:
            reason_for_service_change_tmp = long(str(mocallrecord_record.getComponentByName("reasonForServiceChange")))
            reason_for_service_change = ""
            if reason_for_service_change_tmp == 0:
                reason_for_service_change = "msubInitiate"
            elif reason_for_service_change_tmp == 1:
                reason_for_service_change = "mscInitiated"
            elif reason_for_service_change_tmp == 2:
                reason_for_service_change = "callSetupFallBack"
            elif reason_for_service_change_tmp == 3:
                reason_for_service_change = "callSetupChangeOrder"

        if mocallrecord_record.getComponentByName("bscIdentification24") is None:
            bsdidentification_24 = ""
        else:
            bsdidentification_24 = binascii.hexlify(str(mocallrecord_record.getComponentByName("bscIdentification24")))

        if mocallrecord_record.getComponentByName("globalCallReference") is None:
            global_call_reference = ""
        else:
            global_call_reference = str(mocallrecord_record.getComponentByName("globalCallReference"))

        if mocallrecord_record.getComponentByName("byPass") is None:
            by_pass = -255
        else:
            by_pass = long(str(mocallrecord_record.getComponentByName("byPass")))

        if mocallrecord_record.getComponentByName("wANAPPS") is None:
            wanapps = -255
        else:
            wanapps = long(str(mocallrecord_record.getComponentByName("wANAPPS")))

        if mocallrecord_record.getComponentByName("mNPNumber") is None:
            mnpnumber = ""
        else:
            mnpnumber = str(mocallrecord_record.getComponentByName("mNPNumber"))

        if mocallrecord_record.getComponentByName("iuReleaseCause") is None:
            iu_release_cause = -255
        else:
            iu_release_cause = long(str(mocallrecord_record.getComponentByName("iuReleaseCause")))

        if mocallrecord_record.getComponentByName("aReleaseCause") is None:
            a_release_cause = -255
        else:
            a_release_cause = long(str(mocallrecord_record.getComponentByName("aReleaseCause")))

        if mocallrecord_record.getComponentByName("calledIMSI") is None:
            called_imsi = ""
        else:
            called_imsi = str(mocallrecord_record.getComponentByName("calledIMSI"))

        if mocallrecord_record.getComponentByName("calledIMEI") is None:
            called_imei = ""
        else:
            called_imei = str(mocallrecord_record.getComponentByName("calledIMEI"))

        if mocallrecord_record.getComponentByName("routingCategory") is None:
            routing_category = ""
        else:
            routing_category = str(mocallrecord_record.getComponentByName("routingCategory"))

        if mocallrecord_record.getComponentByName("millisecDuration") is None:
            milli_sec_duration = -255
        else:
            milli_sec_duration = long(str(mocallrecord_record.getComponentByName("millisecDuration")))

        if mocallrecord_record.getComponentByName("msClassmark") is None:
            ms_classmark = ""
        else:
            ms_classmark = binascii.hexlify(str(mocallrecord_record.getComponentByName("msClassmark")))

        if mocallrecord_record.getComponentByName("outPulsedNumber") is None:
            outpulsed_number = ""
        else:
            outpulsed_number = str(mocallrecord_record.getComponentByName("outPulsedNumber"))

        if mocallrecord_record.getComponentByName("byPassFlag2") is None:
            by_pass_flag2 = -255
        else:
            by_pass_flag2 = long(str(mocallrecord_record.getComponentByName("byPassFlag2")))

        if mocallrecord_record.getComponentByName("operatorId") is None:
            operator_id = -255
        else:
            operator_id = long(str(mocallrecord_record.getComponentByName("operatorId")))

        if mocallrecord_record.getComponentByName("defaultCallHandling_3") is None:
            default_call_handling3 = ""
        else:
            default_call_handling3_tmp = long(str(mocallrecord_record.getComponentByName("defaultCallHandling_3")))
            default_call_handling3 = ""
            if default_call_handling3_tmp == 0:
                default_call_handling3 = "continueCall"
            elif default_call_handling3_tmp == 1:
                default_call_handling3 = "releaseCall"

        if mocallrecord_record.getComponentByName("recordSequenceNumber") is None:
            record_sequence_number = ""
        else:
            record_sequence_number = binascii.hexlify(str(mocallrecord_record.getComponentByName("recordSequenceNumber")))

        if mocallrecord_record.getComponentByName("gsm-SCFAddress_3") is not None:
            gsm_scfaddress_3_binascii = binascii.hexlify(
                str(mocallrecord_record.getComponentByName("gsm-SCFAddress_3")))
            gsm_scfaddress_3_list = []
            gsm_scfaddress_3_data = list(gsm_scfaddress_3_binascii)
            for i in xrange(0, len(gsm_scfaddress_3_data) - 1, 2):
                gsm_scfaddress_3_list.append(gsm_scfaddress_3_data[i + 1] + "" + gsm_scfaddress_3_data[i])
                gsm_scfaddress_3_concatenated = ''.join(gsm_scfaddress_3_list)
                gsm_scfaddress_3 = gsm_scfaddress_3_concatenated[2:].replace("f", "")
        else:
            gsm_scfaddress_3 = ""

        if mocallrecord_record.getComponentByName("serviceKey_3") is None:
            service_key_3 = -255
        else:
            service_key_3 = long(str(mocallrecord_record.getComponentByName("serviceKey_3")))

        if mocallrecord_record.getComponentByName("freeFormatData_3") is None:
            free_format_data_3 = ""
        else:
            free_format_data_3 = str(mocallrecord_record.getComponentByName("freeFormatData_3"))

        if mocallrecord_record.getComponentByName("freeFormatDataAppend_3") is None:
            free_format_data_append_3 = ""
        else:
            free_format_data_append_3 = str(mocallrecord_record.getComponentByName("freeFormatDataAppend_3"))

        is_inapmnquery_get_value = mocallrecord_record.getComponentByName("isINAPMNPQuery")
        is_inapmnquery = boolean_value(is_inapmnquery_get_value)

        if mocallrecord_record.getComponentByName("iNAPMNPQueryMethod") is None:
            inapmnquerymethod = ""
        else:
            inapmnquerymethod_tmp = long(str(mocallrecord_record.getComponentByName("iNAPMNPQueryMethod")))
            inapmnquerymethod = ""
            if inapmnquerymethod_tmp == 1:
                inapmnquerymethod = "unkown"
            elif inapmnquerymethod_tmp == 2:
                inapmnquerymethod = "iNSolution"
            elif inapmnquerymethod_tmp == 3:
                inapmnquerymethod = "sRFSolution"

        if mocallrecord_record.getComponentByName("iNAPMNPRoutingNumber") is None:
            inapmn_routing_number = ""
        else:
            inapmn_routing_number = str(mocallrecord_record.getComponentByName("iNAPMNPRoutingNumber"))

        if mocallrecord_record.getComponentByName("iNAPMNPPortStatus") is None:
            inapmnpport_status = ""
        else:
            inapmnpport_status_tmp = long(str(mocallrecord_record.getComponentByName("iNAPMNPPortStatus")))
            inapmnpport_status = ""
            if inapmnpport_status_tmp == 1:
                inapmnpport_status = "unkown"
            elif inapmnpport_status_tmp == 2:
                inapmnpport_status = "portedNumber"
            elif inapmnpport_status_tmp == 3:
                inapmnpport_status = "nonPortedNumber"

        if mocallrecord_record.getComponentByName("suppressCAMELInd") is None:
            suppress_camel_ind = ""
        else:
            suppress_camel_ind = str(mocallrecord_record.getComponentByName("suppressCAMELInd"))

        csfbmo_indicator_get_value = mocallrecord_record.getComponentByName("cSFBMOIndicator")
        csfbmo_indicator = boolean_value(csfbmo_indicator_get_value)

        ics_flag_get_value = mocallrecord_record.getComponentByName("iCSFlag")
        ics_flag = boolean_value(ics_flag_get_value)

        if mocallrecord_record.getComponentByName("tCSIDestRoutAddress") is None:
            tcsidestrout_address = ""
        else:
            tcsidestrout_address = str(mocallrecord_record.getComponentByName("tCSIDestRoutAddress"))

        location = mocallrecord_record.getComponentByName("location")
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

        if mocallrecord_record.getComponentByName("speechVersionSupported") is None:
            speech_version_supported = ""
        else:
            speech_version_supported = binascii.hexlify(str(mocallrecord_record.getComponentByName("speechVersionSupported")))

        if mocallrecord_record.getComponentByName("speechVersionUsed") is None:
            speech_version_used = ""
        else:
            speech_version_used = binascii.hexlify(str(mocallrecord_record.getComponentByName("speechVersionUsed")))

        mocallrecord_record = [a_release_cause, charge_indicator, charged_party, aiur_requested,
                               answer_time, aoc_parametere1, aoc_parametere2, aoc_parametere3, aoc_parametere4,
                               aoc_parametere5, aoc_parametere6, aoc_parametere7, bsdidentification, basic_service,
                               bsdidentification_24,
                               by_pass,
                               by_pass_flag2, csfbmo_indicator, cug_category_calltype, cug_category_usertype,
                               cug_index, cug_interlock_code, cug_outgoing_accessind, cug_outgoing_accessused,
                               call_duration, call_reference, called_imei, called_imsi, called_location_cellid,
                               called_location_area_code,
                               called_location_plmn, called_location_sac, called_number, calling_party_roamingind,
                               carp, cause_for_termination, chan_coding_used, change_time, radio_channel,
                               changeofradiochan_speech_version_used, channel_type, connected_number, ddcflag,
                               data_rate, data_volume,
                               default_call_handling, default_call_handling2, default_call_handling3,
                               diagnostics, dialled_number, emlpp_priority_level, emlpp_request_priority_level,
                               exchange_identity, file_id, fnur, free_format_data, free_format_data_append,
                               free_format_data_append_2, free_format_data_append_3, free_format_data_2,
                               free_format_data_3, global_call_reference, gsm_scfaddress, gsm_scfaddress_2,
                               gsm_scfaddress_3, guaranteed_bit_rate, hscsd_chan_allocated, hscsd_chan_requested,
                               hot_billing_tag, hot_billing_tag2, ics_flag, inapmnpport_status,
                               inapmnquerymethod, inapmn_routing_number, incoming_cic_channel, incoming_cic_pcmunit,
                               incoming_tkgpname,
                               is_camel_call, is_cug_used, is_hscsd_used, is_inapmnquery, is_multi_media_call,
                               iu_release_cause, last_long_part_ind, level_of_camel_service, location_cellid,
                               location_area_code, location_plmn, location_sac, mnpnumber,
                               inapmn_routing_number, msc_address, maximumbitrate, milli_sec_duration,
                               ms_classmark,
                               msc_incoming_tkgp, msc_outgoing_tkgp, mscspc14, mscspc24, network_call_reference,
                               number_of_dp_encountered, operator_id, outpulsed_number, outgoing_cic_channel,
                               outgoing_cic_pcmunit,
                               outgoing_tkgpname, partial_record_type, pbrt, radio_chan_requested,
                               radio_chan_used, rate_indication, reason_for_service_change,
                               record_sequence_number, record_type,
                               recording_entity, release_time, roaming_number, routing_category, seizure_time,
                               sequence_number, served_imei, served_imsi, served_msisdn, service_category,
                               service_change_initiator, service_key, service_key_2, service_key_3,
                               speech_version_supported, speech_version_used, subscriber_category,
                               suppress_camel_ind, system_type, tcsidestrout_address, transaction_identification,
                               translated_number, transparency_indicator, types_of_service_change, wanapps,
                               long(seizure_time.strftime("%Y%m%d")), long(seizure_time.strftime("%H"))]

        return mocallrecord_record

