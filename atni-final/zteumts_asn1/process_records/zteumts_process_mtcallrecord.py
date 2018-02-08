import binascii

from common.utilities import parseTimestamp, append_timezone_offset, boolean_value, parse_start_time


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


class ProcessZTEumtsMTCallRecord:
    def __init__(self):
        pass

    @staticmethod
    def process_mtcall_records(mtcall_record, file_id, file_name):

        if mtcall_record.getComponentByName("aReleaseCause") is None:
            a_release_cause = -255
        else:
            a_release_cause = long(str(mtcall_record.getComponentByName("aReleaseCause")))

        additional_chg_info = mtcall_record.getComponentByName("additionalChgInfo")

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

        if mtcall_record.getComponentByName("aiurRequested") is None:
            aiur_requested = ""
        else:
            aiur_requested_tmp = long(str(mtcall_record.getComponentByName("aiurRequested")))
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

        answer_time_tmp = binascii.hexlify(str(mtcall_record.getComponentByName("answerTime")))
        answer_time = append_timezone_offset(parseTimestamp(answer_time_tmp))

        if mtcall_record.getComponentByName("aocParameters") is not None:
            aoc_parameter = str(mtcall_record.getComponentByName("aocParameters"))
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

        if mtcall_record.getComponentByName("bSCIdentification") is None:
            bsdidentification = ""
        else:
            bsdidentification = binascii.hexlify(str(mtcall_record.getComponentByName("bSCIdentification")))

        if mtcall_record.getComponentByName("basicService") is None:
            basic_service = ""
        else:
            basic_service_choice = mtcall_record.getComponentByName("basicService")
            basic_service_choice_hexvalue = basic_service_choice.getComponent().prettyPrint()
            if basic_service_choice_hexvalue == "0x11":
                basic_service = "teleservice"
            else:
                basic_service = "bearerservice"

        if mtcall_record.getComponentByName("bscIdentification24") is None:
            bsdidentification_24 = ""
        else:
            bsdidentification_24 = binascii.hexlify(str(mtcall_record.getComponentByName("bscIdentification24")))

        csfbmt_indicator_get_value = mtcall_record.getComponentByName("cSFBMTIndicator")
        csfbmt_indicator = boolean_value(csfbmt_indicator_get_value)

        cug_category = mtcall_record.getComponentByName("cUGCategory")

        if cug_category is None:
            cug_category_calltype = -255
        else:
            cug_category_calltype = long(str(cug_category.getComponentByName("callType")))

        if cug_category is None:
            cug_category_usertype = -255
        else:
            cug_category_usertype = long(str(cug_category.getComponentByName("userType")))

        if cug_category is None:
            cug_index = -255
        else:
            cug_index = long(str(cug_category.getComponentByName("cUGIndex")))

        if mtcall_record.getComponentByName("cUGInterLockCode") is None:
            cug_interlock_code = ""
        else:
            cug_interlock_code = str(mtcall_record.getComponentByName("cUGInterLockCode"))

        cug_incoming_accessind_get_value = mtcall_record.getComponentByName("cUGIncomingAccessUsed")
        cug_incoming_accessind = boolean_value(cug_incoming_accessind_get_value)

        cug_incoming_accessused_get_value = mtcall_record.getComponentByName("cUGIncomingAccessInd")
        cug_incoming_accessused = boolean_value(cug_incoming_accessused_get_value)

        if mtcall_record.getComponentByName("callDuration") is None:
            call_duration = -255
        else:
            call_duration = long(str(mtcall_record.getComponentByName("callDuration")))

        if mtcall_record.getComponentByName("callReference") is None:
            call_reference = ""
        else:
            call_reference = binascii.hexlify(str(mtcall_record.getComponentByName("callReference")))

        if mtcall_record.getComponentByName("calledPartyRoamingInd") is None:
            called_party_roamingind = ""
        else:
            calling_party_roamingind_int = str(mtcall_record.getComponentByName("calledPartyRoamingInd"))
            if calling_party_roamingind_int == "1":
                called_party_roamingind = "TRUE"
            else:
                called_party_roamingind = "FALSE"

        if mtcall_record.getComponentByName("callingLocation") is None:
            calling_location = ""
        else:
            calling_location = str(mtcall_record.getComponentByName("callingLocation"))

        if mtcall_record.getComponentByName("callingNumber") is None:
            calling_number = ""
        else:
            calling_number_binascii = binascii.hexlify(
                    str(mtcall_record.getComponentByName("callingNumber")))
            calling_number_list = []
            calling_number_data = list(calling_number_binascii)
            for i in xrange(0, len(calling_number_data) - 1, 2):
                calling_number_list.append(calling_number_data[i + 1] + "" + calling_number_data[i])
            calling_number_concatenated = ''.join(calling_number_list)
            calling_number = calling_number_concatenated[2:]

        if mtcall_record.getComponentByName("carp") is None:
            carp = -255
        else:
            carp = long(str(mtcall_record.getComponentByName("carp")))

        if mtcall_record.getComponentByName("causeForTerm") is None:
            cause_for_termination = ""
        else:
            cause_for_termination_tmp = long(str(mtcall_record.getComponentByName("causeForTerm")))
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

        if mtcall_record.getComponentByName("chanCodingUsed") is None:
            chan_coding_used = ""
        else:
            chan_coding_used_tmp = long(str(mtcall_record.getComponentByName("chanCodingUsed")))
            chan_coding_used = ""
            if chan_coding_used_tmp == 1:
                chan_coding_used = "tchF4800"
            elif chan_coding_used_tmp == 2:
                chan_coding_used = "tchF9600"
            elif chan_coding_used_tmp == 3:
                chan_coding_used = "tchF14400"

        if mtcall_record.getComponentByName("changeOfRadioChan") is not None:
            change_of_radio_chan = mtcall_record.getComponentByName("changeOfRadioChan")
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

        if mtcall_record.getComponentByName("channelType") is None:
            channel_type = ""
        else:
            channel_type = str(mtcall_record.getComponentByName("channelType"))

        if mtcall_record.getComponentByName("connectedNumber") is None:
            connected_number = ""
        else:
            connected_number_binascii = binascii.hexlify(str(mtcall_record.getComponentByName("connectedNumber")))
            connected_number_list = []
            connected_number_data = list(connected_number_binascii)
            for i in xrange(0, len(connected_number_data) - 1, 2):
                connected_number_list.append(connected_number_data[i + 1] + "" + connected_number_data[i])
            connected_number_concatenated = ''.join(connected_number_list)
            connected_number = connected_number_concatenated[2:]

        if mtcall_record.getComponentByName("dataRate") is None:
            data_rate = -255
        else:
            data_rate = long(str(mtcall_record.getComponentByName("dataRate")))

        if mtcall_record.getComponentByName("dataVolume") is None:
            data_volume = -255
        else:
            data_volume = long(str(mtcall_record.getComponentByName("dataVolume")))

        if mtcall_record.getComponentByName("dDCFlag") is None:
            ddcflag = -255
        else:
            ddcflag = long(str(mtcall_record.getComponentByName("dDCFlag")))

        if mtcall_record.getComponentByName("defaultCallHandling") is None:
            default_call_handling = ""
        else:
            default_call_handling_tmp = long(str(mtcall_record.getComponentByName("defaultCallHandling")))
            default_call_handling = ""
            if default_call_handling_tmp == 0:
                default_call_handling = "continueCall"
            elif default_call_handling_tmp == 1:
                default_call_handling = "releaseCall"

        if mtcall_record.getComponentByName("diagnostics") is None:
            diagnostics = ""
        else:
            diagnostics = mtcall_record.getComponentByName("diagnostics").getName()

        if mtcall_record.getComponentByName("eMLPPPriorityLevel") is None:
            emlpp_priority_level = -255
        else:
            emlpp_priority_level = long(str(mtcall_record.getComponentByName("eMLPPPriorityLevel")))

        if mtcall_record.getComponentByName("eMLPPRequestPriorityLevel") is None:
            emlpp_request_priority_level = -255
        else:
            emlpp_request_priority_level = long(str(mtcall_record.getComponentByName("eMLPPRequestPriorityLevel")))

        if mtcall_record.getComponentByName("exchangeIdentity") is None:
            exchange_identity = ""
        else:
            exchange_identity = str(mtcall_record.getComponentByName("exchangeIdentity"))

        if mtcall_record.getComponentByName("fnur") is None:
            fnur = ""
        else:
            fnur_tmp = long(str(mtcall_record.getComponentByName("fnur")))
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

        if mtcall_record.getComponentByName("freeFormatData") is None:
            free_format_data = ""
        else:
            free_format_data = str(mtcall_record.getComponentByName("freeFormatData"))

        if mtcall_record.getComponentByName("globalCallReference") is None:
            global_call_reference = ""
        else:
            global_call_reference = str(mtcall_record.getComponentByName("globalCallReference"))

        if mtcall_record.getComponentByName("gsm-SCFAddress") is not None:
            gsm_scfaddress_binascii = binascii.hexlify(str(mtcall_record.getComponentByName("gsm-SCFAddress")))
            gsm_scfaddress_list = []
            gsm_scfaddress_data = list(gsm_scfaddress_binascii)
            for i in xrange(0, len(gsm_scfaddress_data) - 1, 2):
                gsm_scfaddress_list.append(gsm_scfaddress_data[i + 1] + "" + gsm_scfaddress_data[i])
                gsm_scfaddress_concatenated = ''.join(gsm_scfaddress_list)
                gsm_scfaddress = gsm_scfaddress_concatenated[2:].replace("f", "")
        else:
            gsm_scfaddress = ""

        if mtcall_record.getComponentByName("guaranteedBitRate") is None:
            guaranteed_bit_rate = ""
        else:
            guaranteed_bit_rate_tmp = long(str(mtcall_record.getComponentByName("guaranteedBitRate")))
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

        if mtcall_record.getComponentByName("hSCSDChanAllocated") is None:
            hscsd_chan_allocated = -255
        else:
            hscsd_chan_allocated = long(str(mtcall_record.getComponentByName("hSCSDChanAllocated")))

        if mtcall_record.getComponentByName("hSCSDChanRequested") is None:
            hscsd_chan_requested = -255
        else:
            hscsd_chan_requested = long(str(mtcall_record.getComponentByName("hSCSDChanRequested")))

        if mtcall_record.getComponentByName("hotBillingTag") is None:
            hot_billing_tag = -255
        else:
            hot_billing_tag = long(str(mtcall_record.getComponentByName("hotBillingTag")))

        if mtcall_record.getComponentByName("hotBillingTag2") is None:
            hot_billing_tag2 = -255
        else:
            hot_billing_tag2 = long(str(mtcall_record.getComponentByName("hotBillingTag2")))

        ics_flag_get_value = mtcall_record.getComponentByName("iCSFlag")
        ics_flag = boolean_value(ics_flag_get_value)

        incoming_cic = mtcall_record.getComponentByName("incomingCic")

        if incoming_cic is None:
            incoming_cic_channel = -255
        else:
            incoming_cic_channel = long(str(incoming_cic.getComponentByName("channel")))

        if incoming_cic is None:
            incoming_cic_pcmunit = -255
        else:
            incoming_cic_pcmunit = long(str(incoming_cic.getComponentByName("pcmUnit")))

        if mtcall_record.getComponentByName("incomingTKGPName") is None:
            incoming_tkgpname = ""
        else:
            incoming_tkgpname = str(mtcall_record.getComponentByName("incomingTKGPName"))

        if mtcall_record.getComponentByName("incomingTrafficType") is None:
            incoming_traffic_type = ""
        else:
            incoming_traffic_type_tmp = long(str(mtcall_record.getComponentByName("incomingTrafficType")))
            incoming_traffic_type = ""
            if incoming_traffic_type_tmp == 0:
                incoming_traffic_type = "unknown"
            elif incoming_traffic_type_tmp == 1:
                incoming_traffic_type = "localNetworkMobile"
            elif incoming_traffic_type_tmp == 2:
                incoming_traffic_type = "localNetworkFixed"
            elif incoming_traffic_type_tmp == 3:
                incoming_traffic_type = "externalNetwork"

        is_camel_call_get_value = mtcall_record.getComponentByName("isCAMELCall")
        is_camel_call = boolean_value(is_camel_call_get_value)

        is_cug_used_get_value = mtcall_record.getComponentByName("isCUGUsed")
        is_cug_used = boolean_value(is_cug_used_get_value)

        is_hscsd_used_get_value = mtcall_record.getComponentByName("isHSCSDUsed")
        is_hscsd_used = boolean_value(is_hscsd_used_get_value)

        is_multi_media_call_get_value = mtcall_record.getComponentByName("isMultiMediaCall")
        is_multi_media_call = boolean_value(is_multi_media_call_get_value)

        if mtcall_record.getComponentByName("iuReleaseCause") is None:
            iu_release_cause = -255
        else:
            iu_release_cause = long(str(mtcall_record.getComponentByName("iuReleaseCause")))

        last_long_part_ind_get_value = mtcall_record.getComponentByName("lastLongPartInd")
        last_long_part_ind = boolean_value(last_long_part_ind_get_value)

        if mtcall_record.getComponentByName("location") is not None:
            location = mtcall_record.getComponentByName("location")
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

        if mtcall_record.getComponentByName("mSCAddress") is None:
            msc_address = ""
        else:
            msc_address_binascii = binascii.hexlify(str(mtcall_record.getComponentByName("mSCAddress")))
            msc_address_list = []
            msc_address_data = list(msc_address_binascii)
            for i in xrange(0, len(msc_address_data) - 1, 2):
                msc_address_list.append(msc_address_data[i + 1] + "" + msc_address_data[i])
                msc_address_concatenated = ''.join(msc_address_list)
                msc_address = msc_address_concatenated[2:].replace("f", "")

        if mtcall_record.getComponentByName("maximumBitRate") is None:
            maximumbitrate = ""
        else:
            maximumbitrate_tmp = long(str(mtcall_record.getComponentByName("maximumBitRate")))
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

        if mtcall_record.getComponentByName("millisecDuration") is None:
            milli_sec_duration = -255
        else:
            milli_sec_duration = long(str(mtcall_record.getComponentByName("millisecDuration")))

        if mtcall_record.getComponentByName("msClassmark") is None:
            ms_classmark = ""
        else:
            ms_classmark = binascii.hexlify(str(mtcall_record.getComponentByName("msClassmark")))

        if mtcall_record.getComponentByName("mscIncomingTKGP") is None:
            msc_incoming_tkgp = ""
        else:
            msc_incoming_tkgp = binascii.hexlify(str(mtcall_record.getComponentByName("mscIncomingTKGP")))

        if mtcall_record.getComponentByName("mscOutgoingTKGP") is None:
            msc_outgoing_tkgp = ""
        else:
            msc_outgoing_tkgp = binascii.hexlify(str(mtcall_record.getComponentByName("mscOutgoingTKGP")))

        if mtcall_record.getComponentByName("mscSPC14") is None:
            mscspc14 = ""
        else:
            mscspc14 = binascii.hexlify(str(mtcall_record.getComponentByName("mscSPC14")))

        if mtcall_record.getComponentByName("mscSPC24") is None:
            mscspc24 = ""
        else:
            mscspc24 = binascii.hexlify(str(mtcall_record.getComponentByName("mscSPC24")))

        if mtcall_record.getComponentByName("networkCallReference") is None:
            network_call_reference = ""
        else:
            network_call_reference = binascii.hexlify(str(mtcall_record.getComponentByName("networkCallReference")))

        if mtcall_record.getComponentByName("operatorId") is None:
            operator_id = -255
        else:
            operator_id = long(str(mtcall_record.getComponentByName("operatorId")))

        if mtcall_record.getComponentByName("operatorId") is None:
            original_called_number = ""
        else:
            original_called_number = str(mtcall_record.getComponentByName("originalCalledNumber"))

        outgoing_cic = mtcall_record.getComponentByName("outgoingCic")

        if outgoing_cic is None:
            outgoing_cic_channel = -255
        else:
            outgoing_cic_channel = long(str(outgoing_cic.getComponentByName("channel")))

        if outgoing_cic is None:
            outgoing_cic_pcmunit = -255
        else:
            outgoing_cic_pcmunit = long(str(outgoing_cic.getComponentByName("pcmUnit")))

        if mtcall_record.getComponentByName("outgoingTKGPName") is None:
            outgoing_tkgpname = ""
        else:
            outgoing_tkgpname = str(mtcall_record.getComponentByName("outgoingTKGPName"))

        if mtcall_record.getComponentByName("outgoingTrafficType") is None:
            outgoing_traffic_type = ""
        else:
            outgoing_traffic_type_tmp = long(str(mtcall_record.getComponentByName("outgoingTrafficType")))
            outgoing_traffic_type = ""
            if outgoing_traffic_type_tmp == 0:
                outgoing_traffic_type = "unknown"
            elif outgoing_traffic_type_tmp == 1:
                outgoing_traffic_type = "localNetworkMobile"
            elif outgoing_traffic_type_tmp == 2:
                outgoing_traffic_type = "localNetworkFixed"
            elif outgoing_traffic_type_tmp == 3:
                outgoing_traffic_type = "externalNetwork"

        if mtcall_record.getComponentByName("partialRecordType") is None:
            partial_record_type = ""
        else:
            partial_record_type_tmp = long(str(mtcall_record.getComponentByName("partialRecordType")))
            partial_record_type = ""
            if partial_record_type_tmp == 1:
                partial_record_type = "serviceChange"

        if mtcall_record.getComponentByName("pbrt") is None:
            pbrt = -255
        else:
            pbrt = long(str(mtcall_record.getComponentByName("pbrt")))

        if mtcall_record.getComponentByName("radioChanRequested") is None:
            radio_chan_requested = ""
        else:

            radio_chan_requested_tmp = long(str(mtcall_record.getComponentByName("radioChanRequested")))
            radio_chan_requested = ""
            if radio_chan_requested_tmp == 0:
                radio_chan_requested = "halfRateChannel"
            elif radio_chan_requested_tmp == 1:
                radio_chan_requested = "fullRateChannel"
            elif radio_chan_requested_tmp == 2:
                radio_chan_requested = "dualHalfRatePreferred"
            elif radio_chan_requested_tmp == 3:
                radio_chan_requested = "dualFullRatePreferred"

        if mtcall_record.getComponentByName("radioChanUsed") is None:
            radio_chan_used = ""
        else:
            radio_chan_used_tmp = long(str(mtcall_record.getComponentByName("radioChanUsed")))
            radio_chan_used = ""
            if radio_chan_used_tmp == 0:
                radio_chan_used = "fullRate"
            elif radio_chan_used_tmp == 1:
                radio_chan_used = "halfRate"

        if mtcall_record.getComponentByName("rateIndication") is None:
            rate_indication = ""
        else:
            rate_indication = str(mtcall_record.getComponentByName("rateIndication"))

        if mtcall_record.getComponentByName("reasonForServiceChange") is None:
            reason_for_service_change = ""
        else:
            reason_for_service_change_tmp = long(str(mtcall_record.getComponentByName("reasonForServiceChange")))
            reason_for_service_change = ""
            if reason_for_service_change_tmp == 0:
                reason_for_service_change = "msubInitiate"
            elif reason_for_service_change_tmp == 1:
                reason_for_service_change = "mscInitiated"
            elif reason_for_service_change_tmp == 2:
                reason_for_service_change = "callSetupFallBack"
            elif reason_for_service_change_tmp == 3:
                reason_for_service_change = "callSetupChangeOrder"

        if mtcall_record.getComponentByName("recordSequenceNumber") is None:
            record_sequence_number = ""
        else:
            record_sequence_number = binascii.hexlify(str(mtcall_record.getComponentByName("recordSequenceNumber")))

        if mtcall_record.getComponentByName("recordType") is None:
            record_type = -255
        else:
            record_type = long(str(mtcall_record.getComponentByName("recordType")))

        if mtcall_record.getComponentByName("recordingEntity") is None:
            recording_entity = ""
        else:
            recording_entity_binascii = binascii.hexlify(str(mtcall_record.getComponentByName("recordingEntity")))
            recording_entity_list = []
            recording_entity_data = list(recording_entity_binascii)
            for l in xrange(0, len(recording_entity_data) - 1, 2):
                recording_entity_list.append(recording_entity_data[l+1] + "" + recording_entity_data[l])
            recording_entity_concatenated = ''.join(recording_entity_list)
            recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
            recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        if mtcall_record.getComponentByName("redirectNumber") is None:
            redirect_number = ""
        else:
            redirecting_number_binascii = binascii.hexlify(str(mtcall_record.getComponentByName("redirectNumber")))
            redirecting_number_list = []
            redirecting_number_data = list(redirecting_number_binascii)
            for i in xrange(0, len(redirecting_number_data) - 1, 2):
                redirecting_number_list.append(redirecting_number_data[i + 1] + "" + redirecting_number_data[i])
            redirecting_number_concatenated = ''.join(redirecting_number_list)
            redirect_number = redirecting_number_concatenated[2:].replace("f", "")

        if mtcall_record.getComponentByName("redirectingNumber") is None:
            redirecting_number = ""
        else:
            redirecting_number = str(mtcall_record.getComponentByName("redirectingNumber"))

        release_time_tmp = binascii.hexlify(str(mtcall_record.getComponentByName("releaseTime")))
        release_time = append_timezone_offset(parseTimestamp(release_time_tmp))

        if mtcall_record.getComponentByName("routingCategory") is None:
            routing_category = ""
        else:
            routing_category = str(mtcall_record.getComponentByName("routingCategory"))

        seizure_time_tmp = binascii.hexlify(str(mtcall_record.getComponentByName("seizureTime")))
        seizure_time = append_timezone_offset(parseTimestamp(seizure_time_tmp))

        if mtcall_record.getComponentByName("sequenceNumber") is None:
            sequence_number = -255
        else:
            sequence_number = long(str(mtcall_record.getComponentByName("sequenceNumber")))

        if mtcall_record.getComponentByName("servedIMEI") is None:
            served_imei = ""
        else:
            served_imei_binascii = binascii.hexlify(str(mtcall_record.getComponentByName("servedIMEI")))
            served_imei_list = []
            served_imei_data = list(served_imei_binascii)
            for i in xrange(0, len(served_imei_data) - 1, 2):
                served_imei_list.append(served_imei_data[i + 1] + "" + served_imei_data[i])
            served_imei_concatenated = ''.join(served_imei_list)
            served_imei = served_imei_concatenated.replace("f", "")

        if mtcall_record.getComponentByName("servedIMSI") is None:
            served_imsi = ""
        else:
            served_imsi_binascii = binascii.hexlify(str(mtcall_record.getComponentByName("servedIMSI")))
            served_imsi_list = []
            served_imsi_data = list(served_imsi_binascii)
            for j in xrange(0, len(served_imsi_data) - 1, 2):
                served_imsi_list.append(served_imsi_data[j+1] + "" + served_imsi_data[j])
            served_imsi_concatenated = ''.join(served_imsi_list)
            served_imsi = served_imsi_concatenated.replace("f", "")

        if mtcall_record.getComponentByName("servedMSISDN") is None:
            served_msisdn = ""
        else:
            served_msisdn_binascii = binascii.hexlify(str(mtcall_record.getComponentByName("servedMSISDN")))
            served_msisdn_list = []
            served_msisdn_data = list(served_msisdn_binascii)
            for k in xrange(0, len(served_msisdn_data) - 1, 2):
                served_msisdn_list.append(served_msisdn_data[k+1] + "" + served_msisdn_data[k])
            served_msisdn_concatenated = ''.join(served_msisdn_list)
            served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
            served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        if mtcall_record.getComponentByName("serviceCategory") is None:
            service_category = ""
        else:
            service_category = binascii.hexlify(str(mtcall_record.getComponentByName("serviceCategory")))

        if mtcall_record.getComponentByName("serviceChangeInitiator") is None:
            service_change_initiator = -255
        else:
            service_change_initiator = long(str(mtcall_record.getComponentByName("serviceChangeInitiator")))

        if mtcall_record.getComponentByName("serviceKey") is None:
            service_key = ""
        else:
            service_key = long(str(mtcall_record.getComponentByName("serviceKey")))

        if mtcall_record.getComponentByName("speechVersionSupported") is None:
            speech_version_supported = ""
        else:
            speech_version_supported = binascii.hexlify(str(mtcall_record.getComponentByName("speechVersionSupported")))

        if mtcall_record.getComponentByName("speechVersionUsed") is None:
            speech_version_used = ""
        else:
            speech_version_used = binascii.hexlify(str(mtcall_record.getComponentByName("speechVersionUsed")))

        if mtcall_record.getComponentByName("startTime") is None:
            start_time = parse_start_time(file_name)
        else:
            start_time_tmp = binascii.hexlify(str(mtcall_record.getComponentByName("startTime")))
            start_time = append_timezone_offset(parseTimestamp(start_time_tmp))

        if mtcall_record.getComponentByName("subscriberCategory") is None:
            subscriber_category = ""
        else:
            subscriber_category = binascii.hexlify(str(mtcall_record.getComponentByName("subscriberCategory")))

        if mtcall_record.getComponentByName("systemType") is None:
            system_type = ""
        else:
            system_type_tmp = long(str(mtcall_record.getComponentByName("systemType")))
            system_type = ""
            if system_type_tmp == 0:
                system_type = "unknown"
            elif system_type_tmp == 1:
                system_type = "iuUTRAN"
            elif system_type_tmp == 2:
                system_type = "gERAN"

        if mtcall_record.getComponentByName("terminatingRecordType") is None:
            terminating_record_type = ""
        else:
            terminating_record_type_tmp = long(str(mtcall_record.getComponentByName("terminatingRecordType")))
            terminating_record_type = ""
            if terminating_record_type_tmp == 0:
                terminating_record_type = "ordinaryTerminating"
            elif terminating_record_type_tmp == 1:
                terminating_record_type = "forwardOnUnconditionalTerminating"
            elif terminating_record_type_tmp == 2:
                terminating_record_type = "forwardOnBusyTerminating"
            elif terminating_record_type_tmp == 3:
                terminating_record_type = "forwardOnNotReplyTerminating"
            elif terminating_record_type_tmp == 4:
                terminating_record_type = "forwardOnMsNotReachableTerminating"
            elif terminating_record_type_tmp == 255:
                terminating_record_type = "forwardOnOtherReasonTerminating"

        if mtcall_record.getComponentByName("transactionIdentification") is None:
            transaction_identification = -255
        else:
            transaction_identification = long(str(mtcall_record.getComponentByName("transactionIdentification")))

        if mtcall_record.getComponentByName("transparencyIndicator") is None:
            transparency_indicator = ""
        else:
            transparency_indicator_tmp = long(str(mtcall_record.getComponentByName("transparencyIndicator")))
            transparency_indicator = ""
            if transparency_indicator_tmp == 0:
                transparency_indicator = "transparent"
            elif transparency_indicator_tmp == 1:
                transparency_indicator = "nonTransparent"

        if mtcall_record.getComponentByName("typeOfServiceChange") is None:
            types_of_service_change = ""
        else:
            types_of_service_change_tmp = long(str(mtcall_record.getComponentByName("typeOfServiceChange")))
            types_of_service_change = ""
            if types_of_service_change_tmp == 0:
                types_of_service_change = "changeToSpeech"
            elif types_of_service_change_tmp == 1:
                types_of_service_change = "changeToMultimedia"

        mtcall_record = [a_release_cause, charge_indicator, charged_party, aiur_requested,
                         answer_time, aoc_parametere1, aoc_parametere2, aoc_parametere3,
                         aoc_parametere4, aoc_parametere5, aoc_parametere6,
                         aoc_parametere7,
                         bsdidentification, basic_service, bsdidentification_24, csfbmt_indicator,
                         cug_category_calltype, cug_category_usertype, cug_incoming_accessind,
                         cug_incoming_accessused, cug_index, cug_interlock_code, call_duration,
                         call_reference, called_party_roamingind, calling_location, calling_number,
                         carp, cause_for_termination, chan_coding_used, change_time,
                         radio_channel,
                         changeofradiochan_speech_version_used, channel_type, connected_number, ddcflag,
                         data_rate, data_volume,
                         default_call_handling, diagnostics, emlpp_priority_level,
                         emlpp_request_priority_level,
                         exchange_identity, file_id, fnur, free_format_data, global_call_reference,
                         gsm_scfaddress, guaranteed_bit_rate, hscsd_chan_allocated, hscsd_chan_requested,
                         hot_billing_tag, hot_billing_tag2, ics_flag, incoming_cic_channel, incoming_cic_pcmunit,
                         incoming_tkgpname,
                         incoming_traffic_type, is_camel_call, is_cug_used, is_hscsd_used,
                         is_multi_media_call, iu_release_cause, last_long_part_ind, location_cellid,
                         location_area_code,
                         location_plmn,
                         location_sac, msc_address, maximumbitrate, milli_sec_duration, ms_classmark,
                         msc_incoming_tkgp, msc_outgoing_tkgp, mscspc14, mscspc24, network_call_reference,
                         operator_id, original_called_number, outgoing_cic_channel, outgoing_cic_pcmunit,
                         outgoing_tkgpname,
                         outgoing_traffic_type, partial_record_type, pbrt, radio_chan_requested,
                         radio_chan_used, rate_indication, reason_for_service_change,
                         record_sequence_number,
                         record_type, recording_entity, redirect_number, redirecting_number, release_time,
                         routing_category, seizure_time, sequence_number, served_imei, served_imsi, served_msisdn,
                         service_category, service_change_initiator, service_key, speech_version_supported,
                         speech_version_used, start_time, subscriber_category, system_type,
                         terminating_record_type, transaction_identification, transparency_indicator,
                         types_of_service_change, long(seizure_time.strftime("%Y%m%d")),
                         long(seizure_time.strftime("%H"))]

        return mtcall_record
