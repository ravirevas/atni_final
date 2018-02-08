import binascii

from common.utilities import parseTimestamp, append_timezone_offset, boolean_value


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


class ProcessZTEumtsMCFCallRecord:
    def __init__(self):
        pass

    @staticmethod
    def process_mcfcall_records(mcfcall_record, file_id, file_name):

        additional_chg_info = mcfcall_record.getComponentByName("additionalChgInfo")

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

        answer_time_tmp = binascii.hexlify(str(mcfcall_record.getComponentByName("answerTime")))
        answer_time = append_timezone_offset(parseTimestamp(answer_time_tmp))
        if mcfcall_record.getComponentByName("bSCIdentification") is None:
            bsdidentification = ""
        else:
            bsdidentification = binascii.hexlify(str(mcfcall_record.getComponentByName("bSCIdentification")))

        if mcfcall_record.getComponentByName("basicService") is None:
            basic_service = ""
        else:
            basic_service_choice = mcfcall_record.getComponentByName("basicService")
            basic_service_choice_hexvalue = basic_service_choice.getComponent().prettyPrint()
            if basic_service_choice_hexvalue == "0x11":
                basic_service = "teleservice"
            else:
                basic_service = "bearerservice"

        if mcfcall_record.getComponentByName("bscIdentification24") is None:
            bsdidentification_24 = ""
        else:
            bsdidentification_24 = binascii.hexlify(str(mcfcall_record.getComponentByName("bscIdentification24")))

        if mcfcall_record.getComponentByName("byPass") is None:
            by_pass = -255
        else:
            by_pass = long(str(mcfcall_record.getComponentByName("byPass")))

        if mcfcall_record.getComponentByName("byPassFlag2") is None:
            by_pass_flag2 = -255
        else:
            by_pass_flag2 = long(str(mcfcall_record.getComponentByName("byPassFlag2")))

        if mcfcall_record.getComponentByName("cAMELInitCFIndicator") is None:
            camel_initcf_indicator = ""
        else:
            camel_initcf_indicator_tmp = long(str(mcfcall_record.getComponentByName("cAMELInitCFIndicator")))
            camel_initcf_indicator = ""
            if camel_initcf_indicator_tmp == 0:
                camel_initcf_indicator = "noCAMELCallForwarding"
            if camel_initcf_indicator_tmp == 1:
                camel_initcf_indicator = "cAMELCallForwarding"

        cug_category = mcfcall_record.getComponentByName("cUGCategory")

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

        if mcfcall_record.getComponentByName("cUGInterLockCode") is None:
            cug_interlock_code = ""
        else:
            cug_interlock_code = str(mcfcall_record.getComponentByName("cUGInterLockCode"))

        cug_outgoing_accessind = bool(mcfcall_record.getComponentByName("cUGOutgoingAccessInd"))

        cug_outgoing_accessused = bool(mcfcall_record.getComponentByName("cUGOutgoingAccessUsed"))

        if mcfcall_record.getComponentByName("callDuration") is None:
            call_duration = -255
        else:
            call_duration = long(str(mcfcall_record.getComponentByName("callDuration")))

        if mcfcall_record.getComponentByName("callReference") is None:
            call_reference = ""
        else:
            call_reference = binascii.hexlify(str(mcfcall_record.getComponentByName("callReference")))

        if mcfcall_record.getComponentByName("calledLocation") is not None:
            called_location = mcfcall_record.getComponentByName("calledLocation")
            location_area = binascii.hexlify(str(called_location.getComponentByName("locationArea")))
            location_area_str = str(location_area)

            location_cellid_binascii = location_area_str[10:14]
            format_location_cellid_binascii = int(location_cellid_binascii, 16)
            called_location_cellid = format(format_location_cellid_binascii, '05')

            location_area_code_binascii = location_area_str[6:10]
            format_location_area_code_binascii = int(location_area_code_binascii, 16)
            called_location_area_code = format(format_location_area_code_binascii, '05')

            called_location_plmn = parseMcc(location_area_str[0:6]) + parseMnc(location_area_str[0:6])

            called_location_sac = location_area_str[-4:]
        else:
            called_location_cellid = ""
            called_location_area_code = ""
            called_location_plmn = ""
            called_location_sac = ""

        if mcfcall_record.getComponentByName("calledNumber") is None:
            called_number = ""
        else:
            called_number_binascii = binascii.hexlify(str(mcfcall_record.getComponentByName("calledNumber")))
            called_number_list = []
            called_number_data = list(called_number_binascii)
            for i in xrange(0, len(called_number_data) - 1, 2):
                called_number_list.append(called_number_data[i + 1] + "" + called_number_data[i])
            called_number_concatenated = ''.join(called_number_list)
            called_number = called_number_concatenated[2:]

        if mcfcall_record.getComponentByName("callingNumber") is None:
            calling_number = ""
        else:
            calling_number_binascii = binascii.hexlify(
                    str(mcfcall_record.getComponentByName("callingNumber")))
            calling_number_list = []
            calling_number_data = list(calling_number_binascii)
            for i in xrange(0, len(calling_number_data) - 1, 2):
                calling_number_list.append(calling_number_data[i + 1] + "" + calling_number_data[i])
            calling_number_concatenated = ''.join(calling_number_list)
            calling_number = calling_number_concatenated[2:]

        if mcfcall_record.getComponentByName("callingPartyRoamingInd") is None:
            calling_party_roamingind = ""
        else:
            calling_party_roamingind_int = str(mcfcall_record.getComponentByName("callingPartyRoamingInd"))
            if calling_party_roamingind_int == "1":
                calling_party_roamingind = "TRUE"
            else:
                calling_party_roamingind = "FALSE"

        if mcfcall_record.getComponentByName("carp") is None:
            carp = -255
        else:
            carp = long(str(mcfcall_record.getComponentByName("carp")))

        cause_for_termination_tmp = long(str(mcfcall_record.getComponentByName("causeForTerm")))
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

        if mcfcall_record.getComponentByName("connectedNumber") is None:
            connected_number = ""
        else:
            connected_number_binascii = binascii.hexlify(str(mcfcall_record.getComponentByName("connectedNumber")))
            connected_number_list = []
            connected_number_data = list(connected_number_binascii)
            for i in xrange(0, len(connected_number_data) - 1, 2):
                connected_number_list.append(connected_number_data[i + 1] + "" + connected_number_data[i])
            connected_number_concatenated = ''.join(connected_number_list)
            connected_number = connected_number_concatenated[2:]

        if mcfcall_record.getComponentByName("dataRate") is None:
            data_rate = -255
        else:
            data_rate = long(str(mcfcall_record.getComponentByName("dataRate")))

        if mcfcall_record.getComponentByName("dataVolume") is None:
            data_volume = -255
        else:
            data_volume = long(str(mcfcall_record.getComponentByName("dataVolume")))

        if mcfcall_record.getComponentByName("defaultCallHandling") is None:
            default_call_handling = ""
        else:
            default_call_handling_tmp = long(str(mcfcall_record.getComponentByName("defaultCallHandling")))
            default_call_handling = ""
            if default_call_handling_tmp == 0:
                default_call_handling = "continueCall"
            elif default_call_handling_tmp == 1:
                default_call_handling = "releaseCall"

        if mcfcall_record.getComponentByName("defaultCallHandling_2") is None:
            default_call_handling2 = ""
        else:
            default_call_handling2_tmp = long(str(mcfcall_record.getComponentByName("defaultCallHandling_2")))
            default_call_handling2 = ""
            if default_call_handling2_tmp == 0:
                default_call_handling2 = "continueCall"
            elif default_call_handling2_tmp == 1:
                default_call_handling2 = "releaseCall"

        if mcfcall_record.getComponentByName("defaultCallHandling_3") is None:
            default_call_handling3 = ""
        else:
            default_call_handling3_tmp = long(str(mcfcall_record.getComponentByName("defaultCallHandling_3")))
            default_call_handling3 = ""
            if default_call_handling3_tmp == 0:
                default_call_handling3 = "continueCall"
            elif default_call_handling3_tmp == 1:
                default_call_handling3 = "releaseCall"

        if mcfcall_record.getComponentByName("diagnostics") is None:
            diagnostics = ""
        else:
            diagnostics = mcfcall_record.getComponentByName("diagnostics").getName()

        if mcfcall_record.getComponentByName("exchangeIdentity") is None:
            exchange_identity = ""
        else:
            exchange_identity = str(mcfcall_record.getComponentByName("exchangeIdentity"))

        if mcfcall_record.getComponentByName("fnur") is None:
            fnur = ""
        else:
            fnur_tmp = long(str(mcfcall_record.getComponentByName("fnur")))
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

        if mcfcall_record.getComponentByName("fordwardedToIMSI") is None:
            forwarded_to_imsi = ""
        else:
            forwarded_to_imsi = str(mcfcall_record.getComponentByName("fordwardedToIMSI"))

        if mcfcall_record.getComponentByName("freeFormatData") is None:
            free_format_data = ""
        else:
            free_format_data = str(mcfcall_record.getComponentByName("freeFormatData"))

        if mcfcall_record.getComponentByName("freeFormatDataAppend") is None:
            free_format_data_append = ""
        else:
            free_format_data_append = str(mcfcall_record.getComponentByName("freeFormatDataAppend"))

        if mcfcall_record.getComponentByName("freeFormatDataAppend_2") is None:
            free_format_data_append_2 = ""
        else:
            free_format_data_append_2 = str(mcfcall_record.getComponentByName("freeFormatDataAppend_2"))

        if mcfcall_record.getComponentByName("freeFormatDataAppend_3") is None:
            free_format_data_append_3 = ""
        else:
            free_format_data_append_3 = str(mcfcall_record.getComponentByName("freeFormatDataAppend_3"))

        if mcfcall_record.getComponentByName("freeFormatData_2") is None:
            free_format_data_2 = ""
        else:
            free_format_data_2 = str(mcfcall_record.getComponentByName("freeFormatData_2"))

        if mcfcall_record.getComponentByName("freeFormatData_3") is None:
            free_format_data_3 = ""
        else:
            free_format_data_3 = str(mcfcall_record.getComponentByName("freeFormatData_3"))

        if mcfcall_record.getComponentByName("globalCallReference") is None:
            global_call_reference = ""
        else:
            global_call_reference = str(mcfcall_record.getComponentByName("globalCallReference"))

        if mcfcall_record.getComponentByName("gsm-SCFAddress") is not None:
            gsm_scfaddress_binascii = binascii.hexlify(str(mcfcall_record.getComponentByName("gsm-SCFAddress")))
            gsm_scfaddress_list = []
            gsm_scfaddress_data = list(gsm_scfaddress_binascii)
            for i in xrange(0, len(gsm_scfaddress_data) - 1, 2):
                gsm_scfaddress_list.append(gsm_scfaddress_data[i + 1] + "" + gsm_scfaddress_data[i])
                gsm_scfaddress_concatenated = ''.join(gsm_scfaddress_list)
                gsm_scfaddress = gsm_scfaddress_concatenated[2:].replace("f", "")
        else:
            gsm_scfaddress = ""

        if mcfcall_record.getComponentByName("gsm-SCFAddress_2") is not None:
            gsm_scfaddress_2_binascii = binascii.hexlify(str(mcfcall_record.getComponentByName("gsm-SCFAddress_2")))
            gsm_scfaddress_2_list = []
            gsm_scfaddress_2_data = list(gsm_scfaddress_2_binascii)
            for j in xrange(0, len(gsm_scfaddress_2_data) - 1, 2):
                gsm_scfaddress_2_list.append(gsm_scfaddress_2_data[j + 1] + "" + gsm_scfaddress_2_data[j])
                gsm_scfaddress_2_concatenated = ''.join(gsm_scfaddress_2_list)
                gsm_scfaddress_2 = gsm_scfaddress_2_concatenated[2:].replace("f", "")
        else:
            gsm_scfaddress_2 = ""

        if mcfcall_record.getComponentByName("gsm-SCFAddress_3") is not None:
            gsm_scfaddress_3_binascii = binascii.hexlify(str(mcfcall_record.getComponentByName("gsm-SCFAddress_3")))
            gsm_scfaddress_3_list = []
            gsm_scfaddress_3_data = list(gsm_scfaddress_3_binascii)
            for k in xrange(0, len(gsm_scfaddress_3_data) - 1, 2):
                gsm_scfaddress_3_list.append(gsm_scfaddress_3_data[k + 1] + "" + gsm_scfaddress_3_data[k])
                gsm_scfaddress_3_concatenated = ''.join(gsm_scfaddress_3_list)
                gsm_scfaddress_3 = gsm_scfaddress_3_concatenated[2:].replace("f", "")
        else:
            gsm_scfaddress_3 = ""

        if mcfcall_record.getComponentByName("hotBillingTag") is None:
            hot_billing_tag = -255
        else:
            hot_billing_tag = long(str(mcfcall_record.getComponentByName("hotBillingTag")))

        if mcfcall_record.getComponentByName("hotBillingTag2") is None:
            hot_billing_tag2 = -255
        else:
            hot_billing_tag2 = long(str(mcfcall_record.getComponentByName("hotBillingTag2")))

        if mcfcall_record.getComponentByName("iNAPMNPPortStatus") is None:
            inapmnpport_status = ""
        else:
            inapmnpport_status_tmp = long(str(mcfcall_record.getComponentByName("iNAPMNPPortStatus")))
            inapmnpport_status = ""
            if inapmnpport_status_tmp == 1:
                inapmnpport_status = "unkown"
            elif inapmnpport_status_tmp == 2:
                inapmnpport_status = "portedNumber"
            elif inapmnpport_status_tmp == 3:
                inapmnpport_status = "nonPortedNumber"

        if mcfcall_record.getComponentByName("iNAPMNPQueryMethod") is None:
            inapmnquerymethod = ""
        else:
            inapmnquerymethod_tmp = long(str(mcfcall_record.getComponentByName("iNAPMNPQueryMethod")))
            inapmnquerymethod = ""
            if inapmnquerymethod_tmp == 1:
                inapmnquerymethod = "unkown"
            elif inapmnquerymethod_tmp == 2:
                inapmnquerymethod = "iNSolution"
            elif inapmnquerymethod_tmp == 3:
                inapmnquerymethod = "sRFSolution"

        if mcfcall_record.getComponentByName("iNAPMNPRoutingNumber") is None:
            inapmn_routing_number = ""
        else:
            inapmn_routing_number = str(mcfcall_record.getComponentByName("iNAPMNPRoutingNumber"))

        incoming_cic = mcfcall_record.getComponentByName("incomingCic")

        if incoming_cic is None:
            incoming_cic_channel = -255
        else:
            incoming_cic_channel = long(str(incoming_cic.getComponentByName("channel")))

        if incoming_cic is None:
            incoming_cic_pcmunit = -255
        else:
            incoming_cic_pcmunit = long(str(incoming_cic.getComponentByName("pcmUnit")))

        if mcfcall_record.getComponentByName("incomingTKGPName") is None:
            incoming_tkgpname = ""
        else:
            incoming_tkgpname = str(mcfcall_record.getComponentByName("incomingTKGPName"))

        if mcfcall_record.getComponentByName("incomingTrafficType") is None:
            incoming_traffic_type = ""
        else:
            incoming_traffic_type_tmp = long(str(mcfcall_record.getComponentByName("incomingTrafficType")))
            incoming_traffic_type = ""
            if incoming_traffic_type_tmp == 0:
                incoming_traffic_type = "unknown"
            elif incoming_traffic_type_tmp == 1:
                incoming_traffic_type = "localNetworkMobile"
            elif incoming_traffic_type_tmp == 2:
                incoming_traffic_type = "localNetworkFixed"
            elif incoming_traffic_type_tmp == 3:
                incoming_traffic_type = "externalNetwork"

        is_camel_call_get_value = mcfcall_record.getComponentByName("isCAMELCall")
        is_camel_call = boolean_value(is_camel_call_get_value)

        is_cug_used_get_value = mcfcall_record.getComponentByName("isCUGUsed")
        is_cug_used = boolean_value(is_cug_used_get_value)

        is_inapmnquery_get_value = mcfcall_record.getComponentByName("isINAPMNPQuery")
        is_inapmnquery = boolean_value(is_inapmnquery_get_value)

        is_multi_media_call_get_value = mcfcall_record.getComponentByName("isMultiMediaCall")
        is_multi_media_call = boolean_value(is_multi_media_call_get_value)

        last_long_part_ind_get_value = mcfcall_record.getComponentByName("lastLongPartInd")
        last_long_part_ind = boolean_value(last_long_part_ind_get_value)

        if mcfcall_record.getComponentByName("levelOfCAMELService") is None:
            level_of_camel_service = ""
        else:
            level_of_camel_service_tmp = mcfcall_record.getComponentByName("levelOfCAMELService").prettyPrint()
            level_of_camel_service = level_of_camel_service_tmp[2:5]

        if mcfcall_record.getComponentByName("localOfficeForward") is None:
            local_office_forward = ""
        else:
            local_office_forward = str(mcfcall_record.getComponentByName("localOfficeForward"))

        if mcfcall_record.getComponentByName("location") is not None:
            location = mcfcall_record.getComponentByName("location")
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

        if mcfcall_record.getComponentByName("mNPNumber") is None:
            mnpnumber = ""
        else:
            mnpnumber = str(mcfcall_record.getComponentByName("mNPNumber"))

        if mcfcall_record.getComponentByName("mNPRoutingNumber") is None:
            mnproutingnumber = ""
        else:
            mnproutingnumber = str(mcfcall_record.getComponentByName("mNPRoutingNumber"))

        if mcfcall_record.getComponentByName("mSCAddress") is None:
            msc_address = ""
        else:
            msc_address_binascii = binascii.hexlify(str(mcfcall_record.getComponentByName("mSCAddress")))
            msc_address_list = []
            msc_address_data = list(msc_address_binascii)
            for i in xrange(0, len(msc_address_data) - 1, 2):
                msc_address_list.append(msc_address_data[i + 1] + "" + msc_address_data[i])
                msc_address_concatenated = ''.join(msc_address_list)
                msc_address = msc_address_concatenated[2:].replace("f", "")

        if mcfcall_record.getComponentByName("mcfType") is None:
            mcf_type = ""
        else:
            mcf_type_tmp = long(str(mcfcall_record.getComponentByName("mcfType")))
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

        if mcfcall_record.getComponentByName("millisecDuration") is None:
            milli_sec_duration = -255
        else:
            milli_sec_duration = long(str(mcfcall_record.getComponentByName("millisecDuration")))

        if mcfcall_record.getComponentByName("mscIncomingTKGP") is None:
            msc_incoming_tkgp = ""
        else:
            msc_incoming_tkgp = binascii.hexlify(str(mcfcall_record.getComponentByName("mscIncomingTKGP")))

        if mcfcall_record.getComponentByName("mscOutgoingTKGP") is None:
            msc_outgoing_tkgp = -255
        else:
            msc_outgoing_tkgp = binascii.hexlify(str(mcfcall_record.getComponentByName("mscOutgoingTKGP")))

        if mcfcall_record.getComponentByName("mscSPC14") is None:
            mscspc14 = ""
        else:
            mscspc14 = binascii.hexlify(str(mcfcall_record.getComponentByName("mscSPC14")))

        if mcfcall_record.getComponentByName("mscSPC24") is None:
            mscspc24 = ""
        else:
            mscspc24 = binascii.hexlify(str(mcfcall_record.getComponentByName("mscSPC24")))

        if mcfcall_record.getComponentByName("networkCallReference") is None:
            network_call_reference = ""
        else:
            network_call_reference = binascii.hexlify(str(mcfcall_record.getComponentByName("networkCallReference")))

        if mcfcall_record.getComponentByName("numberOfDPEncountered") is None:
            number_of_dp_encountered = -255
        else:
            number_of_dp_encountered = long(str(mcfcall_record.getComponentByName("numberOfDPEncountered")))

        if mcfcall_record.getComponentByName("numberOfForwarding") is None:
            number_of_forwarding = 0
        else:
            number_of_forwarding = long(str(mcfcall_record.getComponentByName("numberOfForwarding")))

        if mcfcall_record.getComponentByName("operatorId") is None:
            operator_id = -255
        else:
            operator_id = long(str(mcfcall_record.getComponentByName("operatorId")))

        if mcfcall_record.getComponentByName("originalCalledNumber") is None:
            original_called_number = ""
        else:
            original_called_number = str(mcfcall_record.getComponentByName("originalCalledNumber"))

        if mcfcall_record.getComponentByName("outPulsedNumber") is None:
            outpulsed_number = ""
        else:
            outpulsed_number = str(mcfcall_record.getComponentByName("outPulsedNumber"))

        outgoing_cic = mcfcall_record.getComponentByName("outgoingCic")

        if outgoing_cic is None:
            outgoing_cic_channel = -255
        else:
            outgoing_cic_channel = long(str(outgoing_cic.getComponentByName("channel")))

        if outgoing_cic is None:
            outgoing_cic_pcmunit = -255
        else:
            outgoing_cic_pcmunit = long(str(outgoing_cic.getComponentByName("pcmUnit")))

        if mcfcall_record.getComponentByName("outgoingTKGPName") is None:
            outgoing_tkgpname = ""
        else:
            outgoing_tkgpname = str(mcfcall_record.getComponentByName("outgoingTKGPName"))

        if mcfcall_record.getComponentByName("outgoingTrafficType") is None:
            outgoing_traffic_type = ""
        else:
            outgoing_traffic_type_tmp = long(str(mcfcall_record.getComponentByName("outgoingTrafficType")))
            outgoing_traffic_type = ""
            if outgoing_traffic_type_tmp == 0:
                outgoing_traffic_type = "unknown"
            elif outgoing_traffic_type_tmp == 1:
                outgoing_traffic_type = "localNetworkMobile"
            elif outgoing_traffic_type_tmp == 2:
                outgoing_traffic_type = "localNetworkFixed"
            elif outgoing_traffic_type_tmp == 3:
                outgoing_traffic_type = "externalNetwork"

        if mcfcall_record.getComponentByName("pbrt") is None:
            pbrt = -255
        else:
            pbrt = long(str(mcfcall_record.getComponentByName("pbrt")))

        if mcfcall_record.getComponentByName("rateIndication") is None:
            rate_indication = ""
        else:
            rate_indication = str(mcfcall_record.getComponentByName("rateIndication"))

        if mcfcall_record.getComponentByName("reasonForServiceChange") is None:
            reason_for_service_change = -255
        else:
            reason_for_service_change_tmp = long(str(mcfcall_record.getComponentByName("reasonForServiceChange")))
            reason_for_service_change = ""
            if reason_for_service_change_tmp == 0:
                reason_for_service_change = "msubInitiate"
            elif reason_for_service_change_tmp == 1:
                reason_for_service_change = "mscInitiated"
            elif reason_for_service_change_tmp == 2:
                reason_for_service_change = "callSetupFallBack"
            elif reason_for_service_change_tmp == 3:
                reason_for_service_change = "callSetupChangeOrder"

        if mcfcall_record.getComponentByName("partialRecordType") is None:
            partial_record_type = ""
        else:
            partial_record_type_tmp = long(str(mcfcall_record.getComponentByName("partialRecordType")))
            partial_record_type = ""
            if partial_record_type_tmp == 1:
                partial_record_type = "serviceChange"

        if mcfcall_record.getComponentByName("recordingEntity") is None:
            recording_entity = ""
        else:
            recording_entity_binascii = binascii.hexlify(str(mcfcall_record.getComponentByName("recordingEntity")))
            recording_entity_list = []
            recording_entity_data = list(recording_entity_binascii)
            for l in xrange(0, len(recording_entity_data) - 1, 2):
                recording_entity_list.append(recording_entity_data[l+1] + "" + recording_entity_data[l])
            recording_entity_concatenated = ''.join(recording_entity_list)
            recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
            recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        release_time_tmp = str(mcfcall_record.getComponentByName("releaseTime"))
        release_time_tmp_hexa = binascii.hexlify(str(release_time_tmp))
        if release_time_tmp is None:
            release_time = append_timezone_offset(parseTimestamp('1606151210022d0700'))
        else:
            release_time = append_timezone_offset(parseTimestamp(release_time_tmp_hexa))

        if mcfcall_record.getComponentByName("roamingNumber") is None:
            roaming_number = ""
        else:
            roaming_number_binascii = binascii.hexlify(str(mcfcall_record.getComponentByName("roamingNumber")))
            roaming_number_list = []
            roaming_number_data = list(roaming_number_binascii)
            for i in xrange(0, len(roaming_number_data) - 1, 2):
                roaming_number_list.append(roaming_number_data[i + 1] + "" + roaming_number_data[i])
            roaming_number_concatenated = ''.join(roaming_number_list)
            roaming_number = roaming_number_concatenated[2:]

        if mcfcall_record.getComponentByName("recordSequenceNumber") is None:
            record_sequence_number = ""
        else:
            record_sequence_number = binascii.hexlify(str(mcfcall_record.getComponentByName("recordSequenceNumber")))

        if mcfcall_record.getComponentByName("recordType") is None:
            record_type = -255
        else:
            record_type = long(str(mcfcall_record.getComponentByName("recordType")))

        if mcfcall_record.getComponentByName("routingCategory") is None:
            routing_category = ""
        else:
            routing_category = str(mcfcall_record.getComponentByName("routingCategory"))

        seizure_time_tmp = binascii.hexlify(str(mcfcall_record.getComponentByName("seizureTime")))
        seizure_time = append_timezone_offset(parseTimestamp(seizure_time_tmp))

        if mcfcall_record.getComponentByName("sequenceNumber") is None:
            sequence_number = -255
        else:
            sequence_number = long(str(mcfcall_record.getComponentByName("sequenceNumber")))

        if mcfcall_record.getComponentByName("servedIMEI") is None:
            served_imei = ""
        else:
            served_imei_binascii = binascii.hexlify(str(mcfcall_record.getComponentByName("servedIMEI")))
            served_imei_list = []
            served_imei_data = list(served_imei_binascii)
            for i in xrange(0, len(served_imei_data) - 1, 2):
                served_imei_list.append(served_imei_data[i + 1] + "" + served_imei_data[i])
            served_imei_concatenated = ''.join(served_imei_list)
            served_imei = served_imei_concatenated.replace("f", "")

        if mcfcall_record.getComponentByName("servedIMSI") is None:
            served_imsi = ""
        else:
            served_imsi_binascii = binascii.hexlify(str(mcfcall_record.getComponentByName("servedIMSI")))
            served_imsi_list = []
            served_imsi_data = list(served_imsi_binascii)
            for j in xrange(0, len(served_imsi_data) - 1, 2):
                served_imsi_list.append(served_imsi_data[j+1] + "" + served_imsi_data[j])
            served_imsi_concatenated = ''.join(served_imsi_list)
            served_imsi = served_imsi_concatenated.replace("f", "")

        if mcfcall_record.getComponentByName("servedMSISDN") is None:
            served_msisdn = ""
        else:
            served_msisdn_binascii = binascii.hexlify(str(mcfcall_record.getComponentByName("servedMSISDN")))
            served_msisdn_list = []
            served_msisdn_data = list(served_msisdn_binascii)
            for k in xrange(0, len(served_msisdn_data) - 1, 2):
                served_msisdn_list.append(served_msisdn_data[k+1] + "" + served_msisdn_data[k])
            served_msisdn_concatenated = ''.join(served_msisdn_list)
            served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
            served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        if mcfcall_record.getComponentByName("serviceCategory") is None:
            service_category = ""
        else:
            service_category = binascii.hexlify(str(mcfcall_record.getComponentByName("serviceCategory")))

        if mcfcall_record.getComponentByName("serviceChangeInitiator") is None:
            service_change_initiator = -255
        else:
            service_change_initiator = long(str(mcfcall_record.getComponentByName("serviceChangeInitiator")))

        if mcfcall_record.getComponentByName("serviceKey") is None:
            service_key = -255
        else:
            service_key = long(str(mcfcall_record.getComponentByName("serviceKey")))

        if mcfcall_record.getComponentByName("serviceKey_2") is None:
            service_key_2 = -255
        else:
            service_key_2 = long(str(mcfcall_record.getComponentByName("serviceKey_2")))

        if mcfcall_record.getComponentByName("serviceKey_3") is None:
            service_key_3 = -255
        else:
            service_key_3 = long(str(mcfcall_record.getComponentByName("serviceKey_3")))

        start_time = append_timezone_offset(parseTimestamp('1606151209292d0700'))

        if mcfcall_record.getComponentByName("subscriberCategory") is None:
            subscriber_category = ""
        else:
            subscriber_category = binascii.hexlify(str(mcfcall_record.getComponentByName("subscriberCategory")))

        if mcfcall_record.getComponentByName("suppressCAMELInd") is None:
            suppress_camel_ind = ""
        else:
            suppress_camel_ind = str(mcfcall_record.getComponentByName("suppressCAMELInd"))

        if mcfcall_record.getComponentByName("systemType") is None:
            system_type = ""
        else:
            system_type_tmp = long(str(mcfcall_record.getComponentByName("systemType")))
            system_type = ""
            if system_type_tmp == 0:
                system_type = "unknown"
            elif system_type_tmp == 1:
                system_type = "iuUTRAN"
            elif system_type_tmp == 2:
                system_type = "gERAN"

        if mcfcall_record.getComponentByName("translatedNumber") is None:
            translated_number = ""
        else:
            translated_number_binascii = binascii.hexlify(str(mcfcall_record.getComponentByName("translatedNumber")))
            translated_number_list = []
            translated_number_data = list(translated_number_binascii)
            for l in xrange(0, len(translated_number_data) - 1, 2):
                translated_number_list.append(translated_number_data[l + 1] + "" + translated_number_data[l])
            translated_number_concatenated = ''.join(translated_number_list)
            translated_number = translated_number_concatenated[2:]

        if mcfcall_record.getComponentByName("transparencyIndicator") is None:
            transparency_indicator = ""
        else:
            transparency_indicator_tmp = long(str(mcfcall_record.getComponentByName("transparencyIndicator")))
            transparency_indicator = ""
            if transparency_indicator_tmp == 0:
                transparency_indicator = "transparent"
            elif transparency_indicator_tmp == 1:
                transparency_indicator = "nonTransparent"

        if mcfcall_record.getComponentByName("typeOfServiceChange") is None:
            types_of_service_change = ""
        else:
            types_of_service_change_tmp = long(str(mcfcall_record.getComponentByName("typeOfServiceChange")))
            types_of_service_change = ""
            if types_of_service_change_tmp == 0:
                types_of_service_change = "changeToSpeech"
            elif types_of_service_change_tmp == 1:
                types_of_service_change = "changeToMultimedia"

        if mcfcall_record.getComponentByName("wANAPPS") is None:
            wanapps = -255
        else:
            wanapps = long(str(mcfcall_record.getComponentByName("wANAPPS")))

        mcfcall_record = [charge_indicator, charged_party, answer_time, bsdidentification,
                          basic_service, bsdidentification_24, by_pass, by_pass_flag2, camel_initcf_indicator,
                          cug_category_calltype, cug_category_usertype, cug_index, cug_interlock_code,
                          cug_outgoing_accessind, cug_outgoing_accessused, call_duration,
                          call_reference,
                          called_location_cellid, called_location_area_code, called_location_plmn,
                          called_location_sac, called_number,
                          calling_number, calling_party_roamingind, carp, cause_for_termination,
                          connected_number, data_rate, data_volume, default_call_handling,
                          default_call_handling2, default_call_handling3, diagnostics,
                          exchange_identity,
                          file_id, fnur, forwarded_to_imsi, free_format_data, free_format_data_append,
                          free_format_data_append_2, free_format_data_append_3, free_format_data_2,
                          free_format_data_3, global_call_reference, gsm_scfaddress, gsm_scfaddress_2,
                          gsm_scfaddress_3, hot_billing_tag, hot_billing_tag2, inapmnpport_status,
                          inapmnquerymethod, inapmn_routing_number, incoming_cic_channel, incoming_cic_pcmunit,
                          incoming_tkgpname,
                          incoming_traffic_type, is_camel_call, is_cug_used, is_inapmnquery, is_multi_media_call,
                          last_long_part_ind,
                          level_of_camel_service, local_office_forward, location_cellid, location_area_code,
                          location_plmn, location_sac, mnpnumber, mnproutingnumber, msc_address,
                          mcf_type, milli_sec_duration, msc_incoming_tkgp, msc_outgoing_tkgp, mscspc14,
                          mscspc24, network_call_reference, number_of_dp_encountered, number_of_forwarding,
                          operator_id, original_called_number, outpulsed_number, outgoing_cic_channel,
                          outgoing_cic_pcmunit,
                          outgoing_tkgpname, outgoing_traffic_type, partial_record_type, pbrt,
                          rate_indication, reason_for_service_change, record_sequence_number, record_type,
                          recording_entity, release_time, roaming_number, routing_category, seizure_time,
                          sequence_number, served_imei, served_imsi, served_msisdn, service_category,
                          service_change_initiator, service_key, service_key_2, service_key_3, start_time,
                          subscriber_category, suppress_camel_ind, system_type, translated_number,
                          transparency_indicator, types_of_service_change, wanapps,
                          long(seizure_time.strftime("%Y%m%d")), long(seizure_time.strftime("%H"))
                          ]

        return mcfcall_record
