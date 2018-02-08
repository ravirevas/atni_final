import binascii

from common.constants import *

from common.utilities import parseTimestamp


class ProcessZTEumtsROAMRecord:
    def __init__(self):
        pass

    @staticmethod
    def process_roam_records(roam_record, file_name):

        answer_time_tmp = binascii.hexlify(str(roam_record.getComponentByName("answerTime")))
        answer_time = parseTimestamp(answer_time_tmp)

        if roam_record.getComponentByName("basicService") is None:
            basic_service = ""
        else:
            basic_service_choice = roam_record.getComponentByName("basicService")
            basic_service_choice_hexvalue = basic_service_choice.getComponent().prettyPrint()
            if basic_service_choice_hexvalue == "0x11":
                basic_service = "teleservice"
            else:
                basic_service = "bearerservice"

        if roam_record.getComponentByName("bSCIdentification") is None:
            bsdidentification = ""
        else:
            bsdidentification = binascii.hexlify(str(roam_record.getComponentByName("bSCIdentification")))

        if roam_record.getComponentByName("bscIdentification24") is None:
            bsdidentification_24 = ""
        else:
            bsdidentification_24 = binascii.hexlify(str(roam_record.getComponentByName("bscIdentification24")))

        if roam_record.getComponentByName("callDuration") is None:
            call_duration = 0
        else:
            call_duration = long(str(roam_record.getComponentByName("callDuration")))

        calling_number_binascii = binascii.hexlify(
                str(roam_record.getComponentByName("callingNumber")))
        calling_number_list = []
        calling_number_data = list(calling_number_binascii)
        for i in xrange(0, len(calling_number_data) - 1, 2):
            calling_number_list.append(calling_number_data[i + 1] + "" + calling_number_data[i])
        calling_number_concatenated = ''.join(calling_number_list)
        calling_number = calling_number_concatenated[2:]

        call_reference = binascii.hexlify(str(roam_record.getComponentByName("callReference")))

        if roam_record.getComponentByName("causeForTerm") is None:
            cause_for_termination = ""
        else:
            cause_for_termination_tmp = long(str(roam_record.getComponentByName("causeForTerm")))
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

        cug_category = roam_record.getComponentByName("cUGCategory")

        if cug_category is None:
            cug_category_calltype = 0
        else:
            cug_category_calltype = long(str(cug_category.getComponentByName("callType")))

        if cug_category is None:
            cug_category_usertype = 0
        else:
            cug_category_usertype = long(str(cug_category.getComponentByName("userType")))

        cug_interlock_code = str(roam_record.getComponentByName("cUGInterLockCode"))

        if roam_record.getComponentByName("dataRate") is None:
            data_rate = 0
        else:
            data_rate = long(str(roam_record.getComponentByName("dataRate")))

        if roam_record.getComponentByName("dataVolume") is None:
            data_volume = 0
        else:
            data_volume = long(str(roam_record.getComponentByName("dataVolume")))

        if roam_record.getComponentByName("diagnostics") is None:
            diagnostics = ""
        else:
            diagnostics = str(roam_record.getComponentByName("diagnostics"))

        exchange_identity = str(roam_record.getComponentByName("exchangeIdentity"))

        file_name = file_name

        global_call_reference = str(roam_record.getComponentByName("globalCallReference"))

        if roam_record.getComponentByName("hotBillingTag2") is None:
            hot_billing_tag2 = 0
        else:
            hot_billing_tag2 = long(str(roam_record.getComponentByName("hotBillingTag2")))

        incoming_cic = roam_record.getComponentByName("incomingCic")

        if incoming_cic is None:
            incoming_cic_channel = 0
        else:
            incoming_cic_channel = long(str(incoming_cic.getComponentByName("channel")))

        if incoming_cic is None:
            incoming_cic_pcmunit = 0
        else:
            incoming_cic_pcmunit = long(str(incoming_cic.getComponentByName("pcmUnit")))

        incoming_tkgpname = str(roam_record.getComponentByName("incomingTKGPName"))

        if roam_record.getComponentByName("incomingTrafficType") is None:
            incoming_traffic_type = ""
        else:
            incoming_traffic_type_tmp = long(str(roam_record.getComponentByName("incomingTrafficType")))
            incoming_traffic_type = ""
            if incoming_traffic_type_tmp == 0:
                incoming_traffic_type = "unknown"
            elif incoming_traffic_type_tmp == 1:
                incoming_traffic_type = "localNetworkMobile"
            elif incoming_traffic_type_tmp == 2:
                incoming_traffic_type = "localNetworkFixed"
            elif incoming_traffic_type_tmp == 3:
                incoming_traffic_type = "externalNetwork"

        is_camel_call = str(roam_record.getComponentByName("isCAMELCall"))

        is_cug_used = str(roam_record.getComponentByName("isCUGUsed"))

        is_multi_media_call = str(roam_record.getComponentByName("isMultiMediaCall"))

        last_long_part_ind = str(roam_record.getComponentByName("lastLongPartInd"))

        if roam_record.getComponentByName("millisecDuration") is None:
            milli_sec_duration = 0
        else:
            milli_sec_duration = long(str(roam_record.getComponentByName("millisecDuration")))

        msc_address = str(roam_record.getComponentByName("mSCAddress"))

        msc_incoming_tkgp = binascii.hexlify(str(roam_record.getComponentByName("mscIncomingTKGP")))

        if roam_record.getComponentByName("mscOutgoingTKGP") is None:
            msc_outgoing_tkgp = 0
        else:
            msc_outgoing_tkgp = binascii.hexlify(str(roam_record.getComponentByName("mscOutgoingTKGP")))

        mscspc14_tmp = binascii.hexlify(str(roam_record.getComponentByName("mscSPC14")))

        mscspc24_tmp = binascii.hexlify(str(roam_record.getComponentByName("mscSPC24")))

        if roam_record.getComponentByName("networkCallReference") is None:
            network_call_reference = ""
        else:
            network_call_reference = binascii.hexlify(str(roam_record.getComponentByName("networkCallReference")))

        if roam_record.getComponentByName("operatorId") is None:
            operator_id = 0
        else:
            operator_id = long(str(roam_record.getComponentByName("operatorId")))

        outgoing_cic = roam_record.getComponentByName("outgoingCic")

        if outgoing_cic is None:
            outgoing_cic_channel = 0
        else:
            outgoing_cic_channel = long(str(outgoing_cic.getComponentByName("channel")))

        if outgoing_cic is None:
            outgoing_cic_pcmunit = 0
        else:
            outgoing_cic_pcmunit = long(str(outgoing_cic.getComponentByName("pcmUnit")))

        outgoing_tkgpname = str(roam_record.getComponentByName("outgoingTKGPName"))

        if roam_record.getComponentByName("outgoingTrafficType") is None:
            outgoing_traffic_type = ""
        else:
            outgoing_traffic_type_tmp = long(str(roam_record.getComponentByName("outgoingTrafficType")))
            outgoing_traffic_type = ""
            if outgoing_traffic_type_tmp == 0:
                outgoing_traffic_type = "unknown"
            elif outgoing_traffic_type_tmp == 1:
                outgoing_traffic_type = "localNetworkMobile"
            elif outgoing_traffic_type_tmp == 2:
                outgoing_traffic_type = "localNetworkFixed"
            elif outgoing_traffic_type_tmp == 3:
                outgoing_traffic_type = "externalNetwork"

        recording_entity_binascii = binascii.hexlify(str(roam_record.getComponentByName("recordingEntity")))
        recording_entity_list = []
        recording_entity_data = list(recording_entity_binascii)
        for l in xrange(0, len(recording_entity_data) - 1, 2):
            recording_entity_list.append(recording_entity_data[l + 1] + "" + recording_entity_data[l])
        recording_entity_concatenated = ''.join(recording_entity_list)
        recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
        recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        record_sequence_number = binascii.hexlify(str(roam_record.getComponentByName("recordSequenceNumber")))

        record_type = long(str(roam_record.getComponentByName("recordType")))

        redirect_number = str(roam_record.getComponentByName("redirectNumber"))

        release_time_tmp = binascii.hexlify(str(roam_record.getComponentByName("releaseTime")))
        release_time = parseTimestamp(release_time_tmp)

        roam_camel_information = roam_record.getComponentByName("roamCamelInformation")

        if roam_camel_information is not None:
            if roam_camel_information.getComponentByName("defaultCallHandling") is None:
                roam_default_call_handling = ""
            else:
                roam_default_call_handling_tmp = long(
                    str(roam_camel_information.getComponentByName("defaultCallHandling")))
                roam_default_call_handling = ""
                if roam_default_call_handling_tmp == 0:
                    roam_default_call_handling = "continueCall"
                elif roam_default_call_handling_tmp == 1:
                    roam_default_call_handling = "releaseCall"

            roam_dest_route_number = str(roam_camel_information.getComponentByName("destRouteNumber"))

            if roam_camel_information.getComponentByName("gsm-SCFAddress") is not None:
                gsm_scfaddress_binascii = binascii.hexlify(
                        str(roam_camel_information.getComponentByName("gsm-SCFAddress")))
                gsm_scfaddress_list = []
                gsm_scfaddress_data = list(gsm_scfaddress_binascii)
                for i in xrange(0, len(gsm_scfaddress_data) - 1, 2):
                    gsm_scfaddress_list.append(gsm_scfaddress_data[i + 1] + "" + gsm_scfaddress_data[i])
                    gsm_scfaddress_concatenated = ''.join(gsm_scfaddress_list)
                    roam_gsm_scfaddress = gsm_scfaddress_concatenated[2:].replace("f", "")
            else:
                roam_gsm_scfaddress = ""

            roam_interrogation_time_tmp = binascii.hexlify(
                str(roam_camel_information.getComponentByName("interrogationTime")))
            roam_interrogation_time = parseTimestamp(roam_interrogation_time_tmp)

            roam_msc_address = str(roam_camel_information.getComponentByName("mSCAddress"))

            roam_service_key = long(str(roam_camel_information.getComponentByName("serviceKey")))

        else:
            roam_default_call_handling = ""
            roam_dest_route_number = ""
            roam_gsm_scfaddress = ""
            roam_interrogation_time = parseTimestamp('1606151209292d0700')
            roam_msc_address = ""
            roam_service_key = 0

        if roam_record.getComponentByName("roamingNumber") is None:
            roaming_number = ""
        else:
            roaming_number_binascii = binascii.hexlify(str(roam_record.getComponentByName("roamingNumber")))
            roaming_number_list = []
            roaming_number_data = list(roaming_number_binascii)
            for i in xrange(0, len(roaming_number_data) - 1, 2):
                roaming_number_list.append(roaming_number_data[i + 1] + "" + roaming_number_data[i])
            roaming_number_concatenated = ''.join(roaming_number_list)
            roaming_number = roaming_number_concatenated[2:]

        routing_category = str(roam_record.getComponentByName("routingCategory"))

        seizure_time_tmp = binascii.hexlify(str(roam_record.getComponentByName("seizureTime")))
        seizure_time = parseTimestamp(seizure_time_tmp)

        if roam_record.getComponentByName("sequenceNumber") is None:
            sequence_number = 0
        else:
            sequence_number = long(str(roam_record.getComponentByName("sequenceNumber")))

        served_imsi_binascii = binascii.hexlify(str(roam_record.getComponentByName("servedIMSI")))
        served_imsi_list = []
        served_imsi_data = list(served_imsi_binascii)
        for j in xrange(0, len(served_imsi_data) - 1, 2):
            served_imsi_list.append(served_imsi_data[j + 1] + "" + served_imsi_data[j])
        served_imsi_concatenated = ''.join(served_imsi_list)
        served_imsi = served_imsi_concatenated.replace("f", "")

        served_msisdn_binascii = binascii.hexlify(str(roam_record.getComponentByName("servedMSISDN")))
        served_msisdn_list = []
        served_msisdn_data = list(served_msisdn_binascii)
        for k in xrange(0, len(served_msisdn_data) - 1, 2):
            served_msisdn_list.append(served_msisdn_data[k + 1] + "" + served_msisdn_data[k])
        served_msisdn_concatenated = ''.join(served_msisdn_list)
        served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
        served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        service_category = binascii.hexlify(str(roam_record.getComponentByName("serviceCategory")))

        start_time = parseTimestamp('1606151209292d0700')

        if roam_record.getComponentByName("transparencyIndicator") is None:
            transparency_indicator = ""
        else:
            transparency_indicator_tmp = long(str(roam_record.getComponentByName("transparencyIndicator")))
            transparency_indicator = ""
            if transparency_indicator_tmp == 0:
                transparency_indicator = "transparent"
            elif transparency_indicator_tmp == 1:
                transparency_indicator = "nonTransparent"

        trans_roaming_number = str(roam_record.getComponentByName("transRoamingNumber"))

        roam_record = [answer_time, basic_service, bsdidentification, bsdidentification_24, call_duration,
                       calling_number, call_reference, cause_for_termination, cug_category_calltype,
                       cug_category_usertype, cug_interlock_code, data_rate, data_volume, diagnostics, exchange_identity
                       , file_name, global_call_reference, hot_billing_tag2, incoming_cic_channel, incoming_cic_pcmunit,
                       incoming_tkgpname, incoming_traffic_type, is_camel_call, is_cug_used, is_multi_media_call,
                       last_long_part_ind, milli_sec_duration, msc_address, msc_incoming_tkgp, msc_outgoing_tkgp,
                       mscspc14_tmp, mscspc24_tmp, network_call_reference, operator_id, outgoing_cic_channel,
                       outgoing_cic_pcmunit, outgoing_tkgpname, outgoing_traffic_type, recording_entity,
                       record_sequence_number, record_type, redirect_number, release_time, roam_default_call_handling,
                       roam_dest_route_number, roam_gsm_scfaddress, roam_interrogation_time, roam_msc_address,
                       roam_service_key, roaming_number, routing_category, seizure_time, sequence_number,
                       served_imsi, served_msisdn, service_category, start_time, transparency_indicator,
                       trans_roaming_number]
        return roam_record




