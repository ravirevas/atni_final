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


class ProcessZTEumtsmtLCSRecordRecord:
    def __init__(self):
        pass

    @staticmethod
    def process_mtlcs_records(mtlcs_record, file_id, file_name):

        additional_chg_info = mtlcs_record.getComponentByName("additionalChgInfo")

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

        if mtlcs_record.getComponentByName("causeForTerm") is None:
            cause_for_termination = ""
        else:
            cause_for_termination_tmp = long(str(mtlcs_record.getComponentByName("causeForTerm")))
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

        if mtlcs_record.getComponentByName("diagnostics") is None:
            diagnostics = ""
        else:
            diagnostics = mtlcs_record.getComponentByName("diagnostics").getName()

        event_timestamp_tmp = binascii.hexlify(str(mtlcs_record.getComponentByName("eventTimeStamp")))
        event_timestamp = append_timezone_offset(parseTimestamp(event_timestamp_tmp))

        if mtlcs_record.getComponentByName("hotBillingTag") is None:
            hot_billing_tag = -255
        else:
            hot_billing_tag = long(str(mtlcs_record.getComponentByName("hotBillingTag")))

        if mtlcs_record.getComponentByName("hotBillingTag2") is None:
            hot_billing_tag2 = -255
        else:
            hot_billing_tag2 = long(str(mtlcs_record.getComponentByName("hotBillingTag2")))

        if mtlcs_record.getComponentByName("lcsCause") is None:
            lcs_cause = ""
        else:
            lcs_cause = str(mtlcs_record.getComponentByName("lcsCause"))

        lcs_client_identity = mtlcs_record.getComponentByName("lcsClientIdentity")

        if lcs_client_identity is None:
            lcs_client_external_address = ""
        else:
            lcs_client_external_id = lcs_client_identity.getComponentByName("lcsClientExternalID")
            lcs_client_external_address_binascii = binascii.hexlify(
                    str(lcs_client_external_id.getComponentByName("externalAddress")))
            lcs_client_external_address_list = []
            lcs_client_external_address_data = list(lcs_client_external_address_binascii)
            for k in xrange(0, len(lcs_client_external_address_data) - 1, 2):
                lcs_client_external_address_list.append(
                        lcs_client_external_address_data[k + 1] + "" + lcs_client_external_address_data[k])
            lcs_client_external_address_concatenated = ''.join(lcs_client_external_address_list)
            lcs_client_external_address = lcs_client_external_address_concatenated[2:].replace("f", "")

        if lcs_client_identity is None:
            lcs_client_dialedbyms = ""
        else:
            if lcs_client_identity.getComponentByName("lcsClientDialedByMS") is None:
                lcs_client_dialedbyms = ""
            else:
                lcs_client_dialedbyms = str(lcs_client_identity.getComponentByName("lcsClientDialedByMS"))

        if lcs_client_identity is None:
            lcs_client_internalid = ""
        else:
            if lcs_client_identity.getComponentByName("lcsClientInternalID") is None:
                lcs_client_internalid = ""
            else:
                lcs_client_internalid = str(lcs_client_identity.getComponentByName("lcsClientInternalID"))

        if mtlcs_record.getComponentByName("lcsClientType") is None:
            lcs_clienttype = ""
        else:
            lcs_clienttype_tmp = long(str(mtlcs_record.getComponentByName("lcsClientType")))
            lcs_clienttype = ""
            if lcs_clienttype_tmp == 0:
                lcs_clienttype = "emergencyServices"
            elif lcs_clienttype_tmp == 1:
                lcs_clienttype = "valueAddedServices"
            elif lcs_clienttype_tmp == 2:
                lcs_clienttype = "plmnOperatorServices"
            elif lcs_clienttype_tmp == 3:
                lcs_clienttype = "lawfulInterceptServices"

        if mtlcs_record.getComponentByName("lcsPriority") is None:
            lcs_priority = ""
        else:
            lcs_priority_tmp = binascii.hexlify(str(mtlcs_record.getComponentByName("lcsPriority")))
            lcs_priority = lcs_priority_tmp[:2]

        if mtlcs_record.getComponentByName("lcsQos") is None:
            lcs_qos = ""
        else:
            lcs_qos = binascii.hexlify(str(mtlcs_record.getComponentByName("lcsQos")))

        if mtlcs_record.getComponentByName("locationEstimate") is None:
            location_estimate = ""
        else:
            location_estimate = str(mtlcs_record.getComponentByName("locationEstimate"))

        location = mtlcs_record.getComponentByName("location")
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
            location_cellid = ""
            location_area_code = ""
            location_plmn = ""
            location_sac = ""

        location_type = mtlcs_record.getComponentByName("locationType")

        if location_type is None:
            deferred_location_eventtype = ""
        else:
            if location_type.getComponentByName("deferredLocationEventType") is None:
                deferred_location_eventtype = ""
            else:
                deferred_location_eventtype = str(location_type.getComponentByName("deferredLocationEventType"))

        if location_type is None:
            location_estimate_type = ""
        else:
            location_estimate_type_tmp = long(str(location_type.getComponentByName("locationEstimateType")))
            location_estimate_type = ""
            if location_estimate_type_tmp == 0:
                location_estimate_type = "currentLocation"
            elif location_estimate_type == 1:
                location_estimate_type = "currentOrLastKnownLocation"
            elif location_estimate_type == 2:
                location_estimate_type = "initialLocation"
            elif location_estimate_type_tmp == 3:
                location_estimate_type = "activateDeferredLocation"
            elif location_estimate_type_tmp == 4:
                location_estimate_type = "cancelDeferredLocation"

        if mtlcs_record.getComponentByName("measureDuration") is None:
            measure_duration = -255
        else:
            measure_duration = long(str(mtlcs_record.getComponentByName("measureDuration")))

        if mtlcs_record.getComponentByName("mlc-Number") is None:
            mlc_number = ""
        else:
            mlc_number_binascii = binascii.hexlify(str(mtlcs_record.getComponentByName("mlc-Number")))
            mlc_number_list = []
            mlc_number_data = list(mlc_number_binascii)
            for k in xrange(0, len(mlc_number_data) - 1, 2):
                mlc_number_list.append(mlc_number_data[k + 1] + "" + mlc_number_data[k])
            mlc_number_concatenated = ''.join(mlc_number_list)
            mlc_number = mlc_number_concatenated[2:].replace("f", "")

        if mtlcs_record.getComponentByName("notificationToMSUser") is None:
            notification_to_msuser = ""
        else:
            notification_to_msuser_tmp = long(str(mtlcs_record.getComponentByName("notificationToMSUser")))
            notification_to_msuser = ""
            if notification_to_msuser_tmp == 0:
                notification_to_msuser = "notifyLocationAllowed"
            elif notification_to_msuser_tmp == 1:
                notification_to_msuser = "notifyAndVerify-LocationAllowedIfNoResponse"
            elif notification_to_msuser_tmp == 2:
                notification_to_msuser = "notifyAndVerify-LocationNotAllowedIfNoRespons"
            elif notification_to_msuser_tmp == 3:
                notification_to_msuser = "locationNotAllowed"

        if mtlcs_record.getComponentByName("operatorId") is None:
            operator_id = -255
        else:
            operator_id = long(str(mtlcs_record.getComponentByName("operatorId")))

        if mtlcs_record.getComponentByName("positioningData") is None:
            positioning_data = ""
        else:
            positioning_data = str(mtlcs_record.getComponentByName("positioningData"))

        privacy_override_get_value = mtlcs_record.getComponentByName("privacyOverride")
        privacy_override = boolean_value(privacy_override_get_value)

        if mtlcs_record.getComponentByName("recordingEntity") is None:
            recording_entity = ""
        else:
            recording_entity_binascii = binascii.hexlify(str(mtlcs_record.getComponentByName("recordingEntity")))
            recording_entity_list = []
            recording_entity_data = list(recording_entity_binascii)
            for l in xrange(0, len(recording_entity_data) - 1, 2):
                recording_entity_list.append(recording_entity_data[l + 1] + "" + recording_entity_data[l])
            recording_entity_concatenated = ''.join(recording_entity_list)
            recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
            recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        if mtlcs_record.getComponentByName("recordSequenceNumber") is None:
            record_sequence_number = ""
        else:
            record_sequence_number = binascii.hexlify(str(mtlcs_record.getComponentByName("recordSequenceNumber")))

        if mtlcs_record.getComponentByName("recordType") is None:
            record_type = -255
        else:
            record_type = long(str(mtlcs_record.getComponentByName("recordType")))

        if mtlcs_record.getComponentByName("servedIMSI") is None:
            served_imsi = ""
        else:
            served_imsi_binascii = binascii.hexlify(str(mtlcs_record.getComponentByName("servedIMSI")))
            served_imsi_list = []
            served_imsi_data = list(served_imsi_binascii)
            for j in xrange(0, len(served_imsi_data) - 1, 2):
                served_imsi_list.append(served_imsi_data[j + 1] + "" + served_imsi_data[j])
            served_imsi_concatenated = ''.join(served_imsi_list)
            served_imsi = served_imsi_concatenated.replace("f", "")

        if mtlcs_record.getComponentByName("servedMSISDN") is None:
            served_msisdn = ""
        else:
            served_msisdn_binascii = binascii.hexlify(str(mtlcs_record.getComponentByName("servedMSISDN")))
            served_msisdn_list = []
            served_msisdn_data = list(served_msisdn_binascii)
            for k in xrange(0, len(served_msisdn_data) - 1, 2):
                served_msisdn_list.append(served_msisdn_data[k + 1] + "" + served_msisdn_data[k])
            served_msisdn_concatenated = ''.join(served_msisdn_list)
            served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
            served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        if mtlcs_record.getComponentByName("systemType") is None:
            system_type = ""
        else:
            system_type_tmp = long(str(mtlcs_record.getComponentByName("systemType")))
            system_type = ""
            if system_type_tmp == 0:
                system_type = "unknown"
            elif system_type_tmp == 1:
                system_type = "iuUTRAN"
            elif system_type_tmp == 2:
                system_type = "gERAN"

        mtlcs_record = [charge_indicator, charged_party, cause_for_termination, diagnostics,
                        event_timestamp, file_id, hot_billing_tag, hot_billing_tag2, lcs_cause,
                        lcs_client_external_address, lcs_client_dialedbyms, lcs_client_internalid,
                        lcs_clienttype, lcs_priority, lcs_qos, location_cellid, location_estimate,
                        location_area_code, location_plmn, location_sac, deferred_location_eventtype,
                        location_estimate_type, measure_duration, mlc_number, notification_to_msuser,
                        operator_id, positioning_data, privacy_override, record_sequence_number,
                        record_type, recording_entity, served_imsi, served_msisdn, system_type,
                        long(event_timestamp.strftime("%Y%m%d")),
                        long(event_timestamp.strftime("%H"))]

        return mtlcs_record
