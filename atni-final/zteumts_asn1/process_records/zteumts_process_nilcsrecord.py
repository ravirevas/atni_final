import binascii

from common.constants import *

from common.utilities import parseTimestamp


class ProcessZTEumtsNILCSRecord:
    def __init__(self):
        pass

    @staticmethod
    def process_nilcs_records(nilcs_record, file_name):

        additional_chg_info = nilcs_record.getComponentByName("additionalChgInfo")

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

        if nilcs_record.getComponentByName("causeForTerm") is None:
            cause_for_termination = ""
        else:
            cause_for_termination_tmp = long(str(nilcs_record.getComponentByName("causeForTerm")))
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

        if nilcs_record.getComponentByName("diagnostics") is None:
            diagnostics = ""
        else:
            diagnostics = str(nilcs_record.getComponentByName("diagnostics"))

        ems_digits = str(nilcs_record.getComponentByName("emsDigits"))

        ems_key = str(nilcs_record.getComponentByName("emsKey"))

        event_timestamp_tmp = binascii.hexlify(str(nilcs_record.getComponentByName("eventTimeStamp")))
        event_timestamp = parseTimestamp(event_timestamp_tmp)

        file_name = file_name

        if nilcs_record.getComponentByName("hotBillingTag") is None:
            hot_billing_tag = 0
        else:
            hot_billing_tag = long(str(nilcs_record.getComponentByName("hotBillingTag")))

        if nilcs_record.getComponentByName("hotBillingTag2") is None:
            hot_billing_tag2 = 0
        else:
            hot_billing_tag2 = long(str(nilcs_record.getComponentByName("hotBillingTag2")))

        lcs_cause = str(nilcs_record.getComponentByName("lcsCause"))

        lcs_client_identity = nilcs_record.getComponentByName("lcsClientIdentity")

        if lcs_client_identity is None:
            lcs_client_external_address = ""
        else:
            lcs_client_external_address = str(lcs_client_identity.getComponentByName("externalAddress"))

        if lcs_client_identity is None:
            lcs_client_dialedbyms = ""
        else:
            lcs_client_dialedbyms = str(lcs_client_identity.getComponentByName("lcsClientDialedByMS"))

        if lcs_client_identity is None:
            lcs_client_internalid = ""
        else:
            lcs_client_internalid = str(lcs_client_identity.getComponentByName("lcsClientInternalID"))

        if nilcs_record.getComponentByName("lcsClientType") is None:
            lcs_clienttype = ""
        else:
            lcs_clienttype_tmp = long(str(nilcs_record.getComponentByName("lcsClientType")))
            lcs_clienttype = ""
            if lcs_clienttype_tmp == 0:
                lcs_clienttype = "emergencyServices"
            elif lcs_clienttype_tmp == 1:
                lcs_clienttype = "valueAddedServices"
            elif lcs_clienttype_tmp == 2:
                lcs_clienttype = "plmnOperatorServices"
            elif lcs_clienttype_tmp == 3:
                lcs_clienttype = "lawfulInterceptServices"

        lcs_priority = str(nilcs_record.getComponentByName("lcsPriority"))

        lcs_qos = binascii.hexlify(str(nilcs_record.getComponentByName("lcsQos")))

        location_estimate = str(nilcs_record.getComponentByName("locationEstimate"))

        if nilcs_record.getComponentByName("measureDuration") is None:
            measure_duration = 0
        else:
            measure_duration = long(str(nilcs_record.getComponentByName("measureDuration")))

        mlc_number_binascii = binascii.hexlify(str(nilcs_record.getComponentByName("mlc-Number")))
        mlc_number_list = []
        mlc_number_data = list(mlc_number_binascii)
        for k in xrange(0, len(mlc_number_data) - 1, 2):
            mlc_number_list.append(mlc_number_data[k+1] + "" + mlc_number_data[k])
        mlc_number_concatenated = ''.join(mlc_number_list)
        mlc_number = mlc_number_concatenated[2:].replace("f", "")

        if nilcs_record.getComponentByName("operatorId") is None:
            operator_id = 0
        else:
            operator_id = long(str(nilcs_record.getComponentByName("operatorId")))

        positioning_data = str(nilcs_record.getComponentByName("positioningData"))

        recording_entity_binascii = binascii.hexlify(str(nilcs_record.getComponentByName("recordingEntity")))
        recording_entity_list = []
        recording_entity_data = list(recording_entity_binascii)
        for l in xrange(0, len(recording_entity_data) - 1, 2):
            recording_entity_list.append(recording_entity_data[l+1] + "" + recording_entity_data[l])
        recording_entity_concatenated = ''.join(recording_entity_list)
        recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
        recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        record_sequence_number = binascii.hexlify(str(nilcs_record.getComponentByName("recordSequenceNumber")))

        record_type = long(str(nilcs_record.getComponentByName("recordType")))

        served_imei_binascii = binascii.hexlify(str(nilcs_record.getComponentByName("servedIMEI")))
        served_imei_list = []
        served_imei_data = list(served_imei_binascii)
        for i in xrange(0, len(served_imei_data) - 1, 2):
            served_imei_list.append(served_imei_data[i + 1] + "" + served_imei_data[i])
        served_imei_concatenated = ''.join(served_imei_list)
        served_imei = served_imei_concatenated.replace("f", "")

        served_imsi_binascii = binascii.hexlify(str(nilcs_record.getComponentByName("servedIMSI")))
        served_imsi_list = []
        served_imsi_data = list(served_imsi_binascii)
        for j in xrange(0, len(served_imsi_data) - 1, 2):
            served_imsi_list.append(served_imsi_data[j + 1] + "" + served_imsi_data[j])
        served_imsi_concatenated = ''.join(served_imsi_list)
        served_imsi = served_imsi_concatenated.replace("f", "")

        served_msisdn_binascii = binascii.hexlify(str(nilcs_record.getComponentByName("servedMSISDN")))
        served_msisdn_list = []
        served_msisdn_data = list(served_msisdn_binascii)
        for k in xrange(0, len(served_msisdn_data) - 1, 2):
            served_msisdn_list.append(served_msisdn_data[k + 1] + "" + served_msisdn_data[k])
        served_msisdn_concatenated = ''.join(served_msisdn_list)
        served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
        served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        if nilcs_record.getComponentByName("systemType") is None:
            system_type = ""
        else:
            system_type_tmp = long(str(nilcs_record.getComponentByName("systemType")))
            system_type = ""
            if system_type_tmp == 0:
                system_type = "unknown"
            elif system_type_tmp == 1:
                system_type = "iuUTRAN"
            elif system_type_tmp == 2:
                system_type = "gERAN"

        nilcs_record = [charged_party, charge_indicator, cause_for_termination, diagnostics, ems_digits, ems_key,
                        event_timestamp, file_name, hot_billing_tag, hot_billing_tag2, lcs_cause,
                        lcs_client_external_address, lcs_client_dialedbyms, lcs_client_internalid, lcs_clienttype,
                        lcs_priority, lcs_qos, location_estimate, measure_duration, mlc_number, operator_id,
                        positioning_data, recording_entity, record_sequence_number, record_type, served_imei,
                        served_imsi, served_msisdn, system_type]
        return nilcs_record
