import binascii

from common.constants import *

from common.utilities import parseTimestamp


class ProcessZTEumtsHLRIntRecord:
    def __init__(self):
        pass

    @staticmethod
    def process_hlrint_records(hlrint_record, file_name):

        if hlrint_record.getComponentByName("basicService") is None:
            basic_service = ""
        else:
            basic_service = str(hlrint_record.getComponentByName("basicService").prettyPrint())

        calling_number_before_sdc = str(hlrint_record.getComponentByName("callingNumberBeforeSDC"))

        dc_interrogated_ori_called_number = str(hlrint_record.getComponentByName("dcInterrogatedOriCalledNumber"))

        exchange_identity = str(hlrint_record.getComponentByName("exchangeIdentity"))

        file_name = file_name

        if hlrint_record.getComponentByName("hotBillingTag2") is None:
            hot_billing_tag2 = ''
        else:
            hot_billing_tag2 = long(str(hlrint_record.getComponentByName("hotBillingTag2")))

        interrogation_result = str(hlrint_record.getComponentByName("interrogationResult"))

        interrogation_time_tmp = binascii.hexlify(str(hlrint_record.getComponentByName("interrogationTime")))
        interrogation_time = parseTimestamp(interrogation_time_tmp)

        number_of_forwarding = long(str(hlrint_record.getComponentByName("numberOfForwarding")))

        if hlrint_record.getComponentByName("operatorId") is None:
            operator_id = 0
        else:
            operator_id = long(str(hlrint_record.getComponentByName("operatorId")))

        ori_called_number_before_sdc = str(hlrint_record.getComponentByName("oriCalledNumberBeforeSDC"))

        recording_entity_binascii = binascii.hexlify(str(hlrint_record.getComponentByName("recordingEntity")))
        recording_entity_list = []
        recording_entity_data = list(recording_entity_binascii)
        for l in xrange(0, len(recording_entity_data) - 1, 2):
            recording_entity_list.append(recording_entity_data[l + 1] + "" + recording_entity_data[l])
        recording_entity_concatenated = ''.join(recording_entity_list)
        recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
        recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        record_sequence_number = binascii.hexlify(str(hlrint_record.getComponentByName("recordSequenceNumber")))

        if hlrint_record.getComponentByName("recordType") is None:
            record_type = 0
        else:
            record_type = long(str(hlrint_record.getComponentByName("recordType")))

        routing_number = str(hlrint_record.getComponentByName("routingNumber"))

        sdc_interrogated_calling_number = str(hlrint_record.getComponentByName("sdcInterrogatedCallingNumber"))

        sdc_interrogation_flag = str(hlrint_record.getComponentByName("sdcInterrogationFlag"))

        served_imsi_binascii = binascii.hexlify(str(hlrint_record.getComponentByName("servedIMSI")))
        served_imsi_list = []
        served_imsi_data = list(served_imsi_binascii)
        for j in xrange(0, len(served_imsi_data) - 1, 2):
            served_imsi_list.append(served_imsi_data[j + 1] + "" + served_imsi_data[j])
        served_imsi_concatenated = ''.join(served_imsi_list)
        served_imsi = served_imsi_concatenated.replace("f", "")

        served_msisdn_binascii = binascii.hexlify(str(hlrint_record.getComponentByName("servedMSISDN")))
        served_msisdn_list = []
        served_msisdn_data = list(served_msisdn_binascii)
        for k in xrange(0, len(served_msisdn_data) - 1, 2):
            served_msisdn_list.append(served_msisdn_data[k + 1] + "" + served_msisdn_data[k])
        served_msisdn_concatenated = ''.join(served_msisdn_list)
        served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
        served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        hlrint_record = [basic_service, calling_number_before_sdc, dc_interrogated_ori_called_number, exchange_identity
            , file_name, hot_billing_tag2, interrogation_result, interrogation_time, number_of_forwarding,
                         operator_id, ori_called_number_before_sdc, recording_entity, record_sequence_number,
                         record_type, routing_number, sdc_interrogated_calling_number, sdc_interrogation_flag,
                         served_imsi, served_msisdn]

        return hlrint_record
