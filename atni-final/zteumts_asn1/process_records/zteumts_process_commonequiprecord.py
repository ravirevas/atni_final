import binascii

from atni.parsers.common.utilities import parseTimestamp, append_timezone_offset, boolean_value


class ProcessZTEumtsCommonEquipRecord:

    def __init__(self):
        pass

    @staticmethod
    def process_commonequip_records(commonequip_record, file_id, file_name):

        if commonequip_record.getComponentByName("recordType") is None:
            record_type = -255
        else:
            record_type = long(str(commonequip_record.getComponentByName("recordType")))

        if commonequip_record.getComponentByName("equipmentType") is None:
            equipment_type = ""
        else:
            equipment_type = str(commonequip_record.getComponentByName("equipmentType"))

        if commonequip_record.getComponentByName("equipmentId") is None:
            equipment_id = -255
        else:
            equipment_id = long(str(commonequip_record.getComponentByName("equipmentId")))

        if commonequip_record.getComponentByName("servedIMSI") is None:
            served_imsi = ""
        else:
            served_imsi_binascii = binascii.hexlify(str(commonequip_record.getComponentByName("servedIMSI")))
            served_imsi_list = []
            served_imsi_data = list(served_imsi_binascii)
            for j in xrange(0, len(served_imsi_data) - 1, 2):
                served_imsi_list.append(served_imsi_data[j+1] + "" + served_imsi_data[j])
            served_imsi_concatenated = ''.join(served_imsi_list)
            served_imsi = served_imsi_concatenated.replace("f", "")

        if commonequip_record.getComponentByName("servedMSISDN") is None:
            served_msisdn = ""
        else:
            served_msisdn_binascii = binascii.hexlify(str(commonequip_record.getComponentByName("servedMSISDN")))
            served_msisdn_list = []
            served_msisdn_data = list(served_msisdn_binascii)
            for k in xrange(0, len(served_msisdn_data) - 1, 2):
                served_msisdn_list.append(served_msisdn_data[k+1] + "" + served_msisdn_data[k])
            served_msisdn_concatenated = ''.join(served_msisdn_list)
            served_msisdn_replace_f = served_msisdn_concatenated.replace("f", "")
            served_msisdn = served_msisdn_replace_f.replace(served_msisdn_replace_f[:3], '')

        if commonequip_record.getComponentByName("recordingEntity") is None:
            recording_entity = ""
        else:
            recording_entity_binascii = binascii.hexlify(str(commonequip_record.getComponentByName("recordingEntity")))
            recording_entity_list = []
            recording_entity_data = list(recording_entity_binascii)
            for l in xrange(0, len(recording_entity_data) - 1, 2):
                recording_entity_list.append(recording_entity_data[l+1] + "" + recording_entity_data[l])
            recording_entity_concatenated = ''.join(recording_entity_list)
            recording_entity_replace_f = recording_entity_concatenated.replace("f", "")
            recording_entity = recording_entity_replace_f[2:7] + recording_entity_replace_f[7:]

        if commonequip_record.getComponentByName("basicService") is None:
            basic_service = ""
        else:
            basic_service_choice = commonequip_record.getComponentByName("basicService")
            basic_service_choice_hexvalue = basic_service_choice.getComponent().prettyPrint()
            if basic_service_choice_hexvalue == "0x11":
                basic_service = "teleservice"
            else:
                basic_service = "bearerservice"

        seizure_time_tmp = binascii.hexlify(str(commonequip_record.getComponentByName("seizureTime")))
        seizure_time = append_timezone_offset(parseTimestamp(seizure_time_tmp))

        release_time_tmp = binascii.hexlify(str(commonequip_record.getComponentByName("releaseTime")))
        release_time = append_timezone_offset(parseTimestamp(release_time_tmp))

        if commonequip_record.getComponentByName("callDuration") is None:
            call_duration = -255
        else:
            call_duration = long(str(commonequip_record.getComponentByName("callDuration")))
        if commonequip_record.getComponentByName("callReference") is None:
            call_reference = ""
        else:
            call_reference = binascii.hexlify(str(commonequip_record.getComponentByName("callReference")))

        if commonequip_record.getComponentByName("sequenceNumber") is None:
            sequence_number = -255
        else:
            sequence_number = long(str(commonequip_record.getComponentByName("sequenceNumber")))

        last_long_part_ind_get_value = commonequip_record.getComponentByName("lastLongPartInd")
        last_long_part_ind = boolean_value(last_long_part_ind_get_value)

        if commonequip_record.getComponentByName("exchangeIdentity") is None:
            exchange_identity = ""
        else:
            exchange_identity = str(commonequip_record.getComponentByName("exchangeIdentity"))

        if commonequip_record.getComponentByName("recordSequenceNumber") is None:
            record_sequence_number = ""
        else:
            record_sequence_number = binascii.hexlify(str(commonequip_record.getComponentByName("recordSequenceNumber")))

        if commonequip_record.getComponentByName("systemType") is None:
            system_type = ""
        else:
            system_type_tmp = long(str(commonequip_record.getComponentByName("systemType")))
            system_type = ""
            if system_type_tmp == 0:
                system_type = "unknown"
            elif system_type_tmp == 1:
                system_type = "iuUTRAN"
            elif system_type_tmp == 2:
                system_type = "gERAN"

        if commonequip_record.getComponentByName("rateIndication") is None:
            rate_indication = ""
        else:
            rate_indication = str(commonequip_record.getComponentByName("rateIndication"))

        if commonequip_record.getComponentByName("fnur") is None:
            fnur = ""
        else:
            fnur_tmp = str(long(commonequip_record.getComponentByName("fnur")))
            if fnur_tmp == "":
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

        if commonequip_record.getComponentByName("hotBillingTag2") is None:
            hot_billing_tag2 = -255
        else:
            hot_billing_tag2 = long(str(commonequip_record.getComponentByName("hotBillingTag2")))

        if commonequip_record.getComponentByName("millisecDuration") is None:
            milli_sec_duration = -255
        else:
            milli_sec_duration = long(str(commonequip_record.getComponentByName("millisecDuration")))

        if commonequip_record.getComponentByName("operatorId") is None:
            operator_id = -255
        else:
            operator_id = long(str(commonequip_record.getComponentByName("operatorId")))

        commonequip_record = [basic_service, call_duration, call_reference, equipment_id, equipment_type,
                              exchange_identity, file_id, fnur, hot_billing_tag2, last_long_part_ind,
                              milli_sec_duration, operator_id, rate_indication, record_sequence_number,
                              record_type, recording_entity, release_time, seizure_time, sequence_number,
                              served_imsi, served_msisdn, system_type, long(seizure_time.strftime("%Y%m%d")),
                              long(seizure_time.strftime("%H"))]

        return commonequip_record
