import binascii

from common.utilities import parseTimestamp


class ProcessZTEumtsVIGRecord:
    def __init__(self):
        pass

    @staticmethod
    def process_vig_records(vig_record, file_name):

        call_data_rate = str(vig_record.getComponent("callDataRate"))

        if vig_record.getComponentByName("callDuration") is None:
            call_duration = 0
        else:
            call_duration = long(str(vig_record.getComponentByName("callDuration")))

        called_number_binascii = binascii.hexlify(str(vig_record.getComponentByName("calledNumber")))
        called_number_list = []
        called_number_data = list(called_number_binascii)
        for i in xrange(0, len(called_number_data) - 1, 2):
            called_number_list.append(called_number_data[i + 1] + "" + called_number_data[i])
        called_number_concatenated = ''.join(called_number_list)
        called_number = called_number_concatenated[2:]

        calling_number_binascii = binascii.hexlify(
                str(vig_record.getComponentByName("callingNumber")))
        calling_number_list = []
        calling_number_data = list(calling_number_binascii)
        for i in xrange(0, len(calling_number_data) - 1, 2):
            calling_number_list.append(calling_number_data[i + 1] + "" + calling_number_data[i])
        calling_number_concatenated = ''.join(calling_number_list)
        calling_number = calling_number_concatenated[2:]

        call_reference = binascii.hexlify(str(vig_record.getComponentByName("callReference")))

        if vig_record.getComponentByName("causeForTerm") is None:
            cause_for_termination = ""
        else:
            cause_for_termination_tmp = long(str(vig_record.getComponentByName("causeForTerm")))
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

        if vig_record.getComponentByName("diagnostics") is None:
            diagnostics = ""
        else:
            diagnostics = str(vig_record.getComponentByName("diagnostics"))

        end_time_tmp = binascii.hexlify(str(vig_record.getComponentByName("endTime")))
        end_time = parseTimestamp(end_time_tmp)

        exchange_identity = str(vig_record.getComponentByName("exchangeIdentity"))

        last_long_part_ind = str(vig_record.getComponentByName("lastLongPartInd"))

        if vig_record.getComponentByName("millisecDuration") is None:
            milli_sec_duration = 0
        else:
            milli_sec_duration = long(str(vig_record.getComponentByName("millisecDuration")))

        record_sequence_number = binascii.hexlify(str(vig_record.getComponentByName("recordSequenceNumber")))

        if vig_record.getComponentByName("recordType") is None:
            record_type = 0
        else:
            record_type = long(str(vig_record.getComponentByName("recordType")))

        if vig_record.getComponentByName("sequenceNumber") is None:
            sequence_number = 0
        else:
            sequence_number = long(str(vig_record.getComponentByName("sequenceNumber")))

        start_time_tmp = binascii.hexlify(str(vig_record.getComponentByName("startTime")))
        start_time = parseTimestamp('1606151210002d0700')

        if vig_record.getComponentByName("vigCallType") is None:
            vig_call_type = ""
        else:
            vig_call_type_tmp = long(str(vig_record.getComponentByName("vigCallType")))
            vig_call_type = ""
            if vig_call_type_tmp == 0:
                vig_call_type = "normalRelease"
            elif vig_call_type_tmp == 1:
                vig_call_type = "partialRecord"

        vig_record = [call_data_rate, call_duration, called_number, calling_number, call_reference,
                      cause_for_termination, diagnostics, end_time, exchange_identity, last_long_part_ind,
                      milli_sec_duration, record_sequence_number, record_type, sequence_number, start_time,
                      vig_call_type]
        return vig_record

