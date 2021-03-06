create table msterminatingsmsinmsc (
CalledIMEI string,
CalledIMSI string,
CalledNumber string,
CalledTONNPI string,
CallingNumber string,
CallingTONNPI string,
CallReference string,
ChargedParty bigint,
DateForStartofCharge timestamp,
DirectSmsFlag bigint,
ExchangeIdentity string,
FirstCellIdentity string,
FirstLac string,
FirstMCC string,
FirstMNC string,
HotBillFlag bigint,
Id bigint,
IncomingCic string,
LastPartialOutput bigint,
MSCIdentification string,
MSCIdenTONNPI string,
Msclassmark string,
MscSpc14 string,
MscSpc24 string,
OutgoingCic string,
PartialOutputRecNum bigint,
RecordExtensions string,
RecordOffset bigint,
RecordType bigint,
SequenceNumber bigint,
ShortMessageFailed bigint,
ShortMessageFailedResult string,
ShortMessageServiceCentre string,
ShortMessageServiceTONNPI string,
SubscriberCategory string,
TimeForStartofCharge timestamp,
TransactionIdentification string
)
stored as parquet;