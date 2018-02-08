create table gtt_new.camelinformationsms (
	file_name string,
	cAMELCallingPartyNumber	string,
	cAMELDestinationSubscriberNumber	string,
	cAMELSMSCAddress	string,
	defaultSMSHandling	smallint,
	freeFormatData	string,
	sCFAddress	string,
	serviceKey	bigint,
	smsReferenceNumber	string
)
stored as parquet
location '/tmp/atni/data/camelinformationsms';
