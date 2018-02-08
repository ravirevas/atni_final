create external table gtt_new.camelinformationpdp (
    file_name string,
    cAMELInformationPDPKey string,
	cAMELAccessPointNameNI	string,
	cAMELAccessPointNameOI	string,
	defaultTransactionHandling	smallint,
	fFDAppendIndicator	smallint,
	freeFormatData	string,
	levelOfCAMELService	bigint,
	numberOfDPEncountered	bigint,
	sCFAddress	string,
	serviceKey	bigint
)
stored as parquet
location '/tmp/atni/data/camelinformationpdp';
