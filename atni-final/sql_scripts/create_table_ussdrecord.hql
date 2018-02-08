create external table commnet_new.ussdrecord (
	callreference string,
	endtime timestamp,
	errorcode string,
	exchangeidentity string,
	filename string,
	hotbillingtag bigint,
	hotbillingtag2 bigint,
	locationcellid string,
	locationlac string,
	locationplmn string,
	locationsac string,
	msclassmark string,
	operatorid bigint,
	recordsequencenumber string,
	recordtype bigint,
	recordingentity string,
	servedimei string,
	servedimsi string,
	servedmsisdn string,
	starttime timestamp,
	systemtype string,
	ussddatacodingscheme string,
	ussdinteractioncount bigint,
	ussdoperationcode string,
	ussdservicecode bigint,
	ussdunstructureddata string
)
partitioned by (timeframe_day bigint,timeframe_hr int)
stored as parquet
location '/tmp/atni/data/ussdrecord';
