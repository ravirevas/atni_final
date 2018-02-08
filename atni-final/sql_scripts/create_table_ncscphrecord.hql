create external table commnet_new.ncscphrecord (
	additionalchginfochargeindicator string,
	additionalchginfochargedparty string,
	basicservice string,
	callreference string,
	callsegmentid string,
	callednumber string,
	causeforterm string,
	defaultcallhandling string,
	diagnostics string,
	filename string,
	freeformatdataappend string,
	globalcallreference string,
	gsm_scfaddress string,
	iscamelcall string,
	lastlongpartind string,
	levelofcamelservice string,
	mscaddress string,
	networkcallreference string,
	numberofdpencountered bigint,
	partsequencenumber string,
	partialrecordtype string,
	recordsequencenumber string,
	recordtype bigint,
	recordingentity string,
	servedimei string,
	servedimsi string,
	servedmsisdn string,
	servicekey bigint
)
stored as parquet
location '/tmp/atni/data/ncscphrecord';


