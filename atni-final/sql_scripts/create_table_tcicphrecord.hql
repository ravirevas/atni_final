create external table commnet_new.tcicphrecord (
	answertime timestamp,
	callduration bigint,
	callreference string,
	callsegmentid string,
	callednumber string,
	callingnumber string,
	causeforterm string,
	defaultcallhandling string,
	diagnostics string,
	filename string,
	freeformatdata string,
	freeformatdataappend string,
	globalcallreference string,
	gsm_scfaddress string,
	interrogationtime timestamp,
	lastlongpartind string,
	levelofcamelservice string,
	mscaddress string,
	millisecduration bigint,
	mscincomingtkgp string,
	networkcallreference string,
	numberofdpencountered bigint,
	partsequencenumber string,
	partialrecordtype string,
	recordsequencenumber string,
	recordtype bigint,
	recordingentity string,
	releasetime timestamp,
	seizuretime timestamp,
	servedimsi string,
	servedmsisdn string,
	servicekey bigint,
	termcalltype string
)
stored as parquet
location '/tmp/atni/data/tcicphrecord';
