create external table commnet_new.mtrfrecord (
	answertime timestamp,
	basicservice string,
	callduration bigint,
	callreference string,
	callingnumber string,
	causeforterm string,
	diagnostics string,
	exchangeidentity string,
	filename string,
	lastlongpartind string,
	mscincomingtkgp string,
	mscoutgoingtkgp string,
	partialrecordtype string,
	recordsequencenumber string,
	recordtype bigint,
	recordingentity string,
	releasetime timestamp,
	roamingnumber string,
	seizuretime timestamp,
	sequencenumber string,
	servedimei string,
	servedimsi string,
	servedmsisdn string
)
stored as parquet
location '/tmp/atni/data/mtrfrecord';


