create external table commnet_new.vigrecord (
	calldatarate string,
	callduration bigint,
	callreference string,
	callednumber string,
	callingnumber string,
	causeforterm string,
	diagnostics string,
	endtime timestamp,
	exchangeidentity string,
	lastlongpartind string,
	millisecduration bigint,
	recordsequencenumber string,
	recordtype bigint,
	sequencenumber string,
	starttime timestamp,
	vigcalltype string
)
stored as parquet
location '/tmp/atni/data/vigrecord';
