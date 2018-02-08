create table gtt_new.trafficvolumes (
	file_name string,
	trafficVolumesKey string,
	changeCondition	smallint,
	changeTime	timestamp,
	dataVolumeGPRSDownlink	bigint,
	dataVolumeGPRSUplink	bigint,
	qosNegotiated	string,
	qosRequested	string
)
stored as parquet
location '/tmp/atni/data/trafficvolumes';
