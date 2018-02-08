create external table commnet_new.commonequipmentrecord (
	basicservice string,
	callduration bigint,
	callreference string,
	equipmentid bigint,
	equipmenttype string,
	exchangeidentity string,
	filename string,
	fnur string,
	hotbillingtag2 bigint,
	lastlongpartind string,
	millisecduration bigint,
	operatorid bigint,
	rateindication string,
	recordsequencenumber string,
	recordtype bigint,
	recordingentity string,
	releasetime timestamp,
	seizuretime timestamp,
	sequencenumber bigint,
	servedimsi string,
	servedmsisdn string,
	systemtype string
)
partitioned by (timeframe_day bigint,timeframe_hr int)
stored as parquet
location '/tmp/atni/data/commonequipmentrecord';
