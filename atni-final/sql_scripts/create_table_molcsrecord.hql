create external table commnet_new.molcsrecord (
	additionalchginfochargeindicator bigint,
	additionalchginfochargedparty string,
	causeforterm string,
	diagnostics string,
	eventtimestamp timestamp,
	filename string,
	hotbillingtag bigint,
	hotbillingtag2 bigint,
	lcscause string,
	lcsclientexternalidexternaladdress string,
	lcsclientidentitylcsclientdialedbyms string,
	lcsclientidentitylcsclientinternalid string,
	lcsclienttype string,
	lcspriority string,
	lcsqos string,
	locationcellid string,
	locationestimate string,
	locationlac string,
	locationplmn string,
	locationsac string,
	measureduration bigint,
	mlc_number string,
	molr_type string,
	operatorid bigint,
	positioningdata string,
	recordsequencenumber string,
	recordtype string,
	recordingentity string,
	servedimsi string,
	servedmsisdn string,
	systemtype string
)
stored as parquet
location '/tmp/atni/data/molcsrecord';
