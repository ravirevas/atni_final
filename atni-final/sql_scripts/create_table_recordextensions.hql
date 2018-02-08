create external table gtt_new.recordextensions (
	file_id string,
	record_extensions_key string,
	identifier string,
	ts48018BssgpCause string,
	ts25413RanapCause string,
	significance string
)
stored as parquet
location '/tmp/atni/data/recordextensions';
