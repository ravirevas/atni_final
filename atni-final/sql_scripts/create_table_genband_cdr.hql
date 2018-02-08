
create external table gtt_new.genband_cdr (
	file_id string,
        file_name string,
	connect_datetime timestamp,
	originating_number string,
	terminating_number string,
	elapsed_time decimal(12,2),
	dom_int_indicator string,
	trunkid1 string,
	trunkid2 string,
	call_code string,
	completion_ind string,
	answer_ind string
)
partitioned by (timeframe_day bigint,timeframe_hr int)
stored as parquet
location '/tmp/atni/data/genband_cdr';
