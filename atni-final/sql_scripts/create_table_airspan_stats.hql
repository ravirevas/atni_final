create external table gtt_new.airspan_stats (
    iteration_id string,
	file_id string,
	file_name string,
	checksum string,
	total_records bigint,
	success_count bigint,
	fail_count bigint
)
partitioned by (timeframe_day bigint,timeframe_hr int)
stored as parquet
location '/tmp/atni/data/airspan_stats';
