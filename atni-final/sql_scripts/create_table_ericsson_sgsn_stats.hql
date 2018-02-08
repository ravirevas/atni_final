create table gtt_new.ericsson_sgsn_stats (
    iteration_id string,
	file_id string,
	file_name string,
	checksum string,
	total_records bigint,
	success_count bigint,
	fail_count bigint,
	pdp_base_total_count bigint,
	pdp_base_success_count bigint,
	pdp_base_failed_count bigint,
	pdp_traffic_count bigint,
	pdp_camel_count bigint,
	pdp_rec_ext_count bigint,
	smo_base_total_count bigint,
	smo_base_success_count bigint,
	smo_base_failed_count bigint,
	smo_camel_count bigint,
	smt_base_total_count bigint,
	smt_base_success_count bigint,
	smt_base_failed_count bigint
)
partitioned by (timeframe_day bigint,timeframe_hr int)
stored as parquet
location '/tmp/atni/data/ericsson_sgsn_stats';
