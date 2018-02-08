create external table gtt_new.airspan_cdr (
	file_id string,
	group_id bigint,
	call_start_datetime timestamp,
	called_party_number string,
	actual_called_party_number string,
	duration int,
	setup_duration smallint,
	home_gatekeeper string,
	disconnect_source smallint,
	disconnect_reason smallint,
	send_gateway_ip string,
	calling_party_number string,
	originator_gw_ip string,
	codec_type smallint,
	called_party_number_np string,
	call_type smallint
)
partitioned by (timeframe_day bigint,timeframe_hr int)
stored as parquet
location '/tmp/atni/data/airspan_cdr';
