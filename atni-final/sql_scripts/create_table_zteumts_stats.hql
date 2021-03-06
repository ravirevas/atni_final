create external table commnet_new.zteumts_stats (
	     file_id string,
	     file_name string,
             iteration_id string,
             checksum string,
             total_records bigint,
             success_count bigint,
             fail_count bigint,
             commonequip_base_total_count bigint,
             commonequip_base_success_count bigint,
             commonequip_base_failed_count bigint,
             incgateway_base_total_count bigint,
             incgateway_base_success_count bigint,
             incgateway_base_failed_count bigint,
             mcfcall_base_total_count bigint,
             mcfcall_base_success_count bigint,
             mcfcall_base_failed_count bigint,
             mocall_base_total_count bigint,
             mocall_base_success_count bigint,
             mocall_base_failed_count bigint,
             mosms_base_total_count bigint,
             mosms_base_success_count bigint,
             mosms_base_failed_count bigint,
             mtcall_base_total_count bigint,
             mtcall_base_success_count bigint,
             mtcall_base_failed_count bigint,
             mtlcs_base_total_count bigint,
             mtlcs_base_success_count bigint,
             mtlcs_base_failed_count bigint,
             mtsms_base_total_count bigint,
             mtsms_base_success_count bigint,
             mtsms_base_failed_count bigint,
             outgateway_base_total_count bigint,
             outgateway_base_success_count bigint,
             outgateway_base_failed_count bigint,
             ssaction_base_total_count bigint,
             ssaction_base_success_count bigint,
             ssaction_base_failed_count bigint,
             ussd_base_total_count bigint,
             ussd_base_success_count bigint,
             ussd_base_failed_count bigint
)
partitioned by (timeframe_day bigint,timeframe_hr int)
stored as parquet
location '/tmp/atni/data/zteumts_stats';
