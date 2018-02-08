
create table dms300_cdr_clli_mappings (
	clli int,
	clli_group string,
	carrier_code string
)
stored as parquet;