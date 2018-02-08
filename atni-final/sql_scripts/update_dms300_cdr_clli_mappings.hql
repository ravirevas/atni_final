drop table if exists dms300_cdr_clli_mappings_src;

create external table dms300_cdr_clli_mappings_src (
   clli int,
   clli_group string,
   carrier_code string
)
row format delimited 
fields terminated by ',';

--load data inpath will move file from source directory
load data inpath '/tmp/static_tables/CLLI_MAPPINGS.csv' into table dms300_cdr_clli_mappings_src;

compute stats dms300_cdr_clli_mappings_src;

drop table if exists dms300_cdr_clli_mappings;

create table dms300_cdr_clli_mappings stored as parquet as
select
    *
from
    dms300_cdr_clli_mappings_src;

compute stats dms300_cdr_clli_mappings;

drop table if exists dms300_cdr_clli_mappings_src;