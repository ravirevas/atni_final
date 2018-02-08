drop table if exists airspan_cdr_ip_mappings_src;

create external table airspan_cdr_ip_mappings_src (
   carrier string,
   ip string
)
row format delimited 
fields terminated by ',';

--load data inpath will move file from source directory
load data inpath '/tmp/static_tables/IP_MAPPINGS.csv' into table airspan_cdr_ip_mappings_src;

compute stats airspan_cdr_ip_mappings_src;

drop table if exists airspan_cdr_ip_mappings;

create table airspan_cdr_ip_mappings stored as parquet as
select
    *
from
    airspan_cdr_ip_mappings_src;

compute stats airspan_cdr_ip_mappings;

drop table if exists airspan_cdr_ip_mappings_src;