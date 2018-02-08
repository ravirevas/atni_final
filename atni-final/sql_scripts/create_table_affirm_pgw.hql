CREATE EXTERNAL TABLE `pgwrecord`(
 `file_id` string,
`recordtype` int,
`servedimsi` string,
`pgwaddress` string,
`chargingid` bigint,
`accesspointnameni` string,
`servingnodeaddress` string,
`pdptype` string,
`servedpdpddnaddress` string,
`dynamicflagaddress` string,
`recordopeningtime` string,
`duration` bigint,
`causeforrecclosing` string,
`diagonstices` string,
`recordsequencenumber` bigint,
`nodeid` string,
`localsequencenumber` int,
`apnselectionmode` string,
`chargingcharacteristics` string,
`chchselectionmode` string,
`servingnodeplmnidentifier` string,
`psfurnishcharginginformation` string,
`servedimei` string,
`mstimezone` string,
`userlocationinformation` string,
`listofservicedata_key` string,
`servingnodetype` string,
`subscriptionIDType` string,
`subscriptionIDData` string,
`pgwplmnidentifier` string,
`servedmsisdn` string,
`starttime` string,
`stoptime` string,
`pdnconnectionid` int,
`servedpdppdnaddressext` string,
`rattype` bigint,
`file_created_timestamp` int)
partitioned by (year int, month int, day int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://quickstart.cloudera:8020/user/hive/warehouse/gtt.db/pgwrecord'
TBLPROPERTIES (
  'transient_lastDdlTime'='1516344049');
