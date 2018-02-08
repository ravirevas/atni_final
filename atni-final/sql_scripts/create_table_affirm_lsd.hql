CREATE TABLE `gtt.listofservicedata`(
  `file_id` string,
`listofservicedata_key` string,
`ratinggroup` string,
`chargingrulebasename` string,
`resultcode` string,
`localsequencenumber` string,																																
`timeoffirstusage` string,
`timeoflastusage` string,
`timeusage` string,
`serviceconditionchange` string,
`servingnodeaddress` string,
`datavolumefbcuplink` string,
`datavolumefbcdownlink` string,
`timeofreport` string,
`failurehandlingcontinue` string,
`serviceidentifier` int,
`psfurnishcharginginformation` string,
`userlocationinformation` string,
`file_created_timestamp` int)
partitioned by(year int, month int, day int) 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://quickstart.cloudera:8020/user/hive/warehouse/gtt.db/listofservicedata'
TBLPROPERTIES (
  'transient_lastDdlTime'='1516956105');
