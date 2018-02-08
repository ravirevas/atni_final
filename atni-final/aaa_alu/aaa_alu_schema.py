from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType, TimestampType, ShortType
   
def get_aaa_schema():
  
    aaa_schema = StructType([StructField("file_name",StringType(), False),
                             StructField("eventtimestamp",StringType(), False),
                             StructField("username",StringType(), False),
                             StructField("pdsnaddress",StringType(), False),
                             StructField("acctstatustype",StringType(), False),
                             StructField("acctsessionid",StringType(), False),
                             StructField("acctsessiontime",StringType(), False),
                             StructField("acctauthentic",StringType(), False),
                             StructField("nasport",StringType(), False),
                             StructField("msid",StringType(), False),
                             StructField("framedprotocol",StringType(), False),
                             StructField("framedipaddress",StringType(), False),
                             StructField("acctinputoctets",StringType(), False),
                             StructField("acctoutputoctets",StringType(), False),
                             StructField("correlationid",StringType(), False),
                             StructField("userid",StringType(), False),
                             StructField("forwardmux",StringType(), False),
                             StructField("reversemux",StringType(), False),
                             StructField("serviceoption",StringType(), False),
                             StructField("forwardtraffictype",StringType(), False),
                             StructField("reversetraffictype",StringType(), False),
                             StructField("fundamentalframesize",StringType(), False),
                             StructField("forwardfundamentalrc",StringType(), False),
                             StructField("reverseFundamentalrc",StringType(), False),
                             StructField("iptechnology",StringType(), False),
                             StructField("comptunnelind",StringType(), False),
                             StructField("releasereason",StringType(), False),
                             StructField("pcfipaddress",StringType(), False),
                             StructField("homeagentipaddress",StringType(), False),
                             StructField("badpppframecount",StringType(), False),
                             StructField("numactive",StringType(), False),
                             StructField("sdbinputoctets",StringType(), False),
                             StructField("sdboutputoctets",StringType(), False),
                             StructField("sdbinputtransactions",StringType(), False),
                             StructField("sdboutputtransactions",StringType(), False), 
                             StructField("iPQoS",StringType(), False), 
                             StructField("SessionContinue",StringType(), False),
                             StructField("mIPSignalInputOctets",StringType(), False),
                             StructField("mIPSignalOutputOctets",StringType(), False),
                             StructField("AirlinkQOS",StringType(), False),
                             StructField("hDLCReceivedOctets",StringType(), False),
                             StructField("rPSessionId",StringType(), False),
                             StructField("MobileTermOrigInd",StringType(), False),
                             StructField("LastUserActivityTime",StringType(), False),
                             StructField("ForwardDCCHMuxOption",StringType(), False),
                             StructField("ReverseDCCHMuxOption",StringType(), False), 
                             StructField("ForwardDCCHRC",StringType(), False), 
                             StructField("ReverseDCCHRC",StringType(), False),
                             StructField("ServiceReferenceID",StringType(), False),
                             StructField("dCCHFrameSize",StringType(), False),
                             StructField("Esn",StringType(), False),
                             StructField("Meid",StringType(), False),
                             StructField("ActiveTime",StringType(), False),
                             StructField("Bsid",StringType(), False),
                             StructField("3ppAcctSessionTime",StringType(), False),
                             StructField("RecordType",StringType(), False),
                             StructField("timeframe_day", LongType(), False),
                             StructField("timeframe_hr", IntegerType(), False)])
                            
                                                          

    return aaa_schema

def get_aaa_stats_schema():

     aaa_stats_schema = StructType([
        StructField("iteration_id", StringType(), False),
        StructField("file_id", StringType(), False),
        StructField("file_name", StringType(), False),
        StructField("checksum", StringType(), False),
        StructField("total_records", LongType(), False),
        StructField("success_count", LongType(), False),
        StructField("fail_count", LongType(), False),
        StructField("timeframe_day", LongType(), False),
        StructField("timeframe_hr", IntegerType(), False)])

     return aaa_stats_schema
     

                              

