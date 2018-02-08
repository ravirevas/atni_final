from pyspark.sql.types import StructField, StructType, StringType, IntegerType, LongType, TimestampType, ShortType


def get_airspan_schema():

    airspan_schema = StructType([StructField("file_id", StringType(), False),
                             StructField("group_id", LongType(), False),
                             StructField("call_start_datetime", TimestampType(), False),
                             StructField("called_party_number", StringType(), False),
                             StructField("actual_called_party_number", StringType(), False),
                             StructField("duration", IntegerType(), False),
                             StructField("setup_duration", IntegerType(), False),
                             StructField("home_gatekeeper", StringType(), False),
                             StructField("disconnect_source", IntegerType(), False),
                             StructField("disconnect_reason", IntegerType(), False),
                             StructField("send_gateway_ip", StringType(), False),
                             StructField("calling_party_number", StringType(), False),
                             StructField("originator_gw_ip", StringType(), False),
                             StructField("codec_type", IntegerType(), False),
                             StructField("called_party_number_np", StringType(), False),
                             StructField("call_type", IntegerType(), False),
                             StructField("timeframe_day", LongType(), False),
                             StructField("timeframe_hr", IntegerType(), False)])
    return airspan_schema

def get_airspan_stats_schema():

    # Custom schema to read input
    airspan_stats_schema = StructType([
        StructField("iteration_id", StringType(), False),
        StructField("file_id", StringType(), False),
        StructField("file_name", StringType(), False),
        StructField("checksum", StringType(), False),
        StructField("total_records", LongType(), False),
        StructField("success_count", LongType(), False),
        StructField("fail_count", LongType(), False),
        StructField("timeframe_day", LongType(), False),
        StructField("timeframe_hr", IntegerType(), False)
    ])
    return airspan_stats_schema
