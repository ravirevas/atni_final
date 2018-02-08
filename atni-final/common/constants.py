# base directory where the data set should be pulled from.

AIRSPAN_HDFS_SOURCE="/tmp/airspan_new/source"
GENBAND_HDFS_SOURCE="/tmp/genband_new/source"
ERICSSON_HDFS_SOURCE="/tmp/ericsson_new/source"
ZTEUMTS_HDFS_SOURCE=["/etl/commnet/zteumts/raw/atlantatx","/etl/commnet/zteumts/raw/castlerockco","/etl/commnet/zteumts/raw/comverse"]
# dest path
hdfs_path_airspan="/tmp/airspan_new/staging"
hdfs_path_genband="/tmp/genband_new/staging"
hdfs_path_ericsson="/tmp/ericsson_new/staging"
hdfs_path_zteumts="/tmp/zteumts_new/staging"
# DATASETS

LOG_BASE = "/opt/atni_repo/"
REPOSITORY_BASE = "/opt/atni_repo/"

AIRSPAN = "airspan"
GENBAND = "genband"
ERICSSON_SGSN = "ericsson_sgsn"
ZTEUMTS = "zteumts"

# HDFS Directories start

AIRSPAN_HDFS_BASE = "/etl/gtt/airspan/"
AIRSPAN_HDFS_STAGING = "/etl/gtt/airspan/staging/"
AIRSPAN_HDFS_COMPLETE = "/etl/gtt/airspan/complete"
AIRSPAN_HDFS_FAILED = "/etl/gtt/airspan/failed"
AIRSPAN_HDFS_DUPLICATE = "/etl/gtt/airspan/duplicate"

GENBAND_HDFS_BASE = "/etl/gtt/genband/"
GENBAND_HDFS_STAGING = "/etl/gtt/genband/staging/temp/"
GENBAND_HDFS_COMPLETE = "/etl/gtt/genband/complete"
GENBAND_HDFS_FAILED = "/etl/gtt/genband/failed"
GENBAND_HDFS_DUPLICATE = "/etl/gtt/genband/duplicate"


GENBAND_HDFS_HIST_BASE = "/etl/gtt/genband/hist"
GENBAND_HDFS_HIST_STAGING = "/etl/gtt/genband/hist/staging/temp/"
GENBAND_HDFS_HIST_COMPLETE = "/etl/gtt/genband/hist/complete"
GENBAND_HDFS_HIST_FAILED = "/etl/gtt/genband/hist/failed"


ERICSSON_SGSN_HDFS_BASE = "/etl/gtt/ericsson_sgsn/"
ERICSSON_SGSN_HDFS_STAGING = "/etl/gtt/ericsson_sgsn/staging/temp/"
ERICSSON_SGSN_HDFS_COMPLETE = "/etl/gtt/ericsson_sgsn/complete"
ERICSSON_SGSN_HDFS_FAILED = "/etl/gtt/ericsson_sgsn/failed"
ERICSSON_SGSN_HDFS_DUPLICATE = "/etl/gtt/ericsson_sgsn/duplicate"


ZTEUMTS_HDFS_BASE = "/etl/commnet/zteumts/"
ZTEUMTS_HDFS_STAGING = "/etl/commnet/zteumts/staging/temp/"
ZTEUMTS_HDFS_COMPLETE = "/etl/commnet/zteumts/complete"
ZTEUMTS_HDFS_FAILED = "/etl/commnet/zteumts/failed"
ZTEUMTS_HDFS_DUPLICATE = "/etl/commnet/zteumts/duplicate"


# HDFS Directories end

# Log Directories start

AIRSPAN_LOG = LOG_BASE + "ftp_process/logs/airspan/"
GENBAND_LOG = LOG_BASE + "ftp_process/logs/genband/"
ERICSSON_SGSN_LOG = LOG_BASE + "ftp_process/logs/ericsson_sgsn/"
ZTEUMTS_LOG = LOG_BASE + "ftp_process/logs/zteumts/"

# Log Directories end

# ######  SPARK CONSTANTS ##########


# ###################### AIRSPAN CONSTANTS ######################

#AIRSPAN_IN_PATH = "/Users/ram/Projects/ATNi/atni_repository/atni/data/airspan/in/accountP_22_5_16#12_12_15.dat.gz"
AIRSPAN_IN_PATH = AIRSPAN_HDFS_STAGING
AIRSPAN_OUT_PATH = ""
AIRSPAN_TABLE_NAME = "gtt_new.airspan_cdr"
AIRSPAN_STATS_TABLE_NAME = "gtt_new.airspan_stats"
AIRSPAN_CDR_OUTPUT_PATH="/tmp/atni/data/airspan_cdr"
AIRSPAN_STATS_OUTPUT_PATH="/tmp/atni/data/airspan_stats"

# ###################### AIRSPAN CONSTANTS ######################

# ###################### AFFIRM CONSTANTS ######################
AFFIRMED_STATS_TABLE_NAME = "gtt.affirm_stats"
AFFIRMED_STATS_OUT_PATH="/tmp/atni/data/affirm_stats"

AFFIRMED_LTE_CDR_PGW_TABLE_NAME = "gtt.pgwrecord"
AFFIRMED_PGW_OUT_PATH="/tmp/atni/data/pgwrecord"

AFFIRMED_LTE_CDR_SGW_TABLE_NAME = "gtt.sgwrecord"
AFFIRMED_SGW_OUT_PATH="/tmp/atni/data/sgwrecord"

AFFIRMED_LTE_CDR_LSD_TABLE_NAME = "gtt.listofservicedata"
AFFIRMED_LSD_OUT_PATH="/tmp/atni/data/listofservicedata"

AFFIRMED_FILE_CHUNK_SIZE = 10000
#########################AFFIRM_IN_PATH###############################
AFFIRM_IN_PATH="/home/cloudera/Desktop/atni/data/affirm/in/"
# ###################### AFFIRM CONSTANTS ######################


#AAA_ALU_IN_PATH = "/Users/ram/Projects/ATNi/atni_repository/atni/data/airspan/in/accountP_22_5_16#12_12_15.dat.gz"
AAA_ALU_IN_PATH = ""
AAA_ALU_OUT_PATH = ""
AAA_ALU_TABLE_NAME = "gtt_new.aaa_cdr"
AAA_ALU_STATS_TABLE_NAME = "gtt_new.aaa_stats"
AAA_ALU_CDR_OUTPUT_PATH="/tmp/atni/data/aaa_cdr"
AAA_ALU_STATS_OUTPUT_PATH="/tmp/atni/data/aaa_stats"

# ###################### GENBAND CONSTANTS ######################

GENBAND_IN_PATH = GENBAND_HDFS_STAGING
GENBAND_OUT_PATH = ""
GENBAND_TABLE_NAME = "gtt_new.genband_cdr"
GENBAND_STATS_TABLE_NAME = "gtt_new.genband_parser_stats"
GENBAND_CDR_OUTPUT_PATH="/tmp/atni/data/genband_cdr"
GENBAND_STATS_OUTPUT_PATH="/tmp/atni/data/genband_parser_stats"

# ###################### ERICSSON CONSTANTS ######################

#ERICSSON_IN_PATH = "/Users/ram/Projects/ATNi/atni_repository/atni/data/ericsson/in/2016182chsLog_261_00_20160701_0018.gz"
ERICSSON_IN_PATH = ERICSSON_SGSN_HDFS_STAGING
ERICSSON_OUT_PATH = ""
ERICSSON_FILE_CHUNK_SIZE = 10000

ERICSSON_PDP_TABLE_NAME = "gtt_new.sgsnpdprecord"
ERICSSON_PDP_OUT_PATH="/tmp/atni/data/sgsnpdprecord"

ERICSSON_PDP_TRAFFIC_VOLUME_TABLE = "gtt_new.trafficvolumes"
ERICSSON_PDP_TRAFFIC_VOLUME_OUT_PATH="/tmp/atni/data/trafficvolumes"


ERICSSON_PDP_CAMEL_INFO_PDP_TABLE = "gtt_new.camelinformationpdp"
ERICSSON_PDP_CAMEL_INFO_PDP_OUT_PATH="/tmp/atni/data/camelinformationpdp"


ERICSSON_PDP_RECORD_EXTENSIONS_TABLE = "gtt_new.recordextensions"
ERICSSON_PDP_RECORD_EXTENSIONS_OUT_PATH = "/tmp/atni/data/recordextensions"


ERICSSON_SMO_TABLE_NAME = "gtt_new.sgsnsmorecord"
ERICSSON_SMO_OUT_PATH = "/tmp/atni/data/sgsnsmorecord"

ERICSSON_SMO_CAMEL_SMS_TABLE = "gtt_new.camelinformationsms"
ERICSSON_SMO_CAMEL_SMS_OUT_PATH = "/tmp/atni/data/camelinformationsms"

ERICSSON_SMT_TABLE_NAME = "gtt_new.sgsnmtrecord"
ERICSSON_SMT_OUT_PATH="/tmp/atni/data/sgsnmtrecord"

ERICSSON_STATS_TABLE_NAME = "gtt_new.ericsson_sgsn_stats"
ERICSSON_STATS_OUT_PATH = "/tmp/atni/data/ericsson_sgsn_stats"

####################### ZTEUMTS-ASN1 CONSTANTS ######################

#ZTEUMTS_IN_PATH = "/tmp/parsers/atni/parsers/zteumts_asn1/B2016061536247.dat.gz"
ZTEUMTS_IN_PATH = ZTEUMTS_HDFS_STAGING
ZTEUMTS_OUT_PATH = ""
# The chunk size is fixed and cannot be varied. This is as per the structure of umts record
ZTEUMTS_FILE_CHUNK_SIZE = 2048
ZTEUMTS_OUTGATEWAY_TABLE_NAME = "commnet_new.outgatewayrecord"
ZTEUMTS_OUTGATEWAY_OUT_PATH="/tmp/atni/data/outgatewayrecord"

ZTEUMTS_COMMONEQUIP_TABLE_NAME = "commnet_new.commonequipmentrecord"
ZTEUMTS_COMMONEQUIP_OUT_PATH = "/tmp/atni/data/commonequipmentrecord"


ZTEUMTS_MOCALL_TABLE_NAME = "commnet_new.mocallrecord"
ZTEUMTS_MOCALL_OUT_PATH = "/tmp/atni/data/mocallrecord"


ZTEUMTS_USSD_TABLE_NAME = "commnet_new.ussdrecord"
ZEUMTS_USSD_OUT_PATH = "/tmp/atni/data/ussdrecord"


ZTEUMTS_MTLCS_TABLE_NAME = "commnet_new.mtlcsrecord"
ZTEUMTS_MTLCS_OUT_PATH = "/tmp/atni/data/mtlcsrecord"


ZTEUMTS_INCGATEWAY_TABLE_NAME = "commnet_new.incgatewayrecord"
ZTEUMTS_INCGATEWAY_OUT_PATH = "/tmp/atni/data/incgatewayrecord"

ZTEUMTS_SSACTION_TABLE_NAME = "commnet_new.ssactionrecord"
ZTEUMTS_SSACTION_OUT_PATH = "/tmp/atni/data/ssactionrecord"

ZTEUMTS_MOSMS_TABLE_NAME = "commnet_new.mosmsrecord"
ZTEUMTS_MOSMS_OUT_PATH = "/tmp/atni/data/mosmsrecord"


ZTEUMTS_MTSMS_TABLE_NAME = "commnet_new.mtsmsrecord"
ZTEUMTS_MTSMS_OUT_PATH = "/tmp/atni/data/mtsmsrecord"


ZTEUMTS_MTCALL_TABLE_NAME = "commnet_new.mtcallrecord"
ZTEUMTS_MTCALL_OUT_PATH = "/tmp/atni/data/mtcallrecord"


ZTEUMTS_MCFCALL_TABLE_NAME = "commnet_new.mcfcallrecord"
ZTEUMTS_MCFCALL_OUT_PATH = "/tmp/atni/data/mcfcallrecord"


ZTEUMTS_GSICPH_TABLE_NAME = "commnet_new.gsicphrecord"
ZTEUMTS_GSICPH_OUT_PATH = "/tmp/atni/data/gsicphrecord"


ZTEUMTS_HLRINT_TABLE_NAME = "commnet_new.hlrintrecord"
ZTEUMTS_HLRINT_OUT_PATH = "/tmp/atni/data/hlrintrecord"


ZTEUMTS_MOLCS_TABLE_NAME = "commnet_new.molcsrecord"
ZTEUMTS_MOLCS_OUT_PATH = "/tmp/atni/data/molcsrecord"


ZTEUMTS_MTRF_TABLE_NAME = "commnet_new.mtrfrecord"
ZTEUMTS_MTRF_OUT_PATH = "/tmp/atni/data/mtrfrecord"


ZTEUMTS_NCSCPH_TABLE_NAME = "commnet_new.ncscphrecord"
ZTEUMTS_NCSCPH_OUT_PATH = "/tmp/atni/data/ncscphrecord"

ZTEUMTS_NILCS_TABLE_NAME = "commnet_new.nilcsrecord"
ZTEUMTS_NILCS_OUT_PATH = "/tmp/atni/data/nilcsrecord"

ZTEUMTS_VIG_TABLE_NAME = "commnet_new.vigrecord"
ZTEUMTS_VIG_OUT_PATH = "/tmp/atni/data/vigrecord"

ZTEUMTS_MCFCPH_TABLE_NAME = "commnet_new.mcfcphrecord"
ZTEUMTS_MCFCPH_OUT_PATH = "/tmp/atni/data/mcfcphrecord"

ZTEUMTS_TCICPH_TABLE_NAME = "commnet_new.tcicphrecord"
ZTEUMTS_TCICPH_OUT_PATH = "/tmp/atni/data/tcicphrecord"


ZTEUMTS_MSCSRVCC_TABLE_NAME = "commnet_new.mscsrvccrecord"
ZTEUMTS_MSCSRVCC_OUT_PATH = "/tmp/atni/data/mscsrvccrecord"

ZTEUMTS_ROAM_TABLE_NAME = "commnet_new.roamrecord"
ZTEUMTS_ROAM_OUT_PATH = "/tmp/atni/data/roamrecord"


ZTEUMTS_MOCPH_TABLE_NAME = "commnet_new.mocphrecord"
ZTEUMTS_MOCPH_OUT_PATH="/tmp/atni/data/mocphrecord"

ZTEUMTS_MOECALL_TABLE_NAME = "commnet_new.moecallrecord"
ZTEUMTS_MOECALL_OUT_PATH="/tmp/atni/data/moecallrecord "

ZTEUMTS_TERMCAMELINT_TABLE_NAME = "commnet_new.termcamelintrecord"
ZTEUMTS_TERMCAMELINT_OUT_PATH="/tmp/atni/data/termcamelintrecord"

ZTEUMTS_STATS_TABLE_NAME = "commnet_new.zteumts_stats"
ZTEUMTS_STATS_OUT_PATH ="/tmp/atni/data/zteumts_stats"
# ###################### Common configuration ######################

WRITE_FORMAT = "parquet"
WRITE_MODE = "append"

IMPALA_DAEMON = "172.19.100.103"
IMPALA_PORT = "21050"


