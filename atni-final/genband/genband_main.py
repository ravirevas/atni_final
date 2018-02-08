from __future__ import print_function
import sys
import uuid

from common.constants import *
from pyspark import SparkConf, SparkContext, HiveContext
from pyspark.sql.types import *

from common.utilities import *
from genband_parser import Parser


def jdefault(o):
    return o.__dict__

class ParserInit:

    id = 1234
    def __init__(self, _id):
        self.id = _id
    # TODO: Return very basic information stating how many good records and bad records (can use ETL to validate returned data)
    def parseFile(self, _hdfsFileName, _content):
        #file = "U160522130065AMA"
        _absoluteFileName = _hdfsFileName.rpartition(':')[2]
        file = _absoluteFileName.rpartition('/')[2]

        filePath = _absoluteFileName.rpartition('/')[0]
        dateTime = datetime(2000+int(file[1:3]), int(file[3:5]), int(file[5:7]), int(file[7:9]), int(file[9:11]), 0)
        fileInfo = { 'filename':file, 'dateTime':datetime(2000+int(file[1:3]), int(file[3:5]), int(file[5:7]), int(file[7:9]), int(file[9:11]), 0)}
        fileInfo['file_path'] = filePath
        fileInfo['id'] = self.id

        check_sum = get_file_check_sum(str(_hdfsFileName))
        #check_sum = 100000
        file_id = str(uuid.uuid4())


        src_file_name = fileInfo['filename']


        cdr_file = Parser('/%s/%s' %(fileInfo['file_path'],fileInfo['filename']), _content)
        threshold_datetime = datetime(fileInfo['dateTime'].year, fileInfo['dateTime'].month, fileInfo['dateTime'].day) + timedelta(days=178)
        # Continue
        records = []
        record_count = [0, 0]
        stats_total = [0, 0]
        for x in cdr_file:
            try:
                rec = {}
                # add file id
                #rec['file_id']=fileInfo['id']
                rec['file_name']=file
                # Calculate 'connect_date' and 'connect_time'
                cdr_month = int(x['Date'][1:3])
                cdr_day = int(x['Date'][3:5])
                cdr_hour = int(x['Connect Time'][0:2])
                cdr_min = int(x['Connect Time'][2:4])
                cdr_sec = int(x['Connect Time'][4:6])
                cdr_datetime = datetime(fileInfo['dateTime'].year, cdr_month, cdr_day, cdr_hour, cdr_min, cdr_sec)
                if cdr_datetime > threshold_datetime:
                    cdr_datetime = datetime(fileInfo['dateTime'].year - 1, cdr_month, cdr_day, cdr_hour, cdr_min, cdr_sec)
                rec['connect_datetime'] = str(cdr_datetime)
                rec['connect_date'] = str(cdr_datetime.date())
                rec['__cdr_date'] = rec['connect_date']   # Mandatory field!
                rec['connect_time'] = str(cdr_datetime.time())
                # Determine 'originating_number'
                orig_digits = int(x['Originating Significant Digits'])
                if orig_digits < 12: rec['originating_number'] = x['Originating Open Digits 1'][-orig_digits:]
                else: rec['originating_number'] = x['Originating Open Digits 1'] + x['Originating Open Digits 2'][11 - orig_digits:]
                # Determine 'terminating_number'
                term_digits = int(x['Terminating Significant Digits'])
                if term_digits < 16: rec['terminating_number'] = x['Terminating Open Digits 1'][-term_digits:]
                else: rec['terminating_number'] = x['Terminating Open Digits 1'] + x['Terminating Open Digits 2'][15 - term_digits:]
                # Calculate elapsed time correctly as decimal
                rec['elapsed_time'] = int(x['Elapsed Time'][1:6]) * 60 + float(x['Elapsed Time'][6:]) / 10
                # Get dom/int indicator
                rec['dom_int_indicator'] = x['Domestic/International Indicator']
                # Get first module (trunk ID)
                mod1 = cdr_file.next_module()
                if mod1: rec['trunk_id_1'] = mod1['Trunk Identification Number']
                else: rec['trunk_id_1'] = None
                # Get second module (trunk ID)
                mod2 = cdr_file.next_module()
                if mod2: rec['trunk_id_2'] = mod2['Trunk Identification Number']
                else: rec['trunk_id_2'] = None
                rec['call_code'] = x['Call Type Code']
                rec['completion_ind'] = x['Completion Indicator']
                rec['answer_ind'] = x['Answer Indicator']
                rec['file_id']=file_id
                records.append(rec)
                record_count[0] += 1
            except:
                #print traceback.print_exc()
                record_count[1] += 1

        stats={}
        stats['check_sum']= check_sum
        stats['file_id'] = file_id
        stats['file_name'] = file
        stats['success_cnt']=record_count[0]
        stats['failed_cnt']=record_count[1]

        return [stats, records]


# Initialize Hive Context

#input_path = GENBAND_IN_PATH
input_path=sys.argv[1]
conf = SparkConf().setAppName("Genband_Parsing")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)
sqlContext.setConf("hive.exec.dynamic.partition", "true")
sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

iteration_id = get_current_time()
start_time_for_iteration = time.time()

calls = sc.binaryFiles(input_path)

test = calls.map(lambda (x,y): x)
print(test.collect())


parser = ParserInit(333)
parsedFiles = calls.map(lambda (x,y): parser.parseFile(x,y))
stats = parsedFiles.map(lambda (x): x[0])
records = parsedFiles.flatMap(lambda (x): x[1])

final = records.map(lambda rec: (rec['file_id'], rec['file_name'], datetime.strptime(rec['connect_datetime'], "%Y-%m-%d %H:%M:%S"),rec['originating_number'],rec['terminating_number'],rec['elapsed_time'],rec['dom_int_indicator'],rec['trunk_id_1'],rec['trunk_id_2'],rec['call_code'],rec['completion_ind'],rec['answer_ind'],long(datetime.strptime(rec['connect_datetime'], "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d")), int(datetime.strptime(rec['connect_datetime'], "%Y-%m-%d %H:%M:%S").strftime("%H"))))
final_stats = stats.map(lambda rec: (iteration_id, rec['file_id'], rec['file_name'], rec['check_sum'], (rec['success_cnt']+rec['failed_cnt']), rec['success_cnt'], rec['failed_cnt'],  get_current_time_day(), get_current_time()))
#print(final.collect())

genband_schema = StructType([
    StructField("file_id", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("connect_datetime", TimestampType(), False),
                             StructField("originating_number", StringType(), False),
                             StructField("terminating_number", StringType(), False),
                             StructField("elapsed_time", FloatType(), False),
                             StructField("dom_int_indicator", StringType(), False),
                             StructField("trunk_id_1", StringType(), False),
                             StructField("trunk_id_2", StringType(), False),
                             StructField("call_code", StringType(), False),
                             StructField("completion_ind", StringType(), False),
                             StructField("answer_ind", StringType(), False),
                             StructField("timeframe_day", LongType(), False),
                             StructField("timeframe_hr", IntegerType(), False)])


genband_stats_schema = StructType([
    StructField("iteration_id", StringType(), False),
    StructField("file_id", StringType(), False),
    StructField("file_name", StringType(), False),
    StructField("check_sum", StringType(), False),
    StructField("total_cnt", StringType(), False),
    StructField("success_cnt", StringType(), False),
    StructField("failed_cnt", StringType(), False),
    StructField("timeframe_day", LongType(), False),
    StructField("timeframe_hr", IntegerType(), False)])


## Below code is for Hive storing
calls_df = sqlContext.createDataFrame(final, genband_schema)
#sqlContext.sql("drop table if exists gtt.GENBAND_CDR")
#sqlContext.sql("CREATE TABLE gtt.GENBAND_CDR (file_id string, file_name string, connect_datetime timestamp, originating_number string ,terminating_number string  ,"
#               "elapsed_time decimal(12,2), dom_int_indicator string, trunkid1 string  ,  trunkid2 string  , call_code string  ,"
#               "completion_ind string,  answer_ind string ) partitioned by (timeframe_day bigint,timeframe_hr int)")

stats_df = sqlContext.createDataFrame(final_stats, genband_stats_schema)

#sqlContext.sql("drop table if exists gtt.GENBAND_PARSER_STATS")
#sqlContext.sql("CREATE TABLE gtt.GENBAND_PARSER_STATS (iteration_id string, file_id string, file_name string, "
#               "check_sum string, total_cnt string, success_cnt string, failed_cnt string ) partitioned by (timeframe_day bigint,timeframe_hr int)")


print (calls_df.columns)
print (calls_df.count())

#calls_df.write.format("parquet").mode("append").saveAsTable("gtt.genband_cdr")
#sqlContext.refreshTable("default.genband_cdr")

calls_df.write.format(WRITE_FORMAT).mode(WRITE_MODE).partitionBy("timeframe_day","timeframe_hr").saveAsTable(GENBAND_TABLE_NAME)
sqlContext.refreshTable(GENBAND_TABLE_NAME)

stats_df.write.format(WRITE_FORMAT).mode(WRITE_MODE).partitionBy("timeframe_day","timeframe_hr").saveAsTable(GENBAND_STATS_TABLE_NAME)
sqlContext.refreshTable(GENBAND_STATS_TABLE_NAME)

