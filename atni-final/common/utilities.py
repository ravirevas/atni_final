import binascii
import cStringIO
import csv
import gzip
import io
import os
import subprocess
import time
from datetime import datetime, timedelta

from pyspark import SparkConf, SparkContext, HiveContext

from common.constants import *


def get_spark_context(appName):
    conf = SparkConf().setAppName(appName)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("INFO")
    return sc


def get_current_time_day():
    return long(time.strftime("%Y%m%d"))



def get_current_time():
    return int(time.strftime("%H"))


def get_current_time_and_day():
    return long(time.strftime("%Y%m%d%H%M%S"))

def get_year():
    return long(time.strftime("%Y"))


def get_month():
    return long(time.strftime("%m"))


def get_day():
    return long(time.strftime("%d"))

def get_sql_context(sc):
    sqlContext = HiveContext(sc)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    return sqlContext


def parseTimestamp(str1):
    year = str1[:2]
    month = str1[2:4]
    day = str1[4:6]
    hh = str1[6:8]
    mm = str1[8:10]
    ss = str1[10:12]
    # sign = str1[12:14]
    # hhh = str1[14:16]
    # mmm = str1[16:18]

    date = "20" + year + "-" + month + "-" + day + " " + hh + ":" + mm + ":" + ss

    return datetime.strptime(date, "%Y-%m-%d %H:%M:%S")


def get_ch_ch_selection_mode(ch_ch_selection_mode_tmp):
    if ch_ch_selection_mode_tmp == 1:
        ch_ch_selection_mode = "subscriptionSpecific"
    elif ch_ch_selection_mode_tmp == 2:
        ch_ch_selection_mode = "aPNSpecific"
    elif ch_ch_selection_mode_tmp == 3:
        ch_ch_selection_mode = "homeDefault"
    elif ch_ch_selection_mode_tmp == 4:
        ch_ch_selection_mode = "roamingDefault"

    return ch_ch_selection_mode


def readFileGzip(src_file):
    return gzip.open(src_file)


def readFile(src_file):
    return io.open(src_file)


def readFileFast(src_file):
    io_method = cStringIO.StringIO
    p = subprocess.Popen(["gzcat", src_file], stdout=subprocess.PIPE)
    fh = io_method(p.communicate()[0])
    assert p.returncode == 0
    return fh


def write_to_csv(cdr_pdp_array, output):
    print "Final count is:" + str(len(cdr_pdp_array))

    print cdr_pdp_array
    # cdr_pdp_array.sort(key=operator.itemgetter())
    with open(output, "w") as o:
        pdp_writer = csv.writer(o, delimiter="|")
        pdp_writer.writerows(cdr_pdp_array)


def get_gzip_file_size(src_file):
    with gzip.open(src_file) as f:
        f.read()
        return f.tell()


def get_file_size(src_file):
    f = open(src_file)
    f.read()
    size = f.tell()
    f.close()
    return size


def get_file_paths_from_hfds(base_folder):
    ls_contents = subprocess.check_output(["hadoop", "fs", "-ls", base_folder])
    lines = ls_contents.split("\n")
    # We dont want the first line of the ls output as it gives total blocks
    lines = lines[1:]
    file_paths = []
    for line in lines:
        if line is not None and line != "":
            data = line.split(" ")
            file_path = data[len(data) - 1]
            file_paths.append(file_path)

    return file_paths


def get_file_check_sum(file_path):
    check_sum_cmd_op = subprocess.check_output(["hadoop", "fs", "-checksum", file_path])
    check_sum = check_sum_cmd_op.split("\t")[2]
    return check_sum.strip("\n")


# This will take the process stage(parse or ftp) and logging base path, data set and returns the log
# file name
def get_logging_file_name(process, base_path, dataset):
    current_date = datetime.now()
    current_datetime_formatted = current_date.strftime("%Y%m%d")
    log_filename = base_path + '/' + dataset + "_" + process + "_" + current_datetime_formatted + '.log'
    return log_filename


######## Airspan specific start ########

# Convert from "Sun May 22 08:12:18 2016" to 20160522
# Initialize Hive Context
def convert_calltime_to_part_date(str):
    oldDateObject = datetime.strptime(str, "%c")
    result_format = oldDateObject.strftime("%Y%m%d")
    return long(result_format)


def convert_calltime_to_part_hr(str):
    oldDateObject = datetime.strptime(str, "%c")
    result_format = oldDateObject.strftime("%H")
    return long(result_format)


def convert_calltime_to_timestamp(str):
    dateObject = datetime.strptime(str, "%c")
    return dateObject


def get_file_name(path):
    file_name = os.path.splitext(os.path.splitext(os.path.basename(path))[0])[0]
    return file_name


######## Airspan specific end ########

######## General ##########

def get_imei_and_imsi(record, attr):
    served_binascii = binascii.hexlify(str(record.getComponentByName(attr)))
    served_binascii_list = []
    served_data = list(served_binascii)
    for i in xrange(0, len(served_data) - 1, 2):
        served_binascii_list.append(served_data[i + 1] + "" + served_data[i])
    served_imei_concatenated = ''.join(served_binascii_list)
    return served_imei_concatenated.replace("f", "")


def get_plmn_id(s):
    plmnid = ""
    if s is not None:
        plmnid = get_mcc(s) + get_mnc(s)
    return plmnid


def get_mcc(s):
    mcc = ""
    if len(s) == 6:
        mcc = s[1] + s[0] + s[3]
    return mcc


def get_mnc(s):
    mnc = ""
    if len(s) == 6:
        mnc = s[5] + s[4] + s[2]
    return mnc


def insert_records(sqlContext, input_rdd, input_schema, table_name):
    input_df = sqlContext.createDataFrame(input_rdd, input_schema)
    input_df.write.format(WRITE_FORMAT).mode(WRITE_MODE).partitionBy("year", "month","day").saveAsTable(
        table_name)
    sqlContext.refreshTable(table_name)


def insert_records_without_timestamp(sqlContext, input_rdd, input_schema, table_name):
    input_df = sqlContext.createDataFrame(input_rdd, input_schema)
    input_df.write.format(WRITE_FORMAT).mode(WRITE_MODE).saveAsTable(table_name)
    sqlContext.refreshTable(table_name)


def insert_sub_records(sqlContext, input_rdd, input_schema, table_name):
    input_df = sqlContext.createDataFrame(input_rdd, input_schema)
    input_df.write.format(WRITE_FORMAT).mode(WRITE_MODE).saveAsTable(table_name)
    sqlContext.refreshTable(table_name)


def get_timezone_offset():
    curr = datetime.now()
    curr_utc = datetime.utcnow()

    offset = curr_utc - curr
    off_hour = offset.seconds / 3600

    return off_hour


def append_timezone_offset(datetimeobj):
    return datetimeobj - timedelta(hours=get_timezone_offset())


def boolean_value(b):
    if b is None:
        return "false"
    else:
        return "true"


def parse_start_time(starttime):
    year = starttime[1:5]
    month = starttime[5:7]
    day = starttime[7:9]
    hh = "00"
    mm = "00"
    ss = "00"

    date = year + "-" + month + "-" + day + " " + hh + ":" + mm + ":" + ss

    return datetime.strptime(date, "%Y-%m-%d %H:%M:%S")

