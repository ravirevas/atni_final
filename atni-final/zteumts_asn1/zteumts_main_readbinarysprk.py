import logging
import sys

from common.constants import *
from pyasn1 import error
from pyasn1.codec.ber import decoder
from pyspark import SparkConf, SparkContext

from common.utilities import  *
from zteumts_asn_gen import CallEventRecord


def clean_zteumts_block(data):

    # Your code goes here, just return the new binary code block without the FFFF
    cleaned_block = ""

    return cleaned_block

def process_zteumts_record_map(data):


    cdr = []

    while data != "":
        try:
            ab, data = decoder.decode(data, asn1Spec=CallEventRecord())

            cdr.append(ab)
        except error.SubstrateUnderrunError as e:
            # logging.exception(e.message)
            #print "SubstrateUnderrunError : we are stopping here"
            break
        except error.PyAsn1Error as p:
            logging.exception(p.message)
            print "PyAsn1Error : we are stopping again"
            break
        except Exception as ep:
            logging.exception(ep.message)
            break

    print cdr
    return cdr

def main(argv=None):
    if argv is None:
        pass

    file_name = os.path.split(ZTEUMTS_IN_PATH)[1]
    zteumts_file = readFileGzip(ZTEUMTS_IN_PATH)
    chunk_size = 2048
    bytes_to_read = chunk_size
    bytes_already_read = 0
    i = 0
    k = 0

    conf = SparkConf()
    sc = SparkContext(conf=conf)

    zteumts_rdd = sc.binaryRecords(ZTEUMTS_IN_PATH, 2048)

    print "rdd count: " + str(zteumts_rdd.count())

    sys.tracebacklimit = 0
    start_time_for_iteration = time.time()

    file_size = get_file_size(ZTEUMTS_IN_PATH)

    print "file size:" + str(file_size)

    #cleaned_rdd = zteumts_rdd.map(process_zteumts_record_map())
    parsed_records = zteumts_rdd.map(process_zteumts_record_map)
    print "parsed things: " + str(parsed_records.count())


    print "records in one partition:" + len(one_mapped_partition)


    print "Time to iterate entire contents:" + str(time.time() - start_time_for_iteration)

    print "Total record count is:" + str(i)
    print "Total chunks divided :" + str(k)

    zteumts_file.close()


if __name__ == "__main__":
    sys.exit(main())
