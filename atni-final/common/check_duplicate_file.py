from impala.dbapi import connect
from impala.interface import _bind_parameters
from constants import *
import subprocess
import sys


def get_file_paths_from_hfds(base_folder):
    print "Fetching files from staging directory"
    ls_contents = subprocess.check_output(["hadoop", "fs", "-ls", base_folder])
    lines = ls_contents.split("\n")
    # We don't want the first line of the ls output as it gives total blocks
    lines = lines[1:]
    file_paths = []
    for line in lines:
        if line is not None and line != "":
            data = line.split(" ")
            file_path = data[len(data) - 1]
            file_paths.append(file_path.split("/")[6])
    print "All files:" + str(file_paths)
    return file_paths

dataset_stats_dict = {

    AIRSPAN: (AIRSPAN_STATS_TABLE_NAME,AIRSPAN_HDFS_STAGING + "*/",AIRSPAN_HDFS_DUPLICATE),
    GENBAND: (GENBAND_STATS_TABLE_NAME,GENBAND_HDFS_STAGING,GENBAND_HDFS_DUPLICATE),
    ERICSSON_SGSN: (ERICSSON_STATS_TABLE_NAME,ERICSSON_SGSN_HDFS_STAGING,ERICSSON_SGSN_HDFS_DUPLICATE),
    ZTEUMTS: (ZTEUMTS_STATS_TABLE_NAME,ZTEUMTS_HDFS_STAGING,ZTEUMTS_HDFS_DUPLICATE)

}

def main(argv=None):

    try:
        switch_type = sys.argv[1]

        table_name, staging_folder, duplicate_folder = dataset_stats_dict[switch_type]

        print "Table Name: " + table_name
        print "Staging Folder: " + staging_folder
        print "Duplicate Folder: " + duplicate_folder

        # Get all the file names for the respective parser
        file_names = get_file_paths_from_hfds(staging_folder)

        print "Total number of Files: " + str(len(file_names))

        conn = connect(host=IMPALA_DAEMON, port=IMPALA_PORT)
        cursor = conn.cursor()

        # Perform invalidate metadata to keep the table updated, before verifying the duplicate
        cursor.execute("INVALIDATE METADATA " + table_name)
        conn.commit()

        # Iterate each file and check if it is already present in stats table. If yes, then move it to duplicate folder.
        for file_name in file_names:

            file_name_no_ext = file_name
            if switch_type == 'airspan':
                file_name_no_ext = file_name.split(".")[0]
            print "Checking:" + file_name
            query = "select count(*) from " + table_name + " where file_name = '" + file_name_no_ext + "'"
            print "Query to verify duplicate: " + query

            cursor.execute(query)
            results = cursor.fetchall()
            print "Count from DB is:" + str(results[0][0])
            if results[0][0] != 0:
                # Move file from staging to duplicate folder
                src_file = staging_folder + file_name
                print "Moving from:" + src_file + " to " + duplicate_folder

                ls_contents = subprocess.check_output(["hadoop", "fs", "-mv", src_file, duplicate_folder])

    except Exception as ep:
        print "$$$$$$$$$$ Error while verifying duplicate file $$$$$$$$$$"
        print ep.message

if __name__ == "__main__":
    sys.exit(main())

