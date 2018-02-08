class AffirmStats:

    def __init__(self, iteration_id, file_id, file_name, checksum,
                 total_count, success_count, fail_count, pgw_total_count,
                 pgw_success_count,pgw_failed_count,pgw_lsd_count,sgw_rec_ext_count,
                 sgw_total_count, sgw_success_count, sgw_failed_count, file_created_timestamp, year, month, day):

        self.iteration_id = iteration_id
        self.file_id = file_id
        self.file_name = file_name
        self.checksum = checksum
        self.total_count = total_count
        self.success_count = success_count
        self.fail_count = fail_count
        self.pgw_total_count = pgw_total_count
        self.pgw_success_count = pgw_success_count
        self.pgw_failed_count = pgw_failed_count
        self.pgw_lsd_count = pgw_lsd_count
        self.sgw_rec_ext_count=sgw_rec_ext_count
        self.sgw_total_count =sgw_total_count
        self.sgw_success_count = sgw_success_count
        self.sgw_failed_count =sgw_failed_count
        self.file_created_timestamp = file_created_timestamp
        self.year = year
        self.month = month
        self.day = day









