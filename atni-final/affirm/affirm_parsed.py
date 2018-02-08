class AffirmParsedRecord:

    def __init__(self, file_name, iter_stats,pgwrecord,pgw_list_of_service_data, sgw_record,sgw_rec_ext):

        self.file_name = file_name
        self.iter_stats = iter_stats
        self.pgw_record = pgwrecord
        self.pgw_list_of_service_data = pgw_list_of_service_data
        self.sgw_record = sgw_record
        self.sgw_rec_ext = sgw_rec_ext