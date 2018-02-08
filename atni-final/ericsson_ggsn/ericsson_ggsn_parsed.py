class EricssonggsnParsedRecord:

    def __init__(self, file_name, iter_stats, ggsnrecord,ggsn_pdp_traffic_volume_array,ggsn_pdp_rec_ext,egsn_base,egsn_pdp_traffic_volume_array,egsn_pdp_service_data_array,egsn_pdp_rec_ext):

        self.file_name = file_name
        self.iter_stats = iter_stats
        self.ggsnrecord = ggsnrecord
        self.ggsn_pdp_traffic_volume_array = ggsn_pdp_traffic_volume_array
        self.ggsn_pdp_rec_ext = ggsn_pdp_rec_ext
        self.egsn_base = egsn_base
        self.egsn_pdp_traffic_volume_array = egsn_pdp_traffic_volume_array
        self.egsn_pdp_service_data_array = egsn_pdp_service_data_array
        self.egsn_pdp_rec_ext = egsn_pdp_rec_ext
