class EricssonStats:

    def __init__(self,iteration_id,file_id,file_name,checksum,
                 total_records,success_count,fail_count,
                 ggsn_total_count,ggsn_success_count,ggsn_failed_count,ggsn_pdp_traffic_count,ggsn_pdp_rec_ext_count,
                 egsn_base_total_count,egsn_base_success_count,egsn_base_failed_count,egsn_pdp_traffic_count,egsn_pdp_servicedata_count,egsn_pdp_rec_ext_count
                 ):
        self.iteration_id = iteration_id
        self.file_id = file_id
        self.file_name = file_name
        self.checksum = checksum
        self.total_records = total_records
        self.success_count = success_count
        self.fail_count = fail_count
        self.ggsnrecord_total_count = ggsn_total_count
        self.ggsnrecord_success_count = ggsn_success_count
        self.ggsnrecord_failed_count = ggsn_failed_count
        self.ggsn_pdp_traffic_count = ggsn_pdp_traffic_count
        self.ggsn_pdp_rec_ext_count = ggsn_pdp_rec_ext_count
        self.egsn_base_total_count = egsn_base_total_count
        self.egsn_base_success_count = egsn_base_success_count
        self.egsn_base_failed_count = egsn_base_failed_count
        self.egsn_pdp_traffic_count = egsn_pdp_traffic_count
        self.egsn_pdp_servicedata_count = egsn_pdp_servicedata_count
        self.egsn_pdp_rec_ext_count = egsn_pdp_rec_ext_count

  
