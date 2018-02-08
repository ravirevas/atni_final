class EricssonStats:

    def __init__(self,iteration_id,file_id,file_name,checksum,
                 total_records,success_count,fail_count,
                 pdp_base_total_count,pdp_base_success_count,pdp_base_failed_count,
                 pdp_traffic_count, pdp_camel_count,pdp_rec_ext_count,
                 smo_base_total_count,smo_base_success_count,smo_base_failed_count,
                 smo_camel_count,
                 smt_base_total_count,smt_base_success_count,smt_base_failed_count):
        self.iteration_id = iteration_id
        self.file_id = file_id
        self.file_name = file_name
        self.checksum = checksum
        self.total_records = total_records
        self.success_count = success_count
        self.fail_count = fail_count
        self.pdp_base_total_count = pdp_base_total_count
        self.pdp_base_success_count = pdp_base_success_count
        self.pdp_base_failed_count = pdp_base_failed_count
        self.pdp_traffic_count = pdp_traffic_count
        self.pdp_camel_count = pdp_camel_count
        self.pdp_rec_ext_count = pdp_rec_ext_count
        self.smo_base_total_count = smo_base_total_count
        self.smo_base_success_count = smo_base_success_count
        self.smo_base_failed_count = smo_base_failed_count
        self.smo_camel_count = smo_camel_count
        self.smt_base_total_count = smt_base_total_count
        self.smt_base_success_count = smt_base_success_count
        self.smt_base_failed_count = smt_base_failed_count

  