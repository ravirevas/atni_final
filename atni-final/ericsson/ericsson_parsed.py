class EricssonParsedRecord:

    def __init__(self, file_name, iter_stats, pdp_base,  pdp_traffic_volume_array, pdp_camel_pdp, pdp_rec_ext, smo_base, smo_camel, smt_base):

        self.file_name = file_name
        self.iter_stats = iter_stats
        self.pdp_base  = pdp_base
        self.pdp_traffic_volume_array = pdp_traffic_volume_array
        self.pdp_camel_pdp = pdp_camel_pdp
        self.pdp_rec_ext = pdp_rec_ext
        self.smo_base = smo_base
        self.smo_camel = smo_camel
        self.smt_base = smt_base