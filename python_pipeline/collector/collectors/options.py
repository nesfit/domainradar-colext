class DNSCollectorOptions:
    def __init__(self, **kwargs):
        self.dns_servers = kwargs.get('dns_servers')
        self.timeout = kwargs.get('timeout')
        self.rotate_nameservers = kwargs.get('rotate_nameservers')
        self.types_to_scan = kwargs.get('types_to_scan')
        self.types_to_process_IPs_from = kwargs.get('types_to_process_IPs_from')
        self.max_record_retries = kwargs.get('max_record_retries')
        self.use_one_socket = kwargs.get('use_one_socket')
