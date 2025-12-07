"""dns_options.py: Shared options container for the zone and DNS collectors."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"


class DNSCollectorOptions:
    """
    Class to hold the options for the zone and DNS collectors.

    This class is used to store the various options that can be configured for the zone and DNS collectors.

    Attributes:
        dns_servers (list): The DNS servers to use for DNS queries.
        timeout (int): The timeout for DNS queries, in seconds.
        rotate_nameservers (bool): Whether to rotate the DNS servers for each query.
        types_to_scan (list): The types of DNS records to scan. Not used in the zone collector.
        types_to_process_IPs_from (list): The types of DNS records to process IPs from. Not used in the zone collector.
        max_record_retries (int): The maximum number of retries for each record. Not used in the zone collector.
    """

    @classmethod
    def from_config(cls, component_config):
        """Creates a DNSCollectorOptions instance based on the provided configuration dictionary."""
        dns_servers = component_config.get("dns_servers", ['195.113.144.194', '193.17.47.1',
                                                           '195.113.144.233', '185.43.135.1'])
        timeout = component_config.get("timeout", 5)
        rotate_nameservers = component_config.get("rotate_nameservers", False)
        types_to_scan = component_config.get("types_to_scan", ['A', 'AAAA', 'CNAME', 'MX', 'NS', 'TXT'])
        types_to_process_ips_from = component_config.get("types_to_process_IPs_from", ['A', 'AAAA', 'CNAME'])
        max_record_retries = component_config.get("max_record_retries", 2)

        return cls(dns_servers=dns_servers, timeout=timeout, rotate_nameservers=rotate_nameservers,
                   types_to_scan=types_to_scan, types_to_process_IPs_from=types_to_process_ips_from,
                   max_record_retries=max_record_retries)

    def __init__(self, **kwargs):
        """
        Initializes a new instance of the DNSCollectorOptions class.

        Args:
            **kwargs: Arbitrary keyword arguments used to set the options for the zone or DNS collector.
                      If an option is not specified, the default value is None. Refer to the class documentation
                      for the list of available options.
        """
        self.dns_servers = kwargs.get('dns_servers')
        self.timeout = kwargs.get('timeout')
        self.rotate_nameservers = kwargs.get('rotate_nameservers')
        self.types_to_scan = kwargs.get('types_to_scan')
        self.types_to_process_IPs_from = kwargs.get('types_to_process_IPs_from')
        self.max_record_retries = kwargs.get('max_record_retries')
