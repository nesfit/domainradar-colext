from common.models import SOARecord, DNSData, IPToProcess


def test_ip_to_process_str_and_alias():
    model = IPToProcess(dn="example.com", ip="1.2.3.4")

    assert str(model) == "example.com/1.2.3.4"
    assert model.domain_name == "example.com"


def test_soa_record_aliases_round_trip():
    soa = SOARecord(
        primary_ns="ns1.example.com",
        resp_mailbox_dname="hostmaster.example.com",
        serial="1",
        refresh=10,
        retry=20,
        expire=30,
        min_ttl=60,
    )

    data = soa.model_dump(by_alias=True)

    assert data["primaryNS"] == "ns1.example.com"
    assert data["respMailboxDname"] == "hostmaster.example.com"
    assert data["minTTL"] == 60


def test_dns_data_aliases():
    dns_data = DNSData(a={"1.2.3.4"}, txt=["v=spf1 -all"])

    data = dns_data.model_dump(by_alias=True)

    assert data["A"] == {"1.2.3.4"}
    assert data["TXT"] == ["v=spf1 -all"]
