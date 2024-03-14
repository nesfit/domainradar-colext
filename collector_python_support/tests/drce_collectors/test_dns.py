from typing import NamedTuple

import pytest
import dns.name
from tests import logger

from drce_collectors.dns import DNSCollector, DNSCollectorOptions


class ParamSet(NamedTuple):
    domain_name: str
    expected_zone: str | None = None
    has_primary_ns: bool = False
    has_secondary_ns: bool = False


param_sets = {
    "simple_with_etld": ParamSet('www.fit.vutbr.cz', 'fit.vutbr.cz', True, True),
    "subdomain": ParamSet('www.fit.vutbr.cz', 'fit.vutbr.cz', True, True),
    "two_levels": ParamSet('fit.vutbr.cz', 'fit.vutbr.cz', True, True),
    "simple": ParamSet('vutbr.cz', 'vutbr.cz', True, True),
    "subdomain_with_long_etld": ParamSet('abc.www.city.hakodate.hokkaido.jp', 'city.hakodate.hokkaido.jp', False, True),
    "simple_with_long_etld": ParamSet('city.hakodate.hokkaido.jp', 'city.hakodate.hokkaido.jp', False, True),
    "long_etld": ParamSet('hakodate.hokkaido.jp', 'jp', True, True),
    "etld_only": ParamSet('co.uk', 'co.uk', True, True),
    "tld_only": ParamSet('uk', 'uk', True, True),
    "nonexistent_etld": ParamSet('asdfdasfjewqkrnklwenflpsdnf.co.uk'),
    "nonexistent_simple": ParamSet('asdfdasfjewqkrnklwenflpsdnf.com'),
    "nonexistent_suffix": ParamSet('asdfdasfjewqkrnklwenflpsdnf'),
}

param_sets_existing = {x: y for x, y in param_sets.items() if y.expected_zone is not None}
param_sets_non_existing = {x: y for x, y in param_sets.items() if y.expected_zone is None}
param_sets_with_primary_ips = {x: y for x, y in param_sets_existing.items() if y.has_primary_ns}
param_sets_with_secondary_ips = {x: y for x, y in param_sets_existing.items() if y.has_secondary_ns}


@pytest.fixture(scope='module', params=[DNSCollectorOptions()])
def dns_collector(logger, request) -> DNSCollector:
    return DNSCollector(request.param, logger)


@pytest.mark.parametrize('parset',
                         param_sets_existing.values(), ids=param_sets_existing.keys())
def test_get_zone_info_yields_zone(dns_collector, parset: ParamSet):
    result = dns_collector.get_zone_info(parset.domain_name)
    assert result is not None
    assert result.zone == parset.expected_zone
    assert result.soa is not None


@pytest.mark.parametrize('parset',
                         param_sets_existing.values(), ids=param_sets_existing.keys())
def test_get_zone_info_primary_ns_not_in_secondary(dns_collector, parset: ParamSet):
    result = dns_collector.get_zone_info(parset.domain_name)

    primary_ns_name = dns.name.from_text(result.soa.primary_ns)
    for x in result.secondary_nameservers:
        assert dns.name.from_text(x) != primary_ns_name


@pytest.mark.parametrize('parset',
                         param_sets_existing.values(), ids=param_sets_existing.keys())
def test_get_zone_info_primary_ns_ips_not_in_secondary(dns_collector, parset: ParamSet):
    result = dns_collector.get_zone_info(parset.domain_name)
    assert result.primary_nameserver_ips.isdisjoint(result.secondary_nameservers_ips)


@pytest.mark.parametrize('parset',
                         param_sets_with_primary_ips.values(), ids=param_sets_with_primary_ips.keys())
def test_get_zone_info_yields_primary_ns_ips(dns_collector, parset: ParamSet):
    result = dns_collector.get_zone_info(parset.domain_name)
    assert len(result.primary_nameserver_ips) > 0


@pytest.mark.parametrize('parset',
                         param_sets_with_secondary_ips.values(), ids=param_sets_with_secondary_ips.keys())
def test_get_zone_info_yields_secondary_ns(dns_collector, parset: ParamSet):
    result = dns_collector.get_zone_info(parset.domain_name)
    assert len(result.secondary_nameservers) > 0


@pytest.mark.parametrize('parset',
                         param_sets_with_secondary_ips.values(), ids=param_sets_with_secondary_ips.keys())
def test_get_zone_info_yields_secondary_ns_ips(dns_collector, parset: ParamSet):
    result = dns_collector.get_zone_info(parset.domain_name)
    assert len(result.secondary_nameservers_ips) > 0


@pytest.mark.parametrize('parset',
                         param_sets_non_existing.values(), ids=param_sets_non_existing.keys())
def test_get_zone_info_yields_none(dns_collector, parset: ParamSet):
    result = dns_collector.get_zone_info(parset.domain_name)
    assert result is None
