Thor Collectors
===============

Purpose
-------
Thor Collectors is a suite of Kafka-driven, multiprocessing collectors used to
gather domain-related threat intelligence data. Each collector is a small
service that consumes messages from an input topic, enriches or resolves data,
and produces one or more output messages. The project uses the in-repo
`kafka-multiprocessor` library to run worker pools with exactly-once style
processing per input message.

What it collects
----------------
- Zone: Determines the DNS zone (SOA, nameservers, DNSSEC presence) for a domain.
- DNS: Queries A, AAAA, CNAME, MX, NS, TXT records and emits IPs for further
  processing.
- RDAP (domain): Fetches RDAP data for domains, with entity expansion.
- WHOIS: Queries WHOIS as a fallback when RDAP data is missing or unavailable.
- RDAP (IP): Fetches RDAP data for IPv4/IPv6 addresses.
- RTT: Measures ICMP round-trip time to discovered IPs.

Message flow and topics
-----------------------
Each collector runs as a Kafka consumer/producer with a defined input topic and
one or more outputs:

- Zone collector
  - Input: `to_process_zone`
  - Output: `processed_zone`
  - Fan-out: `to_process_DNS` and `to_process_RDAP_DN` when a zone is found.

- DNS collector
  - Input: `to_process_DNS`
  - Output: `processed_DNS`
  - Fan-out: `to_process_IP` for extracted IPs, `to_process_TLS` for a single
    selected IP if available.

- RDAP (domain) collector
  - Input: `to_process_RDAP_DN`
  - Output: `processed_RDAP_DN`
  - Fan-out: `to_process_WHOIS` on RDAP errors.

- WHOIS collector
  - Input: `to_process_WHOIS`
  - Output: `processed_WHOIS`

- RDAP (IP) collector
  - Input: `to_process_IP`
  - Output: `collected_IP_data`

- RTT collector
  - Input: `to_process_IP`
  - Output: `collected_IP_data`

Data model and status codes
---------------------------
- Data contracts live in `src/common/models.py` (Pydantic models using camelCase
  aliases for Kafka payloads).
- Status and error codes are defined in `src/common/result_codes.py` and are
  used consistently across collectors.

Configuration
-------------
- All components read a TOML config from `APP_CONFIG_FILE` (default:
  `./config.toml`).
- The config defines Kafka connection/consumer/producer options plus
  collector-specific settings (timeouts, DNS servers, rate limiting, etc.).
- `kafka-multiprocessor` supports optional Redis-backed rate limiting; the
  collectors use it for RDAP/WHOIS when enabled.

Running collectors
------------------
Each collector has a module entry point that starts the Kafka worker pool using
`kafka-multiprocessor`:

```bash
export APP_CONFIG_FILE=./config.toml
python -m thor_collectors.zone
python -m thor_collectors.dns
python -m thor_collectors.rdap_dn
python -m thor_collectors.whois
python -m thor_collectors.rdap_ip
python -m thor_collectors.rtt
```

Repository layout
-----------------
- `src/thor_collectors/`: collector implementations and entry points.
- `src/common/`: shared models, logging, utilities, and result codes.
- `kafka-multiprocessor/`: local library for multiprocessing Kafka consumers.
- `config.toml`: example configuration used by the collectors.
- `pyproject.toml`: project metadata and dependencies (Python >= 3.13).
