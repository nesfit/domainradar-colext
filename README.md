# Thor Collectors

This repository hosts the collectors stack for the Thor system. It
is split into Java and Python projects that ingest messages, enrich domain and
IP intelligence, and emit structured results for downstream systems.

## Repository layout

- `collectors-java/`: Maven multi-module build for Java collectors and shared
  libraries.
  - `common/`: shared models, configs, topics, and utilities used across Java
    components.
  - `standalone-collectors/`: collector implementations for TLS, HTTP, GeoASN,
    NERD, and QRadar-style data sources.
  - `serialization/`: JSON (de)serialization and serde helpers for Kafka data
    types.
  - `connect/`: Kafka Connect transforms and converters for ingestion/sink
    pipelines.
  - `log4j-plugins/`: custom Log4j extensions (not used for now).
- `collectors-python/`: Python collectors and shared utilities for DNS, RDAP,
  WHOIS, RTT, and zone intelligence, plus a local `kafka-multiprocessor`
  library for worker pools.
- `build_images.sh`: Docker image build helper for the collectors.
- `sync_result_codes.sh`: helper for syncing result/status code definitions
  between implementations.

## Languages and tooling

- Java 21 with Maven for the Java collector stack.
- Python (uv/pyproject) for the Python collectors and Kafka multiprocessing
  helper library.
- Dockerfiles and compose files for containerized testing and packaging.

## Commit message format

Commit messages follow a [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)-style pattern:

```
type(scope)[!]: short description
```

The scope is optional but the commits **should** ideally introduce changes that are well-focused and can be described by a scope.

If the commit introduces a *breaking change*, please include a `!` just before the `:`.

Types:
- `feat`: a feature – anything that changes or introduces new behavior,
- `fix`: a fix for invalid behavior,
- `refactor`: any other source code change that changes how the program works without changing behavior,
- `style`: any other source code change that doesn't change the program's structure (e.g. reformatting),
- `tests`: any change to testing code,
- `docs`: any change to documentation resources (including more extensive source code comments, javadocs, etc.),
- `chore`: anything that does not fall into one of the above,
- for WIP work in feature branches, you may use `draft`.

Scopes:
- `java`,
- `py`,
- `kmp` for our custom `kafka-multiprocessor` library,
- an ID of a specific collector (zone, dns, tls, http, geo_asn, nerd, rdap_dn, rdap_ip, whois, rtt).