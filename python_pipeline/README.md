# DomainRadar Python Components

This directory contains the Python codebase for the DomainRadar project. It includes:

- **Collectors:**
    - [Zone collector](./collector/collectors/zone/zone.py)
    - [DNS collector](./collector/collectors/dns/dnscol.py)
    - [RDAP-DN collector](./collector/collectors/rdap_dn/rdap_dn.py)
    - [RDAP-IP collector](./collector/collectors/rdap_ip/rdap_ip.py)
    - [RTT collector](./collector/collectors/rtt/rtt.py)
- [Feature extractor](./extractor/extractor/extractor.py)

The project is structured into two separate projects: `collector` with all the collectors, and `extractor` with the
feature extractor. The `common` module is shared by the two projects.

The Python components are based on [faust-streaming](https://faust-streaming.github.io/faust/)
and [aiokafka](https://aiokafka.readthedocs.io/en/stable/). Refer to faust's documentation for more information on
running the application.

[Poetry](https://python-poetry.org/) is used for dependency management. The two projects and their dependencies are
defined in the `pyproject.toml` files in the subdirectories.

There is also a `pyproject.toml` file in the root directory that links the two projects as local dependencies in a
"meta-project". This way, you can have a single Poetry virtual environment for the development of the two projects.

The configuration file `config.example.toml` contains the default configuration for all the pipeline components with descriptions of the available options. Copy it to `config.toml` and edit it to suit your needs. You can also use the `APP_CONFIG_FILE` environment variable to set an alternative path to the configuration file.

To run the components, use the following commands:

```bash
cp config.example.toml config.toml
# Edit the configuration file!
poetry update
poetry shell
# The zone collector
python -m collectors.zone worker -l info
# The feature extractor
python -m extractor worker -l info
```

### Third-party data note
The `data` directory contains several JSON files with n-gram frequencies for benign, malware, phishing and DGA-created domain names. The files are used by the feature extractor. They were created by other members of the DomainRadar research team, not by the work's author.