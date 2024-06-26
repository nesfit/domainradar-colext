"""transformers.py: Contains functions for loading and writing configuration files in TOML and properties formats."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import tomllib
from typing import BinaryIO, TextIO

import tomli_w

_warn_msg = "# This configuration file is managed by config_manager.\n# It should not be changed directly.\n"


def load_toml(toml_fp: BinaryIO) -> dict:
    return tomllib.load(toml_fp)


def write_toml(config: dict, toml_fp: BinaryIO):
    toml_fp.write(_warn_msg.encode("utf-8"))
    return tomli_w.dump(config, toml_fp)


def load_properties(properties_fp: TextIO) -> dict:
    # TODO: Implement stuff like multiline values, space and colon escapes, etc.
    ret = {"collector": {}, "system": {}}
    while line := properties_fp.readline():
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("!"):
            continue

        if "=" in line:
            sep = "="
        elif ":" in line:
            sep = ":"
        else:
            continue

        key, value = line.split(sep, 1)
        key: str = key.strip()
        value: str = value.strip()

        if key.startswith("collectors."):
            ret["collector"][key[11:]] = value
        else:
            ret["system"][key] = value

    return ret


def write_properties(config: dict, properties_fp: TextIO):
    properties_fp.write(_warn_msg)
    collector = config.get("collector", {})
    system = config.get("system", {})
    for key, value in collector.items():
        properties_fp.write(f"collectors.{key}={value}\n")
    for key, value in system.items():
        properties_fp.write(f"{key}={value}\n")
