"""manager.py: Manages the configuration exchange process."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import copy
import logging
import os.path
import subprocess

from . import transformers, codes
from .models import ConfigurationChangeResult as _Res, ConfigurationValidationError as _Err

logger = logging.getLogger(__package__)
config = {}

_components = {
    "collector-dns": "dns.toml",
    "collector-zone": "zone.toml",
    "collector-tls": "tls.properties",
    "collector-nerd": "nerd.properties",
    "collector-geoip": "geoip.properties",
    "collector-rdap-dn": "rdap_dn.toml",
    "collector-rdap-ip": "rdap_ip.toml",
    "collector-rtt": "rtt.toml",
    "merger": "mergers.properties",
    "extractor": "extractor.toml",
    "classifier-unit": "classifier_unit.toml",
}


def apply_all_configs(configs: dict) -> list[tuple[str, _Res]]:
    """
    Applies all configurations from the specified dictionary to the components.
    Returns a list of configuration "change" results with snapshots of the configuration for all known components
    that were not included in the input dictionary.
    """
    ret = []
    logger.info("Applying initial configurations")
    for component_id, file in _components.items():
        if component_id not in configs:
            # Send initial snapshot (sanitized)
            ret.append((component_id, _Res(success=True, errors=None, message=None,
                                           currentConfig=get_current_config(component_id, True))))
            continue

        apply_config(component_id, configs[component_id]["currentConfig"], False)
    return ret


def apply_config(component_id: str, new_config: dict, updown: bool = True) -> _Res:
    """
    Applies the specified configuration to the specified component.
    The updown parameter controls whether the component will be stopped before applying the new configuration
    (and started afterward).
    """
    logger.info("Applying configuration for %s", component_id)

    current_config = get_current_config(component_id)
    is_toml = _is_toml(component_id)
    if is_toml:
        warns = _apply_static_fields_toml(current_config, new_config)
        new_config_snapshot = _sanitize_published_toml_snapshot(new_config)
    else:
        warns = _apply_static_fields_properties(current_config, new_config)
        new_config_snapshot = _sanitize_published_properties_snapshot(new_config)

    if updown:
        if not _down(component_id):
            return _Res(success=False,
                        errors=[_error("", codes.OTHER, "Failed to stop the component")],
                        message="Failed to stop the component",
                        currentConfig=_sanitize_published_toml_snapshot(current_config) \
                            if is_toml else _sanitize_published_properties_snapshot(current_config))
        _write_config(component_id, new_config, current_config)
        if not _up(component_id):
            return _Res(success=False,
                        errors=[_error("", codes.OTHER, "Failed to start the component")] + warns,
                        message="Failed to start the component",
                        currentConfig=new_config_snapshot)
    else:
        _write_config(component_id, new_config, current_config)

    return _Res(success=True,
                errors=None if len(warns) == 0 else warns,
                message=None,
                currentConfig=new_config_snapshot)


def get_current_config(component_id: str, sanitize: bool = False) -> dict:
    """Reads the current configuration of the specified component and transforms it to a configuration dictionary."""
    filename = _components.get(component_id)
    if not filename:
        return {}

    filename = _get_full_config_path(filename)
    if filename.endswith(".toml"):
        with open(filename, "rb") as f:
            ret = transformers.load_toml(f)
    else:
        with open(filename, "r") as f:
            ret = transformers.load_properties(f)

    if sanitize:
        if filename.endswith(".toml"):
            ret = _sanitize_published_toml_snapshot(ret)
        else:
            ret = _sanitize_published_properties_snapshot(ret)

    return ret


def is_known_component(component_id: str) -> bool:
    """Returns True if the specified component is known to the manager, False otherwise."""
    return component_id in _components


def _down(component_id: str) -> bool:
    """Stops the specified component."""
    compose_cmd = config["manager"]["compose_command"]
    rc = subprocess.Popen(f"{compose_cmd} down {component_id}", shell=True).wait()
    if rc != 0:
        logger.error("Failed to stop %s: %s", component_id or "all services", rc)
        return False
    return True


def _up(component_id: str) -> bool:
    """Starts the specified component."""
    compose_cmd = config["manager"]["compose_command"]
    rc = subprocess.Popen(f"{compose_cmd} up -d {component_id}", shell=True).wait()
    if rc != 0:
        logger.error("Failed to start %s: %s", component_id or "all services", rc)
        return False
    return True


def _write_config(component_id: str, new_config: dict, old_config: dict):
    """
    Writes a new configuration to the component's configuration file.
    If serialization of the new configuration dictionary fails, writes the old one.
    """
    config_file = _components[component_id]
    config_file_path = _get_full_config_path(config_file)
    logger.info("Writing configuration file %s", config_file)
    mode = "wb" if _is_toml(component_id) else "w"
    try:
        with open(config_file_path, mode) as f:
            if _is_toml(component_id):
                transformers.write_toml(new_config, f)
            else:
                transformers.write_properties(new_config, f)
    except Exception as e:
        logger.error("Failed to write new configuration: %s", e)
        with open(config_file_path, mode) as f:
            if _is_toml(component_id):
                transformers.write_toml(old_config, f)
            else:
                transformers.write_properties(old_config, f)


def _apply_static_fields_properties(old_config: dict, new_config: dict) -> list:
    """
    Applies the static configuration keys from an old Properties-based configuration to a new one.
    Returns a list of warnings for static keys requested for a change.

    This configuration is considered static:
    - all keys in the "system" section starting with "ssl." or "security."
    """
    if "system" not in new_config:
        new_config["system"] = {}

    warns = []
    key: str
    for key, value in list(new_config.get("system", {}).items()):
        if key.startswith("ssl.") or key.startswith("security."):
            warn = _error("system." + key, codes.READ_ONLY,
                          "Cannot change a system property, keeping the old value", soft=True)
            if key in old_config["system"]:
                if old_config["system"][key] != value:
                    warns.append(warn)
                    new_config["system"][key] = value
            else:
                warns.append(warn)
                del new_config["system"][key]

    return warns


def _apply_static_fields_toml(old_config: dict, new_config: dict) -> list:
    """
    Applies the static configuration keys from an old TOML-based configuration to a new one.
    Returns a list of warnings for static keys requested for a change.

    This configuration is considered static:
    - the entire "connection" section
    - the "app_id" field in any section
    """
    warns = []
    if "connection" in new_config and new_config["connection"] != old_config.get("connection", {}):
        warns.append(_error("connection", codes.READ_ONLY, None, soft=True))

    new_config["connection"] = old_config.get("connection", {})

    for key, value in old_config.items():
        if isinstance(value, dict) and "app_id" in value:
            if key not in new_config:
                new_config[key] = {}

            if "app_id" in new_config[key] and new_config[key]["app_id"] != value["app_id"]:
                warns.append(_error(key + ".app_id", codes.READ_ONLY,
                                    "Cannot change the application ID, keeping the old value", soft=True))

            new_config[key]["app_id"] = value["app_id"]

    return warns


def _sanitize_published_toml_snapshot(config: dict) -> dict:
    """Removes static configuration keys from a TOML-based configuration dictionary."""
    config = copy.deepcopy(config)
    if "connection" in config:
        del config["connection"]
    for key, value in config.items():
        if isinstance(value, dict) and "app_id" in value:
            del value["app_id"]
    return config


def _sanitize_published_properties_snapshot(config: dict):
    """Removes static configuration keys from a Properties-based configuration dictionary."""
    config = copy.deepcopy(config)
    if "system" not in config:
        config["system"] = {}
    for key in list(config["system"].keys()):
        if key.startswith("ssl.") or key.startswith("security."):
            del config["system"][key]
    return config


def _is_toml(component_id: str) -> bool:
    """Returns True if the component uses a TOML configuration file, False otherwise."""
    return _components.get(component_id, "").endswith(".toml")


def _get_full_config_path(filename):
    """Returns the full path to a configuration file based on the manager's configuration."""
    return os.path.join(config["manager"]["configs_dir"], filename)


def _error(path: str, code: int, msg: str | None, soft: bool = False) -> _Err:
    """A helper function for generating a ConfigurationValidationError."""
    return _Err(propertyPath=path,
                errorCode=code,
                error=msg,
                soft=soft)
