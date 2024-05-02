"""extractor.py: The feature extraction process implementation.
Provides the extract_features function that takes raw data, passes it through the configured transformations and returns
 a list of feature vectors. The list of transformations is initialized from the configuration."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

from collections import OrderedDict
from typing import Iterable

from pandas import DataFrame

from .compat import CompatibilityTransformation
from .transformations.base_transformation import Transformation
from .transformations.dns import DNSTransformation
from .transformations.drop_columns import DropColumnsTransformation
from .transformations.geo import GeoTransformation
from .transformations.ip import IPTransformation
from .transformations.lexical import LexicalTransformation
from .transformations.rdap_dn import RDAPDomainTransformation
from .transformations.rdap_ip import RDAPAddressTransformation
from .transformations.tls import TLSTransformation

# The list of all transformations. The order in this dictionary determines the order in which the transformations
# will be applied to the data. The keys are used in the configuration to enable or disable specific transformations.
_all_transformations = OrderedDict([
    ("dns", DNSTransformation),
    ("ip", IPTransformation),
    ("geo", GeoTransformation),
    ("tls", TLSTransformation),
    ("lexical", LexicalTransformation),
    ("rdap_dn", RDAPDomainTransformation),
    ("rdap_ip", RDAPAddressTransformation),
    ("drop", DropColumnsTransformation)
])

_enabled_transformations: list[Transformation] = []
_compat_transformation = CompatibilityTransformation()


def init_transformations(config: dict):
    """
    Initializes the transformation list based on the `enabled_transformations` key in the configuration.
    If the key is not present, all transformations are enabled.
    :param config: The configuration dictionary.
    """
    global _enabled_transformations

    enabled_transformation_ids: Iterable[str] | None = config.get("enabled_transformations", None)

    if enabled_transformation_ids is None:
        _enabled_transformations = [trans(config) for trans in _all_transformations.values()]
    else:
        _enabled_transformations.clear()
        for tid, transformation in _all_transformations.items():
            if tid in enabled_transformation_ids:
                _enabled_transformations.append(transformation(config))


def extract_features(raw_data: Iterable[dict]) -> Iterable[dict]:
    """
    Extracts features from the raw data by passing it through the transformations enabled in the configuration.
    The `init_transformations` function must be called once before this function can be used.
    :param raw_data: An iterable of dictionaries, each representing one entry of raw data.
    :return: An iterable of dictionaries, each representing one feature vector.
    """
    # Transform the raw data into a format compatible with the transformations
    raw_data_compatible = (_compat_transformation.transform(x) for x in raw_data)
    # Create a DataFrame where each row is one entry from the raw_data iterable
    data_frame = DataFrame(raw_data_compatible, copy=False)
    # Apply the transformations
    for transformation in _enabled_transformations:
        data_frame = transformation.transform(data_frame)
    # Return a list of dictionaries
    return data_frame.to_dict(orient="records")
