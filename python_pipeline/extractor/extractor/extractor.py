"""extractor.py: The feature extraction process implementation.
Provides the extract_features function that takes raw data, passes it through the configured transformations and returns
 a DataFrame of feature vectors. The list of transformations is initialized from the configuration."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

from collections import OrderedDict
from typing import Iterable

import pandas as pd
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
"""An ordered dictionary of all data transformer classes.
 The order in this dictionary determines the order in which the transformations will be applied to the data. 
 The keys are used in the configuration to enable or disable specific transformations."""
_enabled_transformations: list[Transformation] = []
"""A list of all enabled and initialized transformer objects."""
_compat_transformation = CompatibilityTransformation()
"""A special transformation that converts the raw data into a format compatible with the other transformations."""
_added_columns_names: list = []
"""A list of the names of all columns added by the transformations."""
_added_columns_with_types: dict = {}
"""A dictionary of the names of all columns added by the transformations and their target DataFrame types."""
_all_columns_with_types: dict = {}
"""A dictionary of all output columns and their target DataFrame types, including the columns added by the
compatibility transformation."""


def init_transformations(config: dict):
    """
    Initializes the transformation list and other metadata based on the `enabled_transformations` key
    in the configuration. If the key is not present, all transformations are enabled.
    :param config: The configuration dictionary.
    """
    global _enabled_transformations, _added_columns_names, _added_columns_with_types, _all_columns_with_types

    enabled_transformation_ids: Iterable[str] | None = config.get("enabled_transformations", None)

    if enabled_transformation_ids is None:
        _enabled_transformations = [trans(config) for trans in _all_transformations.values()]
    else:
        _enabled_transformations.clear()
        for tid, transformation in _all_transformations.items():
            if tid in enabled_transformation_ids:
                _enabled_transformations.append(transformation(config))

    target_features = {}
    for transformation in _enabled_transformations:
        target_features = target_features | transformation.get_new_column_names()

    _added_columns_names = target_features.keys()
    _added_columns_with_types = {k: v for k, v in target_features.items() if not k.startswith("tmp_")}
    _all_columns_with_types = _added_columns_with_types | CompatibilityTransformation.datatypes


def extract_features(raw_data: Iterable[dict]) -> tuple[DataFrame | None, dict[str, Exception]]:
    """
    Extracts features from the raw data by passing it through the transformations enabled in the configuration.
    The `init_transformations` function must be called once before this function can be used.
    :param raw_data: An iterable of dictionaries, each representing one entry of raw data.
    :return: A DataFrame where each row represents one input entry and the columns are the extracted features.
    """
    # Transform the raw data into a format compatible with the transformations
    raw_data_compatible = []
    errors = {}
    for raw_data_entry in raw_data:
        try:
            raw_data_compatible.append(_compat_transformation.transform(raw_data_entry))
        except Exception as e:
            errors[raw_data_entry.get("domain_name", "?")] = e
    if len(raw_data_compatible) == 0:
        return None, errors

    # Create a DataFrame where each row is one entry from the raw_data iterable
    data_frame = DataFrame(raw_data_compatible, copy=False)
    # Create new columns
    new_cols = DataFrame(columns=_added_columns_names)
    data_frame = pd.concat([data_frame, new_cols], axis=1)
    if data_frame.columns.has_duplicates:
        raise ValueError("Invalid input: after adding the features, the DataFrame contains duplicate columns.")
    # Set correct datatypes
    data_frame = data_frame.astype(_all_columns_with_types, copy=False)
    # Apply the transformations
    for transformation in _enabled_transformations:
        data_frame = transformation.transform(data_frame)
    # Ensure the datatypes for the new columns
    # TODO: evaluate if this is necessary
    data_frame = data_frame.astype(_added_columns_with_types, copy=False)
    # Return the final DataFrame
    return data_frame, errors
