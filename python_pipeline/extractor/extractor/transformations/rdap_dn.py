"""rdap_dn.py: Feature extraction transformations for RDAP domain data."""
__authors__ = [
    "Ondřej Ondryáš <xondry02@vut.cz>",
    "Radek Hranický <hranicky@fit.vut.cz>",
    "Adam Horák <ihorak@fit.vut.cz>"
]

from pandas import DataFrame

from extractor.transformations.base_transformation import Transformation
from ._helpers import get_normalized_entropy, simhash, todays_midnight_timestamp


def _get_rdap_domain_features(rdap_entities):
    registrar_name_len = 0
    registrar_name_entropy = 0
    registrar_name_hash = 0

    registrant_name_len = 0
    registrant_name_entropy = 0

    administrative_name_len = 0
    administrative_name_entropy = 0

    administrative_email_len = 0
    administrative_email_entropy = 0

    if rdap_entities is not None:
        if "registrar" in rdap_entities and rdap_entities["registrar"] is not None and len(
                rdap_entities["registrar"]) > 0:
            if "name" in rdap_entities["registrar"][0] and rdap_entities["registrar"][0]["name"] is not None:
                registrar_name_len = len(rdap_entities["registrar"][0]["name"])
                registrar_name_entropy = get_normalized_entropy(rdap_entities["registrar"][0]["name"])
                registrar_name_hash = simhash(
                    rdap_entities["registrar"][0]["name"])  # Makes sense due more than in registrant

        if "registrant" in rdap_entities and rdap_entities["registrant"] is not None and len(
                rdap_entities["registrant"]) > 0:
            if "name" in rdap_entities["registrant"][0] and rdap_entities["registrant"][0]["name"] is not None:
                registrant_name_len = len(rdap_entities["registrant"][0]["name"])
                registrant_name_entropy = administrative_email_entropy = get_normalized_entropy(
                    rdap_entities["registrant"][0]["name"])

        if "administrative" in rdap_entities and rdap_entities["administrative"] is not None and len(
                rdap_entities["administrative"]) > 0:
            if "name" in rdap_entities["administrative"][0] \
                    and rdap_entities["administrative"][0]["name"] is not None:
                administrative_name_len = len(rdap_entities["administrative"][0]["name"])
                administrative_name_entropy = get_normalized_entropy(rdap_entities["administrative"][0]["name"])
            if "email" in rdap_entities["administrative"][0] \
                    and rdap_entities["administrative"][0]["email"] is not None:
                administrative_email_len = len(rdap_entities["administrative"][0]["email"])
                administrative_email_entropy = get_normalized_entropy(rdap_entities["administrative"][0]["email"])

    return registrar_name_len, registrar_name_entropy, registrar_name_hash, registrant_name_len, registrant_name_entropy, \
        administrative_name_len, administrative_name_entropy, \
        administrative_email_len, administrative_email_entropy


class RDAPDomainTransformation(Transformation):
    def transform(self, df: DataFrame) -> DataFrame:
        extraction_ts = todays_midnight_timestamp()

        df['rdap_registration_period'] = df['rdap_expiration_date'] - df['rdap_registration_date']
        df['rdap_domain_age'] = df['rdap_registration_date'].apply(
            lambda x: (extraction_ts - x).total_seconds() / (60 * 60 * 24))
        df['rdap_time_from_last_change'] = df['rdap_last_changed_date'].apply(
            lambda x: (extraction_ts - x).total_seconds() / (60 * 60 * 24))
        df["rdap_domain_active_time"] = (df["rdap_expiration_date"].apply(
            lambda x: min(extraction_ts, x)) - df['rdap_registration_date']).apply(
            lambda x: x.total_seconds() / (60 * 60 * 24))
        df["rdap_has_dnssec"] = df["rdap_dnssec"].astype("bool")
        df["rdap_registrar_name_len"], df["rdap_registrar_name_entropy"], df["rdap_registrar_name_hash"], \
            df["rdap_registrant_name_len"], df["rdap_registrant_name_entropy"], \
            df["rdap_admin_name_len"], df["rdap_admin_name_entropy"], \
            df["rdap_admin_email_len"], df["rdap_admin_email_entropy"] = zip(
            *df["rdap_entities"].apply(_get_rdap_domain_features)
        )

        return df

    def get_new_column_names(self) -> dict[str, str]:
        return {
            "rdap_registration_period": "timedelta64[ms]",
            "rdap_domain_age": "float64",
            "rdap_time_from_last_change": "float64",
            "rdap_domain_active_time": "float64",
            "rdap_has_dnssec": "bool",  # FIXME
            "rdap_registrar_name_len": "Int64",
            "rdap_registrar_name_entropy": "float64",
            "rdap_registrar_name_hash": "Int64",
            "rdap_registrant_name_len": "Int64",
            "rdap_registrant_name_entropy": "float64",
            "rdap_admin_name_len": "Int64",
            "rdap_admin_name_entropy": "float64",
            "rdap_admin_email_len": "Int64",
            "rdap_admin_email_entropy": "float64"
        }
