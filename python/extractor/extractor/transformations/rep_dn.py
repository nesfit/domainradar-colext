"""rep_dn.py: Feature extraction transformations for domain data collected using reputation systems."""
__author__ = "Matěj Čech <xcechm15@stud.fit.vut.cz>"

from collections import Counter
import numpy as np
from pandas import DataFrame, concat

from extractor.transformations.base_transformation import Transformation
from ._helpers import map_opentip_kaspersky_zone, map_pulsedive_risk, map_cloudflare_malicious, OPENTIP_KASPERSKY_ZONES


def _extract_virustotal_features(dn_data):
    virustotal = dn_data.get("virustotal", {})
    return (
        virustotal.get("reputation"),
        virustotal.get("malicious"),
        virustotal.get("suspicious"),
        virustotal.get("undetected"),
        virustotal.get("harmless")
    )


def _extract_hybrid_analysis_features(dn_data):
    hybrid_analysis = dn_data.get("hybrid_analysis", {})
    return (
        hybrid_analysis.get("malicious_cnt"),
        hybrid_analysis.get("suspicious_cnt"),
        hybrid_analysis.get("no_threat_cnt"),
        hybrid_analysis.get("whitelisted_cnt"),
        hybrid_analysis.get("worst_score"),
        hybrid_analysis.get("best_score"),
        hybrid_analysis.get("avg_score"),
        hybrid_analysis.get("null_score_cnt")
    )


def _extract_google_safe_browsing_features(dn_data):
    gsb = dn_data.get("google_safe_browsing", {})
    return (
        gsb.get("unspecified_cnt"),
        gsb.get("malware_cnt"),
        gsb.get("social_engineering_cnt"),
        gsb.get("unwanted_software_cnt"),
        gsb.get("potentially_harmful_cnt")
    )


def _extract_opentip_kaspersky_features(dn_data):
    opentip_kaspersky = dn_data.get("opentip_kaspersky", {})

    zone = opentip_kaspersky.get("zone", "")
    categories = opentip_kaspersky.get("categories", [])
    category_zones = opentip_kaspersky.get("category_zones", [])

    is_adware_cat = True if "CATEGORY_ADWARE" in categories else False
    is_phishing_cat = True if "CATEGORY_PHISHING" in categories else False
    is_malware_cat = True if "CATEGORY_MALWARE" in categories else False
    is_compromised_cat = True if "CATEGORY_COMPROMISED" in categories else False
    is_botnet_cat = True if "CATEGORY_BOTNET_CNC" in categories else False

    zone_counts = Counter(category_zones)

    return (
        map_opentip_kaspersky_zone(zone),
        *(zone_counts.get(zone, 0) for zone in OPENTIP_KASPERSKY_ZONES),
        is_adware_cat,
        is_phishing_cat,
        is_malware_cat,
        is_compromised_cat,
        is_botnet_cat
    )


def _extract_urlvoid_features(dn_data) -> int:
    return dn_data.get("urlvoid_detection_counts")


def _extract_cloudflare_radar_features(dn_data) -> bool:
    return map_cloudflare_malicious(dn_data.get("cloudflare_radar_malicious"))


def _extract_fortiguard_features(dn_data) -> bool:
    return dn_data.get("fortiguard_spam", False)


def _extract_threatfox_features(dn_data):
    threatfox = dn_data.get("threatfox", {})
    return threatfox.get("confidence_level") if threatfox.get("malware") else -1


def _extract_pulsedive_features(dn_data):
    pulsedive = dn_data.get("pulsedive", {})
    return (
        map_pulsedive_risk(pulsedive.get("risk")),
        map_pulsedive_risk(pulsedive.get("risk_recommended")),
        pulsedive.get("manual_risk")
    )


class RepDomainTransformation(Transformation):
    def transform(self, df: DataFrame) -> DataFrame:
        # VirusTotal
        df["rep_dn_virustotal_reputation"], df["rep_dn_virustotal_malicious"], df["rep_dn_virustotal_suspicious"], df[
            "rep_dn_virustotal_undetected"], df["rep_dn_virustotal_harmless"] = zip(
            *df["dn_data"].apply(_extract_virustotal_features))

        # Hybrid analysis
        df["rep_dn_hybrid_analysis_malicious_cnt"], df["rep_dn_hybrid_analysis_suspicious_cnt"], \
            df["rep_dn_hybrid_analysis_no_threat_cnt"], df["rep_dn_hybrid_analysis_whitelisted_cnt"], \
            df["rep_dn_hybrid_analysis_worst_score"], df["rep_dn_hybrid_analysis_best_score"], \
            df["rep_dn_hybrid_analysis_avg_score"], df["rep_dn_hybrid_analysis_null_score_cnt"] = zip(
            *df["dn_data"].apply(_extract_hybrid_analysis_features)
        )

        # Google Safe Browsing
        df["rep_dn_google_safe_browsing_unspecified_cnt"], df["rep_dn_google_safe_browsing_malware_cnt"], \
            df["rep_dn_google_safe_browsing_social_engineering_cnt"], \
            df["rep_dn_google_safe_browsing_unwanted_software_cnt"], \
            df["rep_dn_google_safe_browsing_potentially_harmful_cnt"] = zip(
            *df["dn_data"].apply(_extract_google_safe_browsing_features)
        )

        # Opentip Kaspersky
        df["rep_dn_opentip_kaspersky_zone"], df["rep_dn_opentip_kaspersky_category_green_zone_cnt"], \
            df["rep_dn_opentip_kaspersky_category_grey_zone_cnt"], \
            df["rep_dn_opentip_kaspersky_category_yellow_zone_cnt"], \
            df["rep_dn_opentip_kaspersky_category_orange_zone_cnt"], \
            df["rep_dn_opentip_kaspersky_category_red_zone_cnt"], df["rep_dn_opentip_kaspersky_is_category_adware"], \
            df["rep_dn_opentip_kaspersky_is_category_phishing"], df["rep_dn_opentip_kaspersky_is_category_malware"], \
            df["rep_dn_opentip_kaspersky_is_category_compromised"], \
            df["rep_dn_opentip_kaspersky_is_category_botnet_cnc"] = zip(
            *df["dn_data"].apply(_extract_opentip_kaspersky_features)
        )

        # Urlvoid
        df["rep_dn_urlvoid_detection_cnt"] = df["dn_data"].apply(_extract_urlvoid_features)

        # Cloudflare Radar
        df["rep_dn_cloudflare_malicious"] = df["dn_data"].apply(_extract_cloudflare_radar_features)

        # Fortiguard
        df["rep_dn_fortiguard_spam"] = df["dn_data"].apply(_extract_fortiguard_features)

        # Threatfox
        df["rep_dn_threatfox_malware_confidence_level"] = df["dn_data"].apply(_extract_threatfox_features)

        # Pulsedive
        df["rep_dn_pulsedive_risk"], df["rep_dn_pulsedive_risk_recommended"], df["rep_dn_pulsedive_manual_risk"] = zip(
            *df["dn_data"].apply(_extract_pulsedive_features)
        )

        return df

    @property
    def features(self) -> dict[str, str]:
        return {
            "rep_dn_virustotal_reputation": "Int64",
            "rep_dn_virustotal_malicious": "Int64",
            "rep_dn_virustotal_suspicious": "Int64",
            "rep_dn_virustotal_undetected": "Int64",
            "rep_dn_virustotal_harmless": "Int64",
            "rep_dn_opentip_kaspersky_zone": "Int64",
            "rep_dn_opentip_kaspersky_category_green_zone_cnt": "Int64",
            "rep_dn_opentip_kaspersky_category_grey_zone_cnt": "Int64",
            "rep_dn_opentip_kaspersky_category_yellow_zone_cnt": "Int64",
            "rep_dn_opentip_kaspersky_category_orange_zone_cnt": "Int64",
            "rep_dn_opentip_kaspersky_category_red_zone_cnt": "Int64",
            "rep_dn_opentip_kaspersky_is_category_adware": "bool",
            "rep_dn_opentip_kaspersky_is_category_phishing": "bool",
            "rep_dn_opentip_kaspersky_is_category_malware": "bool",
            "rep_dn_opentip_kaspersky_is_category_compromised": "bool",
            "rep_dn_opentip_kaspersky_is_category_botnet_cnc": "bool",
            "rep_dn_hybrid_analysis_malicious_cnt": "Int64",
            "rep_dn_hybrid_analysis_suspicious_cnt": "Int64",
            "rep_dn_hybrid_analysis_no_threat_cnt": "Int64",
            "rep_dn_hybrid_analysis_whitelisted_cnt": "Int64",
            "rep_dn_hybrid_analysis_worst_score": "Int64",
            "rep_dn_hybrid_analysis_best_score": "Int64",
            "rep_dn_hybrid_analysis_avg_score": "float64",
            "rep_dn_hybrid_analysis_null_score_cnt": "Int64",
            "rep_dn_google_safe_browsing_unspecified_cnt": "Int64",
            "rep_dn_google_safe_browsing_malware_cnt": "Int64",
            "rep_dn_google_safe_browsing_social_engineering_cnt": "Int64",
            "rep_dn_google_safe_browsing_unwanted_software_cnt": "Int64",
            "rep_dn_google_safe_browsing_potentially_harmful_cnt": "Int64",
            "rep_dn_urlvoid_detection_cnt": "Int64",
            "rep_dn_cloudflare_malicious": "Int64",
            "rep_dn_fortiguard_spam": "bool",
            "rep_dn_threatfox_malware_confidence_level": "Int64",
            "rep_dn_pulsedive_risk": "Int64",
            "rep_dn_pulsedive_risk_recommended": "Int64",
            "rep_dn_pulsedive_manual_risk": "Int64",
        }
