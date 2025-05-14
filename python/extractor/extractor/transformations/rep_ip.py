"""rep_ip.py: Feature extraction transformations for IP data collected using reputation systems."""
__author__ = "Matěj Čech <xcechm15@stud.fit.vut.cz>"

from collections import Counter
import numpy as np
from pandas import DataFrame
from typing import Tuple

from extractor.transformations.base_transformation import Transformation
from ._helpers import (get_max, get_min, mean_of_existing_values, OPENTIP_KASPERSKY_ZONES, PULSEDIVE_RISKS,
                       CRIMINALIP_SCORE)


def _get_sum_of_feature(tag, ip_data, feature):
    return sum(ip[tag][feature] if ip[tag][feature] != -1 else 0 for ip in ip_data) if ip_data is not None else 0


def _get_mean_of_feature(tag, ip_data, feature):
    return mean_of_existing_values([ip[tag][feature] for ip in ip_data]) if ip_data is not None else 0


def _count_true_false(ip_data, tag, selector, t=True, f=False) -> Tuple[int, int]:
    return (
        sum(1 for ip in ip_data if ip[tag][selector] is t),
        sum(1 for ip in ip_data if ip[tag][selector] is f)
    )


def _extract_malicious_cnt(ip_data, tag) -> Tuple[int, int]:
    return (
        sum(1 for ip in ip_data if ip[tag] is True),
        sum(1 for ip in ip_data if ip[tag] is False)
    )


def _true_ratio(true_cnt: int, false_cnt: int) -> int:
    total = true_cnt + false_cnt
    return true_cnt / total if total != 0 else 0


def _true_ratio_with_extract(ip_data, tag) -> float:
    true_cnt, false_cnt = _extract_malicious_cnt(ip_data, tag)
    return _true_ratio(true_cnt, false_cnt)


def _extract_virustotal_features(ip_data):
    tag = "virustotal"

    features = ["reputation", "malicious", "suspicious", "undetected", "harmless"]

    return tuple(_get_mean_of_feature(tag, ip_data, feature) for feature in features)


def _extract_opentip_kaspersky_features(ip_data):
    tag = "opentip_kaspersky"

    zone_cnt = Counter(
        ip[tag]["zone"] for ip in ip_data if ip[tag]["zone"] in OPENTIP_KASPERSKY_ZONES
    )

    green_zone_cnt = 0
    grey_zone_cnt = 0
    yellow_zone_cnt = 0
    orange_zone_cnt = 0
    red_zone_cnt = 0

    adware_cat_cnt = 0
    phishing_cat_cnt = 0
    malware_cat_cnt = 0
    compromised_cat_cnt = 0
    botnet_cat_cnt = 0

    for ip in ip_data:
        data = ip[tag]

        category_zones = data["category_zones"]
        zone_counts = Counter(category_zones)

        # Add the zone counts to the respective zone variables
        green_zone_cnt += zone_counts.get('green', 0)
        grey_zone_cnt += zone_counts.get('grey', 0)
        yellow_zone_cnt += zone_counts.get('yellow', 0)
        orange_zone_cnt += zone_counts.get('orange', 0)
        red_zone_cnt += zone_counts.get('red', 0)

        categories = data["categories"]

        adware_cat_cnt += categories.count("CATEGORY_ADWARE")
        phishing_cat_cnt += categories.count("CATEGORY_PHISHING")
        malware_cat_cnt += categories.count("CATEGORY_MALWARE")
        compromised_cat_cnt += categories.count("CATEGORY_COMPROMISED")
        botnet_cat_cnt += categories.count("CATEGORY_BOTNET_CNC")

    return (
        *(zone_cnt.get(zone, 0) for zone in OPENTIP_KASPERSKY_ZONES),
        green_zone_cnt,
        grey_zone_cnt,
        yellow_zone_cnt,
        orange_zone_cnt,
        red_zone_cnt,
        adware_cat_cnt,
        phishing_cat_cnt,
        malware_cat_cnt,
        compromised_cat_cnt,
        botnet_cat_cnt,
    )


def _extract_hybrid_analysis_features(ip_data):
    tag = "hybrid_analysis"

    # Count the AVG score
    numerator = 0
    denominator = 0

    for ip in ip_data:
        data = ip[tag]

        if 'avg_score' in data and data['avg_score'] != -1:
            item_cnt = data['malicious_cnt'] + data['suspicious_cnt']
            numerator += data['avg_score'] * item_cnt
            denominator += item_cnt

    return (
        _get_sum_of_feature(tag, ip_data, "malicious_cnt"),
        _get_sum_of_feature(tag, ip_data, "suspicious_cnt"),
        _get_sum_of_feature(tag, ip_data, "no_threat_cnt"),
        _get_sum_of_feature(tag, ip_data, "whitelisted_cnt"),
        get_max([ip[tag]["worst_score"] for ip in ip_data]) if ip_data is not None else -1,
        numerator / denominator if denominator != 0 else -1,
        get_min([ip[tag]["best_score"] for ip in ip_data]) if ip_data is not None else -1,
        _get_sum_of_feature(tag, ip_data, "null_score_cnt"),
    )


def _extract_google_safe_browsing_features(ip_data) -> Tuple[int, int, int, int, int]:
    tag = "google_safe_browsing"

    return (
        _get_sum_of_feature(tag, ip_data, "unspecified_cnt"),
        _get_sum_of_feature(tag, ip_data, "malware_cnt"),
        _get_sum_of_feature(tag, ip_data, "social_engineering_cnt"),
        _get_sum_of_feature(tag, ip_data, "unwanted_software_cnt"),
        _get_sum_of_feature(tag, ip_data, "potentially_harmful_cnt"),
    )


def _extract_nerd_features(ip_data) -> float:
    return get_max([ip["nerd_rep"] for ip in ip_data]) if ip_data is not None else -1.0


def _extract_project_honeypot_features(ip_data) -> float:
    return _true_ratio_with_extract(ip_data, "project_honeypot_malicious")


def _extract_cloudflare_radar_features(ip_data) -> float:
    return _true_ratio_with_extract(ip_data, "cloudflare_radar_malicious")


def _extract_abuseipdb_features(ip_data) -> Tuple[int, float, int, int, int, int, int, float]:
    tag = "abuseipdb"

    abuse_confidence_score = [ip[tag]["abuse_confidence_score"] for ip in ip_data] if ip_data is not None else []
    reports = [ip[tag]["total_reports"] for ip in ip_data] if ip_data is not None else []

    is_whitelisted, not_whitelisted = _count_true_false(ip_data, tag, "is_whitelisted")
    is_tor, not_tor = _count_true_false(ip_data, tag, "is_tor")

    return (
        get_max(abuse_confidence_score) if ip_data is not None else -1,
        mean_of_existing_values(abuse_confidence_score) if ip_data is not None else 0,
        _true_ratio(is_whitelisted, not_whitelisted) if ip_data is not None else 0,
        _true_ratio(is_tor, not_tor) if ip_data is not None else 0,
        _get_sum_of_feature(tag, ip_data, "total_reports"),
        mean_of_existing_values(reports) if ip_data is not None else 0
    )


def _extract_fortiguard_features(ip_data) -> float:
    return _true_ratio_with_extract(ip_data, "fortiguard_spam")


def _extract_greynoise_features(ip_data) -> Tuple[int, int, int, int]:
    tag = "greynoise"

    is_noise, not_noise = _count_true_false(ip_data, tag, "noise")
    is_riot, not_riot = _count_true_false(ip_data, tag, "riot")

    return (
        _true_ratio(is_noise, not_noise),
        _true_ratio(is_riot, not_riot),
        sum(1 for ip in ip_data if ip[tag]["classification"] == "benign"),
        sum(1 for ip in ip_data if ip[tag]["classification"] == "malicious")
    )


def _extract_criminalip_features(ip_data) -> Tuple[float, float]:
    def get_score_cnt(ip_data, tag, selector):
        return Counter(
            ip[tag][selector] for ip in ip_data if ip[tag][selector] in CRIMINALIP_SCORE
        )

    tag = "criminalip"

    inbound = get_score_cnt(ip_data, tag, "inbound")
    outbound = get_score_cnt(ip_data, tag, "outbound")
    score_total = inbound + outbound

    return tuple(score_total.get(score, 0) for score in CRIMINALIP_SCORE)


def _extract_threatfox_features(ip_data) -> Tuple[float, float]:
    tag = "threatfox"

    confidence_levels = [ip[tag]["confidence_level"] for ip in ip_data if ip[tag]["confidence_level"] != -1]

    malware_cnt = sum(1 for ip in ip_data if ip[tag]["malware"] is not None and ip[tag]["malware"] != "")
    not_malware = len(ip_data) - malware_cnt

    return (
        _true_ratio(malware_cnt, not_malware),
        mean_of_existing_values(confidence_levels) if confidence_levels is not None else -1
    )


def _extract_pulsedive_features(ip_data) -> Tuple[int, int, int, int, int, int, int, float]:
    def get_risk_cnt(ip_data, tag, selector):
        return Counter(
            ip[tag][selector] for ip in ip_data if ip[tag][selector] in PULSEDIVE_RISKS
        )

    tag = "pulsedive"

    risk = get_risk_cnt(ip_data, tag, "risk")
    risk_recommended = get_risk_cnt(ip_data, tag, "risk_recommended")
    risk_all = risk + risk_recommended

    manual_risk_1, manual_risk_0 = _count_true_false(ip_data, tag, "manual_risk", t=1, f=0)

    return (
        *(risk_all.get(r, 0) for r in PULSEDIVE_RISKS),
        _true_ratio(manual_risk_1, manual_risk_0),
    )


class RepAddressTransformation(Transformation):
    def transform(self, df: DataFrame) -> DataFrame:
        # VirusTotal
        (df["rep_ip_virustotal_avg_reputation"], df["rep_ip_virustotal_avg_malicious"],
         df["rep_ip_virustotal_avg_suspicious"], df["rep_ip_virustotal_avg_undetected"],
         df["rep_ip_virustotal_avg_harmless"]) = zip(
            *df["ip_data"].apply(_extract_virustotal_features)
        )

        # Opentip Kaspersky
        df["rep_ip_opentip_kaspersky_green_zone_cnt"], df["rep_ip_opentip_kaspersky_grey_zone_cnt"], \
            df["rep_ip_opentip_kaspersky_yellow_zone_cnt"], df["rep_ip_opentip_kaspersky_orange_zone_cnt"], \
            df["rep_ip_opentip_kaspersky_red_zone_cnt"], df["rep_ip_opentip_kaspersky_category_green_zone_cnt"], \
            df["rep_ip_opentip_kaspersky_category_grey_zone_cnt"], \
            df["rep_ip_opentip_kaspersky_category_yellow_zone_cnt"], \
            df["rep_ip_opentip_kaspersky_category_orange_zone_cnt"], \
            df["rep_ip_opentip_kaspersky_category_red_zone_cnt"], \
            df["rep_ip_opentip_kaspersky_category_adware_cnt"], \
            df["rep_ip_opentip_kaspersky_category_phishing_cnt"], \
            df["rep_ip_opentip_kaspersky_category_malware_cnt"], \
            df["rep_ip_opentip_kaspersky_category_compromised_cnt"], \
            df["rep_ip_opentip_kaspersky_category_botnet_cnc_cnt"] = zip(
            *df["ip_data"].apply(_extract_opentip_kaspersky_features)
        )

        # Hybrid analysis
        df["rep_ip_hybrid_analysis_malicious_cnt"], df["rep_ip_hybrid_analysis_suspicious_cnt"], \
            df["rep_ip_hybrid_analysis_no_threat_cnt"], df["rep_ip_hybrid_analysis_whitelisted_cnt"], \
            df["rep_ip_hybrid_analysis_worst_score"], df["rep_ip_hybrid_analysis_best_score"], \
            df["rep_ip_hybrid_analysis_avg_score"], df["rep_ip_hybrid_analysis_null_score_cnt"] = zip(
            *df["ip_data"].apply(_extract_hybrid_analysis_features)
        )

        # Google Safe Browsing
        df["rep_ip_google_safe_browsing_total_unspecified_cnt"], df["rep_ip_google_safe_browsing_total_malware_cnt"], \
            df["rep_ip_google_safe_browsing_total_social_engineering_cnt"], \
            df["rep_ip_google_safe_browsing_total_unwanted_software_cnt"], \
            df["rep_ip_google_safe_browsing_total_potentially_harmful_cnt"] = zip(
            *df["ip_data"].apply(_extract_google_safe_browsing_features)
        )

        # NERD
        df["rep_ip_nerd_max_score"] = df["ip_data"].apply(_extract_nerd_features)

        # Project Honeypot
        df["rep_ip_project_honeypot_malicious_ratio"] = df["ip_data"].apply(_extract_project_honeypot_features)

        # Cloudflare Radar
        df["rep_ip_cloudflare_radar_malicious_ratio"] = df["ip_data"].apply(_extract_cloudflare_radar_features)

        # AbuseIPDB
        df["rep_ip_abuseipdb_worst_score"], df["rep_ip_abuseipdb_avg_score"], \
            df["rep_ip_abuseipdb_whitelisted_ratio"], df["rep_ip_abuseipdb_tor_ratio"], \
            df["rep_ip_abuseipdb_total_reports"], df["rep_ip_abuseipdb_avg_reports"] = zip(
            *df["ip_data"].apply(_extract_abuseipdb_features)
        )

        # Fortiguard
        df["rep_ip_fortiguard_spam_ratio"] = df["ip_data"].apply(_extract_fortiguard_features)

        # Greynoise
        df["rep_ip_greynoise_noise_ratio"], df["rep_ip_greynoise_riot_ratio"], df["rep_ip_greynoise_total_benign"], \
            df["rep_ip_greynoise_total_malicious"] = zip(
            *df["ip_data"].apply(_extract_greynoise_features)
        )

        # CriminalIP
        df["rep_ip_criminalip_score_safe_cnt"], df["rep_ip_criminalip_score_low_cnt"], \
            df["rep_ip_criminalip_score_moderate_cnt"], df["rep_ip_criminalip_score_dangerous_cnt"], \
            df["rep_ip_criminalip_score_critical_cnt"] = zip(
            *df["ip_data"].apply(_extract_criminalip_features)
        )

        # Threatfox
        df["rep_ip_threatfox_malware_ratio"], df["rep_ip_threatfox_avg_confidence_level"] = zip(
            *df["ip_data"].apply(_extract_threatfox_features)
        )

        # Pulsedive
        df["rep_ip_pulsedive_risk_none_cnt"], df["rep_ip_pulsedive_risk_low_cnt"], \
            df["rep_ip_pulsedive_risk_medium_cnt"], df["rep_ip_pulsedive_risk_high_cnt"], \
            df["rep_ip_pulsedive_risk_critical_cnt"], df["rep_ip_pulsedive_risk_retired_cnt"], \
            df["rep_ip_pulsedive_risk_unknown_cnt"], df["rep_ip_pulsedive_manual_risk_ratio"] = zip(
            *df["ip_data"].apply(_extract_pulsedive_features)
        )

        return df

    @property
    def features(self) -> dict[str, str]:
        return {
            "rep_ip_virustotal_avg_reputation": "float64",
            "rep_ip_virustotal_avg_malicious": "float64",
            "rep_ip_virustotal_avg_suspicious": "float64",
            "rep_ip_virustotal_avg_undetected": "float64",
            "rep_ip_virustotal_avg_harmless": "float64",
            "rep_ip_opentip_kaspersky_green_zone_cnt": "Int64",
            "rep_ip_opentip_kaspersky_grey_zone_cnt": "Int64",
            "rep_ip_opentip_kaspersky_yellow_zone_cnt": "Int64",
            "rep_ip_opentip_kaspersky_orange_zone_cnt": "Int64",
            "rep_ip_opentip_kaspersky_red_zone_cnt": "Int64",
            "rep_ip_opentip_kaspersky_category_green_zone_cnt": "Int64",
            "rep_ip_opentip_kaspersky_category_grey_zone_cnt": "Int64",
            "rep_ip_opentip_kaspersky_category_yellow_zone_cnt": "Int64",
            "rep_ip_opentip_kaspersky_category_orange_zone_cnt": "Int64",
            "rep_ip_opentip_kaspersky_category_red_zone_cnt": "Int64",
            "rep_ip_opentip_kaspersky_category_adware_cnt": "Int64",
            "rep_ip_opentip_kaspersky_category_phishing_cnt": "Int64",
            "rep_ip_opentip_kaspersky_category_malware_cnt": "Int64",
            "rep_ip_opentip_kaspersky_category_compromised_cnt": "Int64",
            "rep_ip_opentip_kaspersky_category_botnet_cnc_cnt": "Int64",
            "rep_ip_hybrid_analysis_malicious_cnt": "Int64",
            "rep_ip_hybrid_analysis_suspicious_cnt": "Int64",
            "rep_ip_hybrid_analysis_no_threat_cnt": "Int64",
            "rep_ip_hybrid_analysis_whitelisted_cnt": "Int64",
            "rep_ip_hybrid_analysis_worst_score": "Int64",
            "rep_ip_hybrid_analysis_best_score": "Int64",
            "rep_ip_hybrid_analysis_avg_score": "float64",
            "rep_ip_hybrid_analysis_null_score_cnt": "Int64",
            "rep_ip_google_safe_browsing_total_unspecified_cnt": "Int64",
            "rep_ip_google_safe_browsing_total_malware_cnt": "Int64",
            "rep_ip_google_safe_browsing_total_social_engineering_cnt": "Int64",
            "rep_ip_google_safe_browsing_total_unwanted_software_cnt": "Int64",
            "rep_ip_google_safe_browsing_total_potentially_harmful_cnt": "Int64",
            "rep_ip_nerd_max_score": "float64",
            "rep_ip_project_honeypot_malicious_ratio": "float64",
            "rep_ip_cloudflare_radar_malicious_ratio": "float64",
            "rep_ip_abuseipdb_worst_score": "Int64",
            "rep_ip_abuseipdb_avg_score": "float64",
            "rep_ip_abuseipdb_whitelisted_ratio": "float64",
            "rep_ip_abuseipdb_tor_ratio": "float64",
            "rep_ip_abuseipdb_total_reports": "Int64",
            "rep_ip_abuseipdb_avg_reports": "float64",
            "rep_ip_fortiguard_spam_ratio": "float64",
            "rep_ip_greynoise_noise_ratio": "Int64",
            "rep_ip_greynoise_riot_ratio": "Int64",
            "rep_ip_greynoise_total_benign": "Int64",
            "rep_ip_greynoise_total_malicious": "Int64",
            "rep_ip_criminalip_score_safe_cnt": "Int64",
            "rep_ip_criminalip_score_low_cnt": "Int64",
            "rep_ip_criminalip_score_moderate_cnt": "Int64",
            "rep_ip_criminalip_score_dangerous_cnt": "Int64",
            "rep_ip_criminalip_score_critical_cnt": "Int64",
            "rep_ip_threatfox_malware_ratio": "float64",
            "rep_ip_threatfox_avg_confidence_level": "float64",
            "rep_ip_pulsedive_risk_none_cnt": "Int64",
            "rep_ip_pulsedive_risk_low_cnt": "Int64",
            "rep_ip_pulsedive_risk_medium_cnt": "Int64",
            "rep_ip_pulsedive_risk_high_cnt": "Int64",
            "rep_ip_pulsedive_risk_critical_cnt": "Int64",
            "rep_ip_pulsedive_risk_retired_cnt": "Int64",
            "rep_ip_pulsedive_risk_unknown_cnt": "Int64",
            "rep_ip_pulsedive_manual_risk_ratio": "float64",
        }