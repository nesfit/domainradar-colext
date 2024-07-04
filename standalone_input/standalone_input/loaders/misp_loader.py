"""
MISP domain name loader - reads a MISP feed and loads domain names from it
"""
__authors__ = ["Jan Polišenský <ipolisensky@fit.vut.cz>",
               "Adam Horák <ihorak@fit.vut.cz>",
               "Ondřej Ondryáš <xondry02@vut.cz"]

import re
from typing import List

from pymisp import PyMISP

from ..models import Domain
from requests.auth import HTTPBasicAuth

from standalone_input.loaders.utils import LoaderUtils as Utils
import logging

logger = logging.getLogger(__name__)


class MISPLoader:
    """Local file data loader for the collector"""
    valid_sources = ("plain", "octet-stream", "html", "csv")

    def __init__(self, feed_name: str, config: dict):
        misp_config = config.get("misp", {})
        misp_url = misp_config.get("url")
        misp_key = misp_config.get("key")
        misp_verify_certificate = misp_config.get("verify_certificate", True)
        misp_debug = misp_config.get("debug", False)
        misp_username = misp_config.get("username")
        misp_password = misp_config.get("password")
        misp_feeds = misp_config.get("feeds", {})

        if misp_username and misp_password:
            auth = HTTPBasicAuth(misp_username, misp_password)
        else:
            auth = None

        if not misp_url or not misp_key or not misp_feeds or len(misp_feeds) == 0:
            raise ValueError("MISP URL, API key and feed ID definitions must be specified in the config file")

        if feed_name not in misp_feeds:
            raise ValueError(f"Feed '{feed_name}' not found in the MISP feeds configuration")

        self.source = feed_name
        self.feed_id, self.category = misp_feeds[feed_name]
        self.misp = PyMISP(misp_url, misp_key, misp_verify_certificate, debug=misp_debug, auth=auth)

    def load(self):
        """A generator that just yields the domains found (generator is used for consistency with other loaders)"""
        domain_names: List[Domain] = []
        event = self.misp.get_event(self.feed_id, pythonify=True)
        for i in event:
            if i == 'Attribute':
                for j in event[i]:
                    domain = re.search(Utils.hostname_regex, j.value)
                    if domain:
                        dom_name = domain.group(0)  # type: str
                        domain_names.append(Domain(
                            name=dom_name,
                            url=j.value,
                            source=self.source,
                            category=self.category
                        ))
        logger.info("Loaded %d domains from MISP feed %s", len(domain_names), self.source)
        yield domain_names
