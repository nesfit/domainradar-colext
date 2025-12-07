"""
iana_db.py: Provides an interface for fetching WHOIS servers for TLDs from the IANA Root Zone Database.

The code is heavily inspired by a similar module in the PyFunceble project:
https://pyfunceble.readthedocs.io/en/latest/code/PyFunceble.cli.scripts.html#PyFunceble.cli.scripts.iana.IanaDBGenerator

Original license:
Copyright 2017, 2018, 2019, 2020, 2022, 2023 Nissar Chababy

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

import concurrent.futures
import json
import requests
import re

from common import timestamp_now_millis
from . import WhoisClient


class IanaDBGenerator:
    """
    Provides an interface for fetching WHOIS servers for TLDs from the IANA Root Zone Database.
    """

    UPSTREAM_LINK: str = "https://www.iana.org/domains/root/db"
    """
    Provides the upstream link.
    """

    IANA_WHOIS_SERVER: str = "whois.iana.org"
    """
    The WHOIS server provided by the IANA.
    """

    MANUAL_SERVER: dict[str, str] = {
        "bm": "whois.afilias-srs.net",
        "bz": "whois.afilias-grs.net",
        "cd": "chois.nic.cd",
        "cm": "whois.netcom.cm",
        "fj": "whois.usp.ac.fj",
        "ga": "whois.my.ga",
        "int": "whois.iana.org",
        "ipiranga": " whois.nic.ipiranga",
        "lc": "whois2.afilias-grs.net",
        "now": " whois.nic.now",
        "pharmacy": " whois.nic.pharmacy",
        "piaget": " whois.nic.piaget",
        "ps": "whois.pnina.ps",
        "rw": "whois.ricta.org.rw",
        "shaw": "whois.afilias-srs.net",
        "xn--1ck2e1b": "whois.nic.xn--1ck2e1b",
        "xn--2scrj9c": "whois.inregistry.net",
        "xn--3hcrj9c": "whois.inregistry.net",
        "xn--45br5cyl": "whois.inregistry.net",
        "xn--45brj9c": "whois.inregistry.net",
        "xn--8y0a063a": "whois.nic.xn--8y0a063a",
        "xn--bck1b9a5dre4c": "whois.nic.xn--bck1b9a5dre4c",
        "xn--cck2b3b": "whois.nic.xn--cck2b3b",
        "xn--czr694b": "whois.nic.xn--czr694b",
        "xn--e1a4c": "whois.eu",
        "xn--eckvdtc9d": "whois.nic.xn--eckvdtc9d",
        "xn--fct429k": "whois.nic.xn--fct429k",
        "xn--fpcrj9c3d": "whois.inregistry.net",
        "xn--fzc2c9e2c": "whois.nic.lk",
        "xn--g2xx48c": "whois.nic.xn--g2xx48c",
        "xn--gckr3f0f": "whois.nic.xn--gckr3f0f",
        "xn--gecrj9c": "whois.inregistry.net",
        "xn--gk3at1e": "whois.nic.xn--gk3at1e",
        "xn--h2breg3eve": "whois.inregistry.net",
        "xn--h2brj9c": "whois.inregistry.net",
        "xn--h2brj9c8c": "whois.inregistry.net",
        "xn--imr513n": "whois.nic.xn--imr513n",
        "xn--jvr189m": "whois.nic.xn--jvr189m",
        "xn--kpu716f": "whois.nic.xn--kpu716f",
        "xn--mgba3a3ejt": "whois.nic.xn--mgba3a3ejt",
        "xn--mgbb9fbpob": "whois.nic.xn--mgbb9fbpob",
        "xn--mgbbh1a": "whois.inregistry.net",
        "xn--mgbbh1a71e": "whois.inregistry.net",
        "xn--mgbgu82a": "whois.inregistry.net",
        "xn--nyqy26a": "whois.nic.xn--nyqy26a",
        "xn--otu796d": "whois.nic.xn--otu796d",
        "xn--pbt977c": "whois.nic.xn--pbt977c",
        "xn--rhqv96g": "whois.nic.xn--rhqv96g",
        "xn--rovu88b": "whois.nic.xn--rovu88b",
        "xn--rvc1e0am3e": "whois.inregistry.net",
        "xn--s9brj9c": "whois.inregistry.net",
        "xn--ses554g": "whois.registry.knet.cn",
        "xn--wgbh1c": "whois.dotmasr.eg",
        "xn--xkc2al3hye2a": "whois.nic.lk",
        "xn--xkc2dl3a5ee0h": "whois.inregistry.net",
        "za": "whois.registry.net.za",
    }

    def __init__(self, db_file: str | None = None, max_db_life_time_ms: int | None = None) -> None:
        self._max_db_life_time_ms = max_db_life_time_ms
        if db_file is not None:
            self._db_file = db_file
        else:
            self._db_file = "iana_whois_servers.json"

        try:
            with open(self._db_file, "r", encoding="utf-8") as f:
                self._database = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self._database = {}

    def _get_referrer_from_extension(self, extension: str) -> str | None:
        """
        Given an extension, tries to get or guess its extension.
        """

        iana_page = requests.get(f"{self.UPSTREAM_LINK}/{extension}.html")
        regex_iana = "<b>WHOIS Server:</b>\s*([a-zA-Z0-9._-]+)\s*<br"
        matched = re.search(regex_iana, iana_page.text)
        if matched:
            return matched.group(1)

        whois_client = WhoisClient()
        dummy_domain = f"hello.{extension}"
        iana_record = whois_client.try_query(query=dummy_domain, server=self.IANA_WHOIS_SERVER)

        if iana_record and "refer" in iana_record:
            regex_referrer = r"(?s)refer\:\s+([a-zA-Z0-9._-]+)\n"
            matched = re.search(regex_referrer, iana_record)
            if matched:
                return matched.group(1)

        possible_server = f"whois.nic.{extension}"
        response = whois_client.try_query(query=dummy_domain, server=possible_server)
        if response:
            return possible_server

        if extension in self.MANUAL_SERVER:
            possible_server = self.MANUAL_SERVER[extension]
            response = whois_client.try_query(query=dummy_domain, server=possible_server)
            if response:
                return possible_server

        return None

    def _get_extension_and_referrer_from_block(
            self, block: str
    ) -> tuple[str | None, str | None]:
        """
        Given an HTML block, we try to extract an extension and it's underlying
        referrer (WHOIS server).

        The referrer is extracted from the official IANA page, and guessed if
        missing.

        :param block:
            The block to parse.
        """

        regex_valid_extension = r"(/domains/root/db/)(.*)(\.html)"

        match = re.search(regex_valid_extension, block)
        if match:
            extension = match.group(2)
            if extension:
                extension = extension.lower()
                return extension, self._get_referrer_from_extension(extension)

        return None, None

    def _fetch_current_data(self, max_workers: int | None = None) -> None:
        """
        Starts the generation of the dataset file.

        :param max_workers:
            The maximal number of workers we are allowed to use.
        """

        remote_data = requests.get(self.UPSTREAM_LINK)
        remote_data.raise_for_status()

        raw_data = remote_data.text.split('<span class="domain tld">')

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for extension, whois_server in executor.map(
                    self._get_extension_and_referrer_from_block, raw_data
            ):
                if extension:
                    self._database[extension] = whois_server

        with open(self._db_file, "w", encoding="utf-8") as f:
            self._database["__last_update"] = timestamp_now_millis()
            json.dump(self._database, f, indent=4, ensure_ascii=False)

    def get_db(self) -> dict:
        """
        Returns the current database as a dictionary.
        """
        current_database = self._database
        if self._max_db_life_time_ms is not None and "__last_update" in self._database:
            last_update = self._database["__last_update"]
            if timestamp_now_millis() - last_update > self._max_db_life_time_ms:
                self._database = {}

        if len(self._database) < 2:
            # noinspection PyBroadException
            try:
                self._fetch_current_data()
            finally:
                if len(self._database) < 2:
                    self._database = current_database

        return self._database
