import socket
import asyncio
from random import Random

import dns.resolver
import dns.asyncresolver
import dns.rdatatype
import dns.exception

from dns.resolver import Cache

import common.result_codes

NOT_FOUND_MESSAGES = [
    "no match",
    "not found",
    "no entries found",
    "invalid query",
    "domain name not known",
    "no object found",
    "available for re-registration",
    "object does not exist",
    "domain you requested is not known",
    "invalid pattern",
    "invalid domain"
]

RATE_LIMITED_MESSAGES = [
    "whois limit exceeded",
    "daily connection limit reached",
    "lookup refused",
    "access denied",
    "repeated excessive querying",
    "rate limit exceeded",
    "too many requests",
    "temporarily denied",
    "permanently denied",
    "temporarily blocked",
    "permanently blocked",
    "excessive querying",
    "ratelimited",
    "query looks similar to automated requests",
    "number of requests per client per time interval is restricted"
]


def find_error(whois_response: str) -> int | None:
    lower = whois_response.lower()
    if any(n in lower for n in NOT_FOUND_MESSAGES):
        return common.result_codes.NOT_FOUND
    if any(r in lower for r in RATE_LIMITED_MESSAGES):
        return common.result_codes.RATE_LIMITED
    return None


class WhoisClient:

    def __init__(self):
        self._dns_cache = Cache()
        self._resolver = dns.resolver.Resolver(configure=True)
        self._resolver.cache = self._dns_cache
        self._resolver.timeout = 10
        self._async_resolver = None
        self._random = Random()

    @staticmethod
    def _decode_whois_response(data: bytes | bytearray) -> str:
        try:
            response = data.decode()
        except UnicodeDecodeError:
            response = data.decode("latin-1", "replace")
        return response.replace("\r\n", "\n")

    def _get_address(self, answers):
        if not answers:
            return None
        target_addresses = answers.get(dns.rdatatype.A)
        if not target_addresses or len(target_addresses) == 0:
            return None
        return target_addresses[self._random.randrange(0, len(target_addresses.rrset))].address

    def query(self, query: str, server: str, timeout: float = 5.0, bufsize: int = 32768) -> str:
        """
        Perform a blocking WHOIS query.

        Args:
            query: Raw WHOIS query text (e.g., "example.com").
            server: WHOIS server hostname (e.g., "whois.verisign-grs.com").
            timeout: Socket timeout in seconds.
            bufsize: Size of the read buffer in bytes.

        Returns:
            Full WHOIS response as text.

        Raises:
            socket.timeout, OSError: On networking errors/timeouts.
        """

        target_answers = self._resolver.resolve_name(server, socket.AF_INET)
        target_address = self._get_address(target_answers)
        if not target_address:
            raise OSError(f"Cannot resolve WHOIS server address: {server}")

        message = query.encode("utf-8", "strict")

        out = bytearray()
        buf = bytearray(bufsize)
        mv = memoryview(buf)

        with socket.create_connection((target_address, 43), timeout=timeout) as sock:
            # Minimize allocations: two sends, no concatenation.
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.sendall(message)
            sock.sendall(b"\r\n")
            while True:
                n = sock.recv_into(mv, bufsize)
                if n == 0:
                    break
                out.extend(mv[:n])

        return WhoisClient._decode_whois_response(out)

    def try_query(self, *args, **kwargs) -> str | None:
        # noinspection PyBroadException
        try:
            return self.query(*args, **kwargs)
        except Exception:
            return None

    async def query_async(self, query: str, server: str, timeout: float = 5.0, bufsize: int = 65536) -> str:
        """
        Perform a WHOIS query using asyncio.

        Args:
            query: Raw WHOIS query text (e.g., "example.com").
            server: WHOIS server hostname (e.g., "whois.verisign-grs.com").
            timeout: Connection timeout in seconds.

        Returns:
            Full WHOIS response as text.

        Raises:
            asyncio.TimeoutError, OSError: On networking errors/timeouts.
        """
        if self._async_resolver is None:
            self._async_resolver = dns.asyncresolver.Resolver(configure=True)
            self._async_resolver.cache = self._dns_cache
            self._async_resolver.timeout = 10.0

        target_answers = await self._async_resolver.resolve_name(server, socket.AF_INET)
        target_address = self._get_address(target_answers)
        if not target_address:
            raise OSError(f"Cannot resolve WHOIS server address: {server}")

        message = query.encode("utf-8", "strict")
        out = bytearray()
        loop = asyncio.get_running_loop()

        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        try:
            sock.setblocking(False)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            await asyncio.wait_for(loop.sock_connect(sock, (target_address, 43)), timeout=timeout)

            await loop.sock_sendall(sock, message)
            await loop.sock_sendall(sock, b"\r\n")

            while True:
                chunk = await asyncio.wait_for(loop.sock_recv(sock, bufsize), timeout=timeout)
                if not chunk:
                    break
                out.extend(chunk)

            return WhoisClient._decode_whois_response(out)
        finally:
            # noinspection PyBroadException
            try:
                sock.close()
            except Exception:
                pass

    async def try_query_async(self, *args, **kwargs) -> str | None:
        # noinspection PyBroadException
        try:
            return await self.query_async(*args, **kwargs)
        except Exception:
            return None
