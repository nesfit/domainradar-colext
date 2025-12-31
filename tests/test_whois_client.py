import asyncio
import socket

import dns.rdatatype
import pytest

from thor_collectors.whois.client import (
    find_error,
    WhoisClient,
)


class _FakeRRset:
    def __init__(self, addresses):
        self.rrset = [type("ARecord", (), {"address": addr}) for addr in addresses]

    def __len__(self):
        return len(self.rrset)

    def __getitem__(self, index):
        return self.rrset[index]


class _FakeAnswers:
    def __init__(self, addresses):
        self._answers = {dns.rdatatype.A: _FakeRRset(addresses)}

    def get(self, key):
        return self._answers.get(key)


class _FakeSocket:
    def __init__(self, recv_chunks):
        self._recv_chunks = list(recv_chunks)
        self.sent = []

    def setsockopt(self, *args, **kwargs):
        pass

    def sendall(self, data):
        self.sent.append(data)

    def recv_into(self, mv, bufsize):
        if not self._recv_chunks:
            return 0
        chunk = self._recv_chunks.pop(0)
        mv[: len(chunk)] = chunk
        return len(chunk)

    def close(self):
        pass


class _SocketContext:
    def __init__(self, sock):
        self._sock = sock

    def __enter__(self):
        return self._sock

    def __exit__(self, exc_type, exc, tb):
        return False


class _AsyncFakeSocket:
    def __init__(self):
        self._closed = False
        self.sent = []

    def setblocking(self, flag):
        pass

    def setsockopt(self, *args, **kwargs):
        pass

    def close(self):
        self._closed = True


class _FakeLoop:
    def __init__(self, recv_chunks):
        self._recv_chunks = list(recv_chunks)

    async def sock_connect(self, sock, addr):
        return None

    async def sock_sendall(self, sock, data):
        sock.sent.append(data)

    async def sock_recv(self, sock, bufsize):
        if not self._recv_chunks:
            return b""
        return self._recv_chunks.pop(0)


def test_find_error_detects_not_found():
    msg = "No entries found for the selected source"
    assert find_error(msg) is not None


def test_find_error_detects_rate_limit():
    msg = "WHOIS LIMIT EXCEEDED. Try later."
    assert find_error(msg) is not None


def test_get_address_selects_from_answers():
    client = WhoisClient()
    client._random.seed(0)
    answers = _FakeAnswers(["1.1.1.1", "2.2.2.2"])

    address = client._get_address(answers)

    assert address in {"1.1.1.1", "2.2.2.2"}


def test_decode_whois_response_falls_back_to_latin1():
    data = b"hello\xffworld"
    decoded = WhoisClient._decode_whois_response(data)

    assert "hello" in decoded
    assert "world" in decoded


def test_query_sends_crlf_and_decodes(monkeypatch):
    client = WhoisClient()
    answers = _FakeAnswers(["1.2.3.4"])
    monkeypatch.setattr(client._resolver, "resolve_name", lambda *args, **kwargs: answers)

    fake_sock = _FakeSocket([b"response\r\n"])
    monkeypatch.setattr(
        socket,
        "create_connection",
        lambda *args, **kwargs: _SocketContext(fake_sock),
    )

    response = client.query("example.com", "whois.example", timeout=1.0)

    assert response == "response\n"
    assert fake_sock.sent == [b"example.com", b"\r\n"]


def test_try_query_returns_none_on_error(monkeypatch):
    client = WhoisClient()
    monkeypatch.setattr(client, "query", lambda *args, **kwargs: (_ for _ in ()).throw(OSError()))

    assert client.try_query("example.com", "whois.example") is None


@pytest.mark.asyncio
async def test_query_async_uses_loop_and_socket(monkeypatch):
    client = WhoisClient()
    answers = _FakeAnswers(["5.6.7.8"])

    async def _resolve_name(*args, **kwargs):
        return answers

    async_resolver = type("AsyncResolver", (), {"resolve_name": _resolve_name})
    monkeypatch.setattr(
        "dns.asyncresolver.Resolver",
        lambda *args, **kwargs: async_resolver,
    )

    fake_sock = _AsyncFakeSocket()
    monkeypatch.setattr(socket, "socket", lambda *args, **kwargs: fake_sock)
    monkeypatch.setattr(asyncio, "get_running_loop", lambda: _FakeLoop([b"ok\r\n"]))

    response = await client.query_async("example.com", "whois.example", timeout=1.0)

    assert response == "ok\n"
    assert fake_sock.sent == [b"example.com", b"\r\n"]


@pytest.mark.asyncio
async def test_try_query_async_returns_none_on_error(monkeypatch):
    client = WhoisClient()

    async def _raise(*args, **kwargs):
        raise OSError()

    monkeypatch.setattr(client, "query_async", _raise)

    assert await client.try_query_async("example.com", "whois.example") is None
