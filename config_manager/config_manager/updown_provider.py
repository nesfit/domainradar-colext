import abc
import logging
import socket
import subprocess
from .pipeline_components import components

class BaseUpDownProvider(abc.ABC):
    @abc.abstractmethod
    def up(self, component: str) -> int:
        pass

    @abc.abstractmethod
    def down(self, component: str) -> int:
        pass

    def up_all(self) -> int:
        return self.up(" ".join(components.keys()))

    def down_all(self) -> int:
        return self.down(" ".join(components.keys()))


class DirectUpDownProvider(BaseUpDownProvider):

    def __init__(self, compose_cmd: str):
        self.compose_cmd = compose_cmd

    def up(self, component: str):
        return subprocess.Popen(f"{self.compose_cmd} up -d {component}", shell=True).wait()

    def down(self, component: str):
        return subprocess.Popen(f"{self.compose_cmd} down {component}", shell=True).wait()


class SocketUpDownProvider(BaseUpDownProvider):
    def __init__(self, socket_path: str, logger: logging.Logger):
        self.logger = logger
        self.socket_path = socket_path
        self.sock = None

    def up(self, component: str):
        msg = b'\x01' + component.encode("ascii")
        return self._send(self._pad(msg))

    def down(self, component: str):
        msg = b'\x02' + component.encode("ascii")
        return self._send(self._pad(msg))

    @staticmethod
    def _pad(data: bytes) -> bytes:
        return data + bytes(1024 - len(data))

    def open_socket(self):
        try:
            if not self.sock:
                self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self.sock.connect(self.socket_path)
        except IOError as e:
            self.logger.warning("Socket open error: %s", str(e))
            self.sock.close()
            self.sock = None

    def _send(self, data: bytes) -> int:
        self.open_socket()
        if not self.sock:
            return -1

        try:
            self.sock.sendall(data)
            self.sock.settimeout(2)
            return int.from_bytes(self.sock.recv(4), byteorder='big', signed=True)
        except (ConnectionResetError, IOError) as e:
            self.logger.warning("Socket error: %s", str(e))
            self.sock = None
            return self._send(data)
        except Exception as e:
            self.logger.error("_send error", exc_info=e)
            return -1

    def __del__(self):
        if self.sock:
            # noinspection PyBroadException
            try:
                self.sock.close()
            except Exception:
                pass
