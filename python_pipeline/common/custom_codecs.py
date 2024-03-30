from typing import Any

from faust.serializers import codecs


class StringCodec(codecs.Codec):

    def _dumps(self, obj: Any) -> bytes:
        return str(obj).encode("utf-8")

    def _loads(self, s: bytes) -> Any:
        return s.decode("utf-8")
