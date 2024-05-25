from typing import Any

from faust.serializers import codecs
from faust.utils import json as _json

from mode.utils.compat import want_str, want_bytes
from pydantic import BaseModel


class StringCodec(codecs.Codec):

    def _dumps(self, obj: Any) -> bytes:
        return str(obj).encode("utf-8")

    def _loads(self, s: bytes) -> Any:
        return s.decode("utf-8")


class PydanticCodec(codecs.Codec):
    def _dumps(self, obj: Any) -> bytes:
        if isinstance(obj, BaseModel):
            return want_bytes(obj.model_dump_json(indent=None, by_alias=True, context={"separators": (',', ':')}))
        else:
            return want_bytes(_json.dumps(obj))

    def _loads(self, s: bytes) -> Any:
        if len(s) == 0:
            return {}
        return _json.loads(want_str(s))
