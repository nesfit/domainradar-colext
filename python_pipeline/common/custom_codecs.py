"""custom_codecs.py: Custom codecs for Faust serialization."""
__author__ = "Ondřej Ondryáš <xondry02@vut.cz>"

from typing import Any

from faust.serializers import codecs
from faust.utils import json as _json

from mode.utils.compat import want_str, want_bytes
from pydantic import BaseModel


class StringCodec(codecs.Codec):
    """
    Custom codec for string serialization and deserialization.
    """

    def _dumps(self, obj: Any) -> bytes:
        return str(obj).encode("utf-8", errors='backslashreplace')

    def _loads(self, s: bytes) -> Any:
        return s.decode("utf-8", errors='backslashreplace')


class PydanticCodec(codecs.Codec):
    """
    Custom codec for Pydantic model serialization and deserialization.

    The _dumps method serializes a Pydantic model into a JSON string. If the object to be serialized is not a Pydantic
    model, it falls back to the default JSON serialization.

    The _loads method deserializes a JSON string into a Python dictionary. If the input string is empty, it returns
    an empty dictionary. The dictionary must be manually converted to a Pydantic model.
    """

    def _dumps(self, obj: Any) -> bytes:
        if isinstance(obj, BaseModel):
            return want_bytes(obj.model_dump_json(indent=None, by_alias=True, context={"separators": (',', ':')}))
        else:
            return want_bytes(_json.dumps(obj))

    def _loads(self, s: bytes) -> Any:
        if len(s) == 0:
            return {}
        return _json.loads(want_str(s))
