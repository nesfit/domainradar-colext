import os
from io import BytesIO
from typing import Any

import avro.schema
from avro.io import DatumWriter, BinaryEncoder
from faust.serializers import codecs
from faust.utils import json as _json
from mode.utils.compat import want_str, want_bytes
from pydantic import BaseModel

from .models import avro_namespaces, avro_overrides


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


class PydanticAvroCodec(codecs.Codec):
    def __init__(self, schemas_dir: str, **kwargs: Any):
        super().__init__(**kwargs)
        self._schemas = {}

        for file in os.listdir(schemas_dir):
            if not file.endswith(".avsc"):
                continue

            type_name = os.path.basename(file).replace(".avsc", "")
            with open(os.path.join(schemas_dir, file), "r") as f:
                schema = avro.schema.parse(f.read())

            self._schemas[type_name] = schema

    def get_schema(self, model_type):
        if model_type in avro_overrides:
            schema_type = avro_overrides[model_type]
        elif model_type in avro_namespaces:
            schema_type = avro_namespaces[model_type] + "." + model_type.__name__
        else:
            schema_type = avro_namespaces["default"] + "." + model_type.__name__

        schema = self._schemas.get(schema_type)
        if schema is None:
            raise ValueError(f"No schema found for type {schema_type}")

        return schema

    def _dumps(self, obj: Any) -> bytes:
        if not isinstance(obj, BaseModel):
            raise ValueError("Only Pydantic models can be serialized with this codec")

        model_type = obj.__class__
        schema = self.get_schema(model_type)

        data = obj.model_dump(mode="python", by_alias=True)
        with BytesIO() as bio:
            DatumWriter(schema).write(data, BinaryEncoder(bio))
            return bio.getbuffer().tobytes()

    def _loads(self, s: bytes) -> Any:
        return s
