import collections
import dataclasses
import os.path
import sys

import py_avro_schema as pas
import pydantic
# noinspection PyProtectedMember
from py_avro_schema._schemas import RecordField, Option

import common.models as models

if len(sys.argv) != 2:
    print("Usage: generate_schemas.py [output directory]")
    sys.exit(1)

out_dir = sys.argv[1]
if not os.path.exists(out_dir):
    os.makedirs(out_dir)


# Patch py_avro_schema's internal function to use the serialization aliases from our Pydantic models
# instead of the general aliases (that we don't use)
def _record_field_patched(self, name: str, py_field: pydantic.fields.FieldInfo) -> RecordField:
    """Return an Avro record field object for a given Pydantic model field"""
    default = dataclasses.MISSING if py_field.is_required() else py_field.get_default(call_default_factory=True)
    py_type = self._annotation(name)
    record_name = py_field.serialization_alias \
        if Option.USE_FIELD_ALIAS in self.options and py_field.serialization_alias else name
    field_obj = RecordField(
        py_type=py_type,
        name=record_name,
        namespace=self.namespace_override,
        default=default,
        docs=py_field.description or "",
        options=self.options,
    )
    return field_obj


def _sequence_schema_handles_type(py_type) -> bool:
    """Whether this schema class can represent a given Python class"""
    py_type = pas._schemas._type_from_annotated(py_type)
    origin = pas._schemas.get_origin(py_type)
    return pas._schemas._is_class(origin,
                                  collections.abc.Sequence) or pas._schemas._is_class(origin, collections.abc.Set)


# noinspection PyProtectedMember
pas._schemas.PydanticSchema._record_field = _record_field_patched
pas._schemas.SequenceSchema.handles_type = _sequence_schema_handles_type

pas_options = Option.JSON_INDENT_2 | Option.USE_FIELD_ALIAS | Option.MILLISECONDS | Option.NO_DOC

# Enumerate all classes in the module
for name in dir(models):
    # Get the class
    cls = getattr(models, name)
    if isinstance(cls, type) and issubclass(cls, models.CustomBaseModel) and cls != models.CustomBaseModel:
        out_file = os.path.join(out_dir, f"{name}.avsc")

        if os.path.isfile(out_file):
            print(f"Schema for {name} already exists, skipping")
            continue

        namespace = models.avro_namespaces.get(cls, models.avro_namespaces["default"])
        print(f"Generating schema for {name} (namespace: {namespace})")
        schema = pas.generate(cls, namespace=namespace, options=pas_options).decode()

        with open(out_file, "w", encoding="utf-8") as f:
            f.write(schema)
