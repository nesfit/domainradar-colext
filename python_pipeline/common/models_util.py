from io import BytesIO
from typing import Type, TypeVar

from avro.io import DatumReader, BinaryDecoder
from pydantic import ValidationError

from .faust_util import pac
from .util import logger

TModel = TypeVar('TModel')


def ensure_model(model_class: Type[TModel], data: dict | bytes | None) -> TModel | None:
    if data is None:
        return None

    if isinstance(data, bytes):
        schema = pac.get_schema(model_class)
        with BytesIO(data) as bio:
            data = DatumReader(schema, schema).read(BinaryDecoder(bio))

    try:
        return model_class.model_validate(data)
    except ValidationError as e:
        logger.warning("Error validating model", exc_info=e,
                       extra={"input_data": data, "model": model_class.__name__})
        return None
    except Exception as e:
        logger.error("Error validating model", exc_info=e,
                     extra={"input_data": data, "model": model_class.__name__})
        return None
