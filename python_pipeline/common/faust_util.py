import faust
from faust.serializers import codecs

from common import make_ssl_context, StringCodec, PydanticCodec, PydanticAvroCodec

USE_AVRO = True
if USE_AVRO:
    pac = PydanticAvroCodec("../schemas")  # TODO
else:
    pac = None


def make_app(name: str, config: dict) -> faust.App:
    """Creates a Faust application with the specified configuration."""
    # [connection] section
    connection_config = config.get("connection", {})
    # [component_name] section
    component_config = config.get(name, {})
    # [component_name.faust] section
    component_faust_config = component_config.get("faust", {})

    # Returns None if SSL is disabled / not configured
    ssl_context = make_ssl_context(config)

    codecs.register("str", StringCodec())
    codecs.register("pydantic", pac if USE_AVRO else PydanticCodec())

    return faust.App(component_config.get("app_id", "domrad-" + name),
                     broker=connection_config.get("brokers", "kafka://localhost:9092"),
                     broker_credentials=ssl_context,
                     debug=component_config.get("debug", False),
                     key_serializer="pydantic",
                     value_serializer="pydantic",
                     web_enabled=False,
                     **component_faust_config)
