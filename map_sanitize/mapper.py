import typing as t

from singer_sdk import typing as th
from singer_sdk._singerlib.messages import Message, RecordMessage, SchemaMessage
import singer_sdk._singerlib as singer

from pydash.strings import snake_case, kebab_case

from map_sanitize.base import BasicMapper


class ConfigKeys:
    PROPERTIES = "properties"

    # --- Sanitize schemas/keys
    KEYS_SNAKE_CASE = "keys_snake_case"
    KEYS_KEBAB_CASE = "keys_kebab_case"

    # --- Sanitize values
    VALUES_TRIM_STRINGS = "values_trim_strings"
    VALUES_REMOVE_EMPTY_STRINGS = "values_remove_empty_strings"
    VALUES_SIMPLFIY_WHITESPACE = "values_simplify_whitespace"


ConfigKeyType = tuple(
    value for key, value in ConfigKeys.__dict__.items() if not key.startswith("__")
)


class SanitizeMapper(BasicMapper):
    name = "map-sanitize"

    config_jsonschema = th.PropertiesList(
        th.Property(
            ConfigKeys.PROPERTIES,
            th.ArrayType,
            default=None,
            description="TODO",
        ),
        th.Property(
            ConfigKeys.KEYS_SNAKE_CASE,
            th.BooleanType,
            default=False,
            description="Snake case for keys",
        ),
        th.Property(
            ConfigKeys.KEYS_KEBAB_CASE,
            th.BooleanType,
            default=False,
            description="Kebab case for keys",
        ),
        th.Property(
            ConfigKeys.VALUES_TRIM_STRINGS,
            th.BooleanType,
            default=False,
            description="Trim whitespace at start and end of strings",
        ),
        th.Property(
            ConfigKeys.VALUES_REMOVE_EMPTY_STRINGS,
            th.BooleanType,
            default=False,
            description="Prefer None to empty string ''",
        ),
        th.Property(
            ConfigKeys.VALUES_SIMPLFIY_WHITESPACE,
            th.BooleanType,
            default=False,
            description="Change newlines and multiple consecutive spaces to a single space",
        ),
    ).to_dict()

    def _process_key(self, key: str):
        new_key = key
        if self.config.get(ConfigKeys.KEYS_SNAKE_CASE):
            new_key = snake_case(new_key)
        if self.config.get(ConfigKeys.KEYS_KEBAB_CASE):
            new_key = kebab_case(new_key)
        return new_key

    def _process_value(self, value: t.Any):
        new_value = value
        if self.config.get(ConfigKeys.VALUES_TRIM_STRINGS) and isinstance(
            new_value, str
        ):
            new_value = new_value.strip()
        if (
            self.config.get(ConfigKeys.VALUES_REMOVE_EMPTY_STRINGS)
            and isinstance(new_value, str)
            and len(new_value) == 0
        ):
            new_value = None
        if self.config.get(ConfigKeys.VALUES_SIMPLFIY_WHITESPACE) and isinstance(
            new_value, str
        ):
            new_value = " ".join(new_value.split(r"\s+"))

        return new_value

    def _process_property_def(self, key, definition):
        new_key = self._process_key(key)
        return new_key, definition

    def map_schema_message(self, message_dict: dict) -> t.Iterable[Message]:
        for result in super().map_schema_message(message_dict):
            self.logger.info("SCHEMA MSG")
            self.logger.info(result)

            props: dict[str, t.Any] = result.schema["properties"]
            required: list[str] = result.schema["required"] or []
            key_props: list[str] | None = (
                list(result.key_properties)
                if result.key_properties is not None
                else None
            )
            # TODO schema.bookmark_properties?

            # Overwrite schema.properties rather than editing in-place
            result.schema["properties"] = dict(
                self._process_property_def(k, v) for k, v in props.items()
            )
            result.schema["required"] = [self._process_key(k) for k in required]
            result.key_properties = (
                [self._process_key(k) for k in key_props] if key_props else None
            )

            yield result

    def map_record_message(self, message_dict: dict) -> t.Iterable[RecordMessage]:
        for result in super().map_record_message(message_dict):

            original = result.record
            new_record = {
                self._process_key(k): self._process_value(v)
                for k, v in original.items()
            }

            # Simply overwrite record
            result.record = new_record

            yield result
