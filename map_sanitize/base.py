import typing as t

from singer_sdk._singerlib.messages import (
    ActivateVersionMessage,
    Message,
    SchemaMessage,
    StateMessage,
    RecordMessage,
)
from singer_sdk.mapper_base import InlineMapper


class BasicMapper(InlineMapper):
    """Inspired by https://github.com/MeltanoLabs/map-gpt-embeddings/blob/main/map_gpt_embeddings/sdk_fixes/mapper_base.py"""

    def map_schema_message(self, message_dict: dict) -> t.Iterable[SchemaMessage]:
        yield t.cast(SchemaMessage, SchemaMessage.from_dict(message_dict))

    def map_record_message(self, message_dict: dict) -> t.Iterable[RecordMessage]:
        yield t.cast(RecordMessage, RecordMessage.from_dict(message_dict))

    def map_state_message(self, message_dict: dict) -> t.Iterable[Message]:
        yield StateMessage.from_dict(message_dict)

    def map_activate_version_message(
        self,
        message_dict: dict,
    ) -> t.Iterable[Message]:
        yield ActivateVersionMessage.from_dict(message_dict)
