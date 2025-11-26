from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from obstore.store import ObjectStore, from_url
from pydantic import AnyUrl, ConfigDict, Field

from . import bound

if TYPE_CHECKING:
    from obstore.store import ClientConfig

_field = Field(description="Object store URL", examples=["s3://bucket"])

ObjectMeta = bound.ObjectMeta


def _extend_schema(parent_config: ConfigDict):
    """Create a json_schema_extra callable that extends parent examples"""
    parent_extra = parent_config.get("json_schema_extra")

    def schema_extra(schema: dict[str, Any]) -> None:
        # Apply parent's json_schema_extra first if it's a callable
        if callable(parent_extra):
            parent_extra(schema)  # pyright: ignore[reportCallIssue]
        # Or merge if it's a dict
        elif isinstance(parent_extra, dict):
            schema.update(parent_extra)

        # Now add our url to examples
        examples = schema.get("examples", [])
        for example in examples:
            example["url"] = "s3://bucket"

    return schema_extra


def _extend_model_config(config: ConfigDict) -> ConfigDict:
    return ConfigDict(**{**config, "json_schema_extra": _extend_schema(config)})


class CopyRequest(bound.CopyRequest):
    model_config = _extend_model_config(bound.CopyRequest.model_config)

    url: AnyUrl = _field


class DeleteRequest(bound.DeleteRequest):
    model_config = _extend_model_config(bound.DeleteRequest.model_config)

    url: AnyUrl = _field


class GetRequest(bound.GetRequest):
    model_config = _extend_model_config(bound.GetRequest.model_config)

    url: AnyUrl = _field


GetResponse = bound.GetResponse


class HeadRequest(bound.HeadRequest):
    model_config = _extend_model_config(bound.HeadRequest.model_config)

    url: AnyUrl = _field


class ListRequest(bound.ListRequest):
    model_config = _extend_model_config(bound.ListRequest.model_config)

    url: AnyUrl = _field


ListResponse = bound.ListResponse


class PutRequest(bound.PutRequest):
    model_config = _extend_model_config(bound.PutRequest.model_config)

    url: AnyUrl = _field


PutResponse = bound.PutResponse


class RenameRequest(bound.RenameRequest):
    model_config = _extend_model_config(bound.RenameRequest.model_config)

    url: AnyUrl = _field


class SignRequest(bound.SignRequest):
    model_config = _extend_model_config(bound.SignRequest.model_config)

    url: AnyUrl = _field


SignResponse = bound.SignResponse


class ObjectStoreFactory(Protocol):
    def create(self, url: AnyUrl) -> ObjectStore: ...


class DefaultObjectStoreFactory:
    def __init__(self, client_options: ClientConfig | None = None):
        self.client_options = client_options

    def create(self, url: AnyUrl) -> ObjectStore:
        return from_url(str(url), client_options=self.client_options)


class Executor:
    def __init__(self, factory: ObjectStoreFactory):
        self.factory = factory

    async def copy(self, request: CopyRequest):
        """Copy an object from one path to another in the same object store."""

        store = self.factory.create(request.url)

        return await bound.copy(store, request)

    async def delete(self, request: DeleteRequest):
        store = self.factory.create(request.url)

        return await bound.delete(store, request)

    async def get(self, request: GetRequest) -> GetResponse:
        store = self.factory.create(request.url)

        return await bound.get(store, request)

    async def head(self, request: HeadRequest) -> ObjectMeta:
        store = self.factory.create(request.url)

        return await bound.head(store, request)

    async def list(self, request: ListRequest) -> ListResponse:
        store = self.factory.create(request.url)

        return await bound._list(store, request)

    async def put(self, request: PutRequest) -> PutResponse:
        store = self.factory.create(request.url)

        return await bound.put(store, request)

    async def rename(self, request: RenameRequest):
        store = self.factory.create(request.url)

        return await bound.rename(store, request)

    async def sign(self, request: SignRequest) -> SignResponse:
        store = self.factory.create(request.url)

        if isinstance(store, bound.SignCapableStore):
            return await bound.sign(store, request)

        raise ValueError(f"Store {store} is not signable")
