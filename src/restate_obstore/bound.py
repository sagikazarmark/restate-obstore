from collections.abc import Sequence
from datetime import datetime, timedelta
from enum import Enum
from typing import overload

import obstore
from pydantic import BaseModel, ConfigDict, Field


class ObjectMeta(BaseModel):
    e_tag: str | None = Field(description="The unique identifier for the object")
    last_modified: datetime = Field(description="The last modified time")
    path: str = Field(description="The full path to the object")
    size: int = Field(description="The size in bytes of the object")
    version: str | None = Field(description="A version indicator for this object")


class CopyRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "from": "path/to/source",
                    "to": "path/to/destination",
                },
            ]
        }
    )

    from_: str = Field(alias="from", description="Source path")
    to: str = Field(description="Destination path")
    overwrite: bool = Field(
        default=True,
        description="Overwrite an object at the destination path if exists, otherwise fail",
    )


async def copy(store: obstore.store.ObjectStore, request: CopyRequest):
    await store.copy_async(
        request.from_,
        request.to,
        overwrite=request.overwrite,
    )


class DeleteRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "paths": ["path/to/delete1", "path/to/delete2"],
                },
            ]
        }
    )

    paths: str | Sequence[str] = Field(
        description="The path or paths within the store to delete"
    )


async def delete(store: obstore.store.ObjectStore, request: DeleteRequest):
    await store.delete_async(request.paths)


class GetRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "path": "path/to/retrieve",
                },
            ]
        }
    )

    path: str = Field(description="The path or paths within the store to retrieve")


class GetResponse(BaseModel):
    pass


async def get(store: obstore.store.ObjectStore, request: GetRequest) -> GetResponse:
    raise NotImplementedError


class HeadRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "path": "path/to/retrieve",
                },
            ]
        }
    )

    path: str = Field(description="The path or paths within the store to retrieve")


async def head(store: obstore.store.ObjectStore, request: HeadRequest) -> ObjectMeta:
    meta = await store.head_async(request.path)

    return ObjectMeta.model_validate(meta)


class ListRequest(BaseModel):
    model_config = ConfigDict(json_schema_extra={"examples": []})
    pass


class ListResponse(BaseModel):
    pass


async def _list(store: obstore.store.ObjectStore, request: ListRequest) -> ListResponse:
    raise NotImplementedError


class PutRequest(BaseModel):
    model_config = ConfigDict(json_schema_extra={"examples": []})
    pass


class PutResponse(BaseModel):
    pass


async def put(store: obstore.store.ObjectStore, request: PutRequest) -> PutResponse:
    raise NotImplementedError


class RenameRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "from": "path/to/source",
                    "to": "path/to/destination",
                },
            ]
        }
    )

    from_: str = Field(alias="from", description="Source path")
    to: str = Field(description="Destination path")
    overwrite: bool = Field(
        default=True,
        description="Overwrite an object at the destination path if exists, otherwise fail",
    )


async def rename(store: obstore.store.ObjectStore, request: RenameRequest):
    await store.rename_async(
        request.from_,
        request.to,
        overwrite=request.overwrite,
    )


class HttpMethod(str, Enum):
    GET = "GET"
    PUT = "PUT"
    POST = "POST"
    HEAD = "HEAD"
    PATCH = "PATCH"
    TRACE = "TRACE"
    DELETE = "DELETE"
    OPTIONS = "OPTIONS"
    CONNECT = "CONNECT"


class SignRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "method": "GET",
                    "paths": ["path/to/file"],
                    "expires_in": "PT1H",
                },
            ]
        }
    )

    method: HttpMethod
    paths: str | Sequence[str]
    expires_in: timedelta


class SignResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "signed": "http://127.0.0.1:9000/bucket/path/to/file?X-Amz-Algorithm=AWS4-HMAC-SHA256&...&X-Amz-Signature=eb9c529b34ce3ebb2a896c5462f62959209055f838f26df77a94919b706449b0",
                },
            ]
        }
    )

    signed: str | Sequence[str]


SignCapableStore = (
    obstore.store.AzureStore | obstore.store.GCSStore | obstore.store.S3Store
)


async def sign(
    store: SignCapableStore,
    request: SignRequest,
) -> SignResponse:
    paths = await obstore.sign_async(
        store,
        request.method.value,
        request.paths,
        request.expires_in,
    )

    return SignResponse(signed=paths)


class Executor:
    def __init__(self, store: obstore.store.ObjectStore):
        self.store: obstore.store.ObjectStore = store

    async def copy(self, request: CopyRequest):
        await copy(self.store, request)

    async def delete(self, request: DeleteRequest):
        await delete(self.store, request)

    async def get(self, request: GetRequest) -> GetResponse:
        return await get(self.store, request)

    async def head(self, request: HeadRequest) -> ObjectMeta:
        return await head(self.store, request)

    async def list(self, request: ListRequest) -> ListResponse:
        return await _list(self.store, request)

    async def put(self, request: PutRequest) -> PutResponse:
        return await put(self.store, request)

    async def rename(self, request: RenameRequest):
        await rename(self.store, request)


class SignCapableExecutor(Executor):
    def __init__(self, store: SignCapableStore):
        super().__init__(store)

        self._sign_capable_store = store

    async def sign(self, request: SignRequest) -> SignResponse:
        return await sign(self._sign_capable_store, request)


@overload
def create_executor(store: SignCapableStore) -> SignCapableExecutor: ...


@overload
def create_executor(store: obstore.store.ObjectStore) -> Executor: ...


def create_executor(store: obstore.store.ObjectStore) -> Executor | SignCapableExecutor:
    if isinstance(store, SignCapableStore):
        return SignCapableExecutor(store)

    return Executor(store)
