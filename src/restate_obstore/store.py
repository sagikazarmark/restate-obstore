from collections.abc import Sequence
from datetime import datetime, timedelta
from enum import Enum
from typing import overload

import obstore
from pydantic import BaseModel, Field


class ObjectMeta(BaseModel):
    e_tag: str | None = Field(description="The unique identifier for the object")
    last_modified: datetime = Field(description="The last modified time")
    path: str = Field(description="The full path to the object")
    size: int = Field(description="The size in bytes of the object")
    version: str | None = Field(description="A version indicator for this object")


class CopyRequest(BaseModel):
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
    paths: str | Sequence[str] = Field(
        description="The path or paths within the store to delete"
    )


async def delete(store: obstore.store.ObjectStore, request: DeleteRequest):
    await store.delete_async(request.paths)


class GetRequest(BaseModel):
    path: str = Field(description="The path or paths within the store to retrieve")


class GetResponse(BaseModel):
    pass


async def get(store: obstore.store.ObjectStore, request: GetRequest) -> GetResponse:
    raise NotImplementedError


class HeadRequest(BaseModel):
    path: str = Field(description="The path or paths within the store to retrieve")


async def head(store: obstore.store.ObjectStore, request: HeadRequest) -> ObjectMeta:
    meta = await store.head_async(request.path)

    return ObjectMeta.model_validate(meta)


class ListRequest(BaseModel):
    pass


class ListResponse(BaseModel):
    pass


async def _list(store: obstore.store.ObjectStore, request: ListRequest) -> ListResponse:
    raise NotImplementedError


class PutRequest(BaseModel):
    pass


class PutResponse(BaseModel):
    pass


async def put(store: obstore.store.ObjectStore, request: PutRequest) -> PutResponse:
    raise NotImplementedError


class RenameRequest(BaseModel):
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
    method: HttpMethod
    paths: str | Sequence[str]
    expires_in: timedelta


class SignResponse(BaseModel):
    signed: str | Sequence[str]


async def sign(
    store: obstore.store.AzureStore | obstore.store.GCSStore | obstore.store.S3Store,
    request: SignRequest,
) -> SignResponse:
    paths = await obstore.sign_async(
        store,
        request.method.value,
        request.paths,
        request.expires_in,
    )

    return SignResponse(signed=paths)


class Store:
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


class SignCapableStore(Store):
    def __init__(
        self,
        store: obstore.store.AzureStore
        | obstore.store.GCSStore
        | obstore.store.S3Store,
    ):
        super().__init__(store)

        self._sign_capable_store = store

    async def sign(self, request: SignRequest) -> SignResponse:
        return await sign(self._sign_capable_store, request)


@overload
def create_store(
    store: obstore.store.AzureStore | obstore.store.GCSStore | obstore.store.S3Store,
) -> SignCapableStore: ...


@overload
def create_store(store: obstore.store.ObjectStore) -> Store: ...


def create_store(
    store: obstore.store.ObjectStore,
) -> Store | SignCapableStore:
    if isinstance(
        store,
        (obstore.store.AzureStore, obstore.store.GCSStore, obstore.store.S3Store),
    ):
        return SignCapableStore(store)
    return Store(store)
