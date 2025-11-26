from typing import overload

import restate

from . import bound, unbound


@overload
def create_service(
    executor: bound.Executor,
    service_name: str = "Obstore",
) -> restate.Service: ...


@overload
def create_service(
    executor: unbound.Executor,
    service_name: str = "Obstore",
) -> restate.Service: ...


def create_service(
    executor: bound.Executor | unbound.Executor,
    service_name: str = "Obstore",
) -> restate.Service:
    service = restate.Service(service_name)

    register_service(executor, service)

    return service


@overload
def register_service(
    executor: bound.Executor,
    service: restate.Service,
): ...


@overload
def register_service(
    executor: unbound.Executor,
    service: restate.Service,
): ...


def register_service(
    executor: bound.Executor | unbound.Executor,
    service: restate.Service,
):
    if isinstance(executor, bound.Executor):
        return register_bound_service(executor, service)

    elif isinstance(executor, unbound.Executor):
        return register_unbound_service(executor, service)


def register_bound_service(executor: bound.Executor, service: restate.Service):
    from .bound import (
        CopyRequest,
        DeleteRequest,
        GetRequest,
        GetResponse,
        HeadRequest,
        ListRequest,
        ListResponse,
        ObjectMeta,
        PutRequest,
        PutResponse,
        RenameRequest,
        SignRequest,
        SignResponse,
    )

    @service.handler()
    async def copy(ctx: restate.Context, request: CopyRequest):
        """Copy an object from one path to another in the same object store."""

        return await ctx.run_typed("copy", executor.copy, request=request)

    @service.handler()
    async def delete(ctx: restate.Context, request: DeleteRequest):
        """Delete the object at the specified location(s)."""

        return await ctx.run_typed("delete", executor.delete, request=request)

    @service.handler()
    async def get(ctx: restate.Context, request: GetRequest) -> GetResponse:
        """Return the bytes that are stored at the specified location."""

        return await ctx.run_typed("get", executor.get, request=request)

    @service.handler()
    async def head(ctx: restate.Context, request: HeadRequest) -> ObjectMeta:
        """Return the metadata for the specified location."""

        return await ctx.run_typed("head", executor.head, request=request)

    @service.handler()
    async def list(ctx: restate.Context, request: ListRequest) -> ListResponse:
        """List all the objects with the given prefix."""

        return await ctx.run_typed("list", executor.list, request=request)

    @service.handler()
    async def put(ctx: restate.Context, request: PutRequest) -> PutResponse:
        """Save the provided bytes to the specified location."""

        return await ctx.run_typed("put", executor.put, request=request)

    @service.handler()
    async def rename(ctx: restate.Context, request: RenameRequest):
        """Move an object from one path to another in the same object store."""

        return await ctx.run_typed("rename", executor.rename, request=request)

    if isinstance(executor, bound.SignCapableExecutor):

        @service.handler()
        async def sign(ctx: restate.Context, request: SignRequest) -> SignResponse:
            """Create a signed URL."""

            return await ctx.run_typed(
                "sign",
                executor.sign,
                request=request,
            )


def register_unbound_service(executor: unbound.Executor, service: restate.Service):
    from .unbound import (
        CopyRequest,
        DeleteRequest,
        GetRequest,
        GetResponse,
        HeadRequest,
        ListRequest,
        ListResponse,
        ObjectMeta,
        PutRequest,
        PutResponse,
        RenameRequest,
        SignRequest,
        SignResponse,
    )

    @service.handler()
    async def copy(ctx: restate.Context, request: CopyRequest):
        """Copy an object from one path to another in the same object store."""

        return await ctx.run_typed("copy", executor.copy, request=request)

    @service.handler()
    async def delete(ctx: restate.Context, request: DeleteRequest):
        """Delete the object at the specified location(s)."""

        return await ctx.run_typed("delete", executor.delete, request=request)

    @service.handler()
    async def get(ctx: restate.Context, request: GetRequest) -> GetResponse:
        """Return the bytes that are stored at the specified location."""

        return await ctx.run_typed("get", executor.get, request=request)

    @service.handler()
    async def head(ctx: restate.Context, request: HeadRequest) -> ObjectMeta:
        """Return the metadata for the specified location."""

        return await ctx.run_typed("head", executor.head, request=request)

    @service.handler()
    async def list(ctx: restate.Context, request: ListRequest) -> ListResponse:
        """List all the objects with the given prefix."""

        return await ctx.run_typed("list", executor.list, request=request)

    @service.handler()
    async def put(ctx: restate.Context, request: PutRequest) -> PutResponse:
        """Save the provided bytes to the specified location."""

        return await ctx.run_typed("put", executor.put, request=request)

    @service.handler()
    async def rename(ctx: restate.Context, request: RenameRequest):
        """Move an object from one path to another in the same object store."""

        return await ctx.run_typed("rename", executor.rename, request=request)

    @service.handler()
    async def sign(ctx: restate.Context, request: SignRequest) -> SignResponse:
        """Create a signed URL."""

        return await ctx.run_typed(
            "sign",
            executor.sign,
            request=request,
        )
