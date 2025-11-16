import restate

from .store import (
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
    SignCapableStore,
    SignRequest,
    SignResponse,
    Store,
)


def create_service(
    store: Store,
    service_name: str = "Obstore",
) -> restate.Service:
    service = restate.Service(service_name)

    register_service(store, service)

    return service


def register_service(store: Store, service: restate.Service | None = None):
    service = service or restate.Service("Obstore")

    @service.handler()
    async def copy(ctx: restate.Context, request: CopyRequest):
        """Copy an object."""
        await ctx.run_typed("copy", store.copy, request=request)

    @service.handler()
    async def delete(ctx: restate.Context, request: DeleteRequest):
        await ctx.run_typed("delete", store.delete, request=request)

    @service.handler()
    async def get(ctx: restate.Context, request: GetRequest) -> GetResponse:
        return await ctx.run_typed("get", store.get, request=request)

    @service.handler()
    async def head(ctx: restate.Context, request: HeadRequest) -> ObjectMeta:
        return await ctx.run_typed("head", store.head, request=request)

    @service.handler()
    async def list(ctx: restate.Context, request: ListRequest) -> ListResponse:
        return await ctx.run_typed("list", store.list, request=request)

    @service.handler()
    async def put(ctx: restate.Context, request: PutRequest) -> PutResponse:
        return await ctx.run_typed("put", store.put, request=request)

    @service.handler()
    async def rename(ctx: restate.Context, request: RenameRequest):
        _ = await ctx.run_typed("rename", store.rename, request=request)

    if isinstance(store, SignCapableStore):

        @service.handler()
        async def sign(ctx: restate.Context, request: SignRequest) -> SignResponse:
            return await ctx.run_typed(
                "sync",
                store.sign,
                request=request,
            )
