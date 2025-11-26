from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast

import obstore
import pydantic_obstore
import restate
import structlog
from pydantic import AnyUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from restate_obstore import bound, create_service, unbound

if TYPE_CHECKING:
    from obstore.store import ClientConfig


class ObstoreSettings(pydantic_obstore.Config):
    url: AnyUrl | None = None


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_nested_delimiter="__")  # pyright: ignore[reportUnannotatedClassAttribute]

    obstore: ObstoreSettings = Field(default_factory=ObstoreSettings)

    service_name: str = "Obstore"

    identity_keys: list[str] = Field(alias="restate_identity_keys", default=[])


settings = Settings()  # pyright: ignore[reportCallIssue]

# logging.basicConfig(level=logging.INFO)
structlog.stdlib.recreate_defaults(log_level=logging.INFO)

client_options: ClientConfig | None = None

if settings.obstore.client_options:
    client_options = cast(
        "ClientConfig",
        settings.obstore.client_options.model_dump(exclude_none=True),
    )

if settings.obstore.url:
    store = obstore.store.from_url(
        str(settings.obstore.url),
        client_options=client_options,
    )

    executor = bound.create_executor(store)
else:
    factory = unbound.DefaultObjectStoreFactory(client_options=client_options)
    executor = unbound.Executor(factory)

service = create_service(executor, service_name=settings.service_name)

app = restate.app(services=[service], identity_keys=settings.identity_keys)
