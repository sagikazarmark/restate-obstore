import obstore.store
import restate
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from . import create_service, create_store


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_nested_delimiter="__")  # pyright: ignore[reportUnannotatedClassAttribute]

    object_store_url: str
    allow_http: bool = False
    identity_keys: list[str] = Field(alias="restate_identity_keys", default=[])


settings = Settings()  # pyright: ignore[reportCallIssue]

settings.object_store_url.startswith("http://")

store = create_store(
    obstore.store.from_url(
        settings.object_store_url,
        client_options={"allow_http": settings.allow_http},
    )
)

service = create_service(store)

app = restate.app(services=[service], identity_keys=settings.identity_keys)
