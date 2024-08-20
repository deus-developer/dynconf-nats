from pydantic import NatsDsn
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    nats_servers: NatsDsn
