from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    environment: Literal["development", "test", "production"] = "development"
    auto_create_db: bool = True

    jwt_secret: str = "dev-secret"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = Field(60, ge=1, le=1440)

    database_url: str = "sqlite+aiosqlite:///./chat.db"
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str = "redis://localhost:6379/1"
    celery_result_backend: str = "redis://localhost:6379/2"
    redis_namespace: str = Field("realtime-chat", pattern=r"^[a-zA-Z0-9_-]+$")

    @field_validator("database_url")
    @classmethod
    def async_database_url(cls, value: str) -> str:
        for prefix in ("postgres://", "postgresql://"):
            if value.startswith(prefix):
                return "postgresql+asyncpg://" + value[len(prefix) :]
        return value

    @model_validator(mode="after")
    def production_settings(self):
        if self.environment == "production":
            if len(self.jwt_secret) < 32 or self.jwt_secret.startswith("dev-secret"):
                raise ValueError(
                    "Production requires a random JWT_SECRET of at least 32 characters"
                )
            if self.auto_create_db:
                raise ValueError(
                    "Set AUTO_CREATE_DB=false and run Alembic migrations in production"
                )
        if self.jwt_algorithm != "HS256":
            raise ValueError("This service supports HS256 signing only")
        return self


settings = Settings()
