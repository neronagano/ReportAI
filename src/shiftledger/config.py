from enum import StrEnum
from functools import lru_cache

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url


class Environment(StrEnum):
    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", extra="ignore", env_ignore_empty=True, hide_input_in_errors=True
    )

    app_env: Environment = Environment.DEVELOPMENT
    log_level: str = "INFO"
    app_secret_key: SecretStr = Field(min_length=32)
    allowed_hosts: list[str] = ["localhost", "127.0.0.1", "api", "testserver"]
    cors_origins: list[str] = []
    metrics_token: SecretStr | None = None
    token_ttl_minutes: int = Field(default=30, ge=1, le=1440)
    login_requests_per_minute: int = Field(default=20, ge=1, le=1000)
    database_url: str
    database_pool_size: int = Field(default=5, ge=1, le=100)
    database_max_overflow: int = Field(default=5, ge=0, le=100)
    redis_url: str
    template_cache_ttl_seconds: int = Field(default=3600, ge=1)

    kafka_bootstrap_servers: str
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str = "PLAIN"
    kafka_sasl_username: str | None = None
    kafka_sasl_password: SecretStr | None = None
    kafka_ssl_cafile: str | None = None
    kafka_report_topic: str = "shiftledger.report-submitted.v1"
    kafka_completed_topic: str = "shiftledger.reconciliation-completed.v1"
    kafka_dlq_topic: str = "shiftledger.reconciliation-dlq.v1"
    kafka_consumer_group: str = "shiftledger-reconciliation-v1"
    kafka_topic_partitions: int = Field(default=3, ge=1)
    kafka_replication_factor: int = Field(default=1, ge=1)
    worker_max_attempts: int = Field(default=5, ge=1, le=10)
    outbox_poll_seconds: float = Field(default=1.0, gt=0, le=30)

    s3_endpoint_url: str | None = None
    s3_region: str = "us-east-1"
    s3_bucket: str = "shiftledger-receipts"
    s3_access_key_id: str | None = None
    s3_secret_access_key: SecretStr | None = None
    upload_max_bytes: int = Field(default=8 * 1024 * 1024, ge=1024, le=32 * 1024 * 1024)
    upload_max_pixels: int = Field(default=24_000_000, ge=1)
    revenue_change_threshold: float = Field(default=0.2, gt=0, le=10)
    seed_admin_password: SecretStr | None = None
    seed_employee_password: SecretStr | None = None

    @model_validator(mode="after")
    def validate_production(self) -> "Settings":
        if not self.database_url.startswith("postgresql+asyncpg://"):
            raise ValueError("DATABASE_URL must use postgresql+asyncpg")
        if self.kafka_security_protocol not in {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}:
            raise ValueError("Unsupported Kafka security protocol")
        if self.kafka_security_protocol.startswith("SASL") and (
            not self.kafka_sasl_username or not self.kafka_sasl_password
        ):
            raise ValueError("SASL requires a Kafka username and password")
        if self.app_env == Environment.PRODUCTION:
            secret = self.app_secret_key.get_secret_value()
            if "change-me" in secret or "development" in secret or len(set(secret)) < 12:
                raise ValueError("Production requires a randomly generated APP_SECRET_KEY")
            if "*" in self.allowed_hosts or not self.allowed_hosts:
                raise ValueError("Production requires explicit ALLOWED_HOSTS")
            if self.kafka_security_protocol not in {"SSL", "SASL_SSL"}:
                raise ValueError("Production requires encrypted Kafka connections")
            if self.kafka_replication_factor < 3:
                raise ValueError("Production requires KAFKA_REPLICATION_FACTOR >= 3")
            if make_url(self.database_url).query.get("ssl") not in {"verify-full", "verify-ca"}:
                raise ValueError("Production PostgreSQL connections must verify TLS certificates")
            if not self.redis_url.startswith("rediss://"):
                raise ValueError("Production Redis connections must use TLS")
            if self.s3_endpoint_url and not self.s3_endpoint_url.startswith("https://"):
                raise ValueError("Production S3 endpoints must use HTTPS")
            if self.seed_admin_password or self.seed_employee_password:
                raise ValueError("Demo credentials must be removed in production")
            if self.metrics_token is None or len(self.metrics_token.get_secret_value()) < 32:
                raise ValueError(
                    "Production requires a separate METRICS_TOKEN with at least 32 characters"
                )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
