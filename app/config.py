import enum

from platform_config.config import BaseSettings, RootSettings
from platform_config.models import Log, RedisBaseConfig, SentryConfig


class BucketConfig(BaseSettings):
    name: str


class RedisConfig(RedisBaseConfig):
    db: int


class SyncGroup(BaseSettings):
    buckets: list[BucketConfig]


class StorageType(str, enum.Enum):
    minio = "minio"
    obs = "obs"


class StorageConfig(BaseSettings):
    type: StorageType
    host: str
    port: int
    access_key: str
    secret_key: str
    notify_topic: str


class PrometheusConfig(BaseSettings):
    port: int
    enable: bool


class AppConfig(RootSettings):
    sentry: SentryConfig
    prometheus: PrometheusConfig
    redis: RedisConfig
    log: Log
    kafka: str
    minio: StorageConfig
    bucket_names: list[str]
    force_send_notification: bool
    pagination_page_size: int

    class Config:
        yaml_file_path = "config.yaml"
