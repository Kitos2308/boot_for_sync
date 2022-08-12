import logging
from datetime import datetime
from typing import Any

import sentry_sdk
from prometheus_client import start_http_server
from sentry_sdk.integrations.logging import LoggingIntegration

from app.clients.kafka import KafkaProducer
from app.clients.redis import Redis
from app.clients.s3 import S3Client
from app.config import AppConfig, StorageType
from app.managers.control import ControlBootstrapManager
from app.metrics import BootstrapMetrics
from app.models import (S3, BucketMinioModel, MinioModel, MinioModelOBS, Object, OwnerIdentity, Records, RecordsOBS,
                        RequestParameters, ResponseElements, Source, UserIdentity, UserMetadata)


class BootstrapService:

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        sentry_sdk.init(
            dsn=self._config.sentry.dsn,
            environment=self._config.sentry.environment,
            integrations=[LoggingIntegration(level=self._config.sentry.log_level)],
            traces_sample_rate=1.0)
        self._client: KafkaProducer = KafkaProducer(bootstrap_servers=[self._config.kafka])
        self._redis = Redis(self._config.redis.host, self._config.redis.port, self._config.redis.db)
        self._control_bootstrap = ControlBootstrapManager(
            redis=self._redis, force_send_notification=self._config.force_send_notification)
        self._s3_client = S3Client(
            host=self._config.minio.host,
            port=self._config.minio.port,
            secret_key=self._config.minio.secret_key,
            access_key=self._config.minio.access_key,
            items_per_page=self._config.pagination_page_size)
        self._topic = self._config.minio.notify_topic
        self._model_records: Any = Records if self._config.minio.type == StorageType.minio else RecordsOBS
        self._model_minio: Any = MinioModel if self._config.minio.type == StorageType.minio else MinioModelOBS
        if self._config.prometheus.enable:
            start_http_server(self._config.prometheus.port)

    async def run_bootstrap(self) -> None:  # Обход бакетов из app_config и отправка данных в kafka
        logging.info(f"bootstrap started with bucket names {self._config.bucket_names}")
        for bucket_name in self._config.bucket_names:
            control = self._control_bootstrap.get_control(bucket_name)
            start_page = await control.get_start_page()
            logging.info(f"current bucket: {bucket_name} begin with page: {start_page}")
            async for data, marker in self._s3_client.generate(bucket_name, start_page):
                BootstrapMetrics.collect_requests_metrics(self._config.minio.type)
                await control.write_current_page(marker)
                for item in data:
                    message = self.prepare_data_for_send(
                        model_records=self._model_records,
                        model_minio=self._model_minio,
                        bucket_name=bucket_name,
                        item=item)
                    if message:
                        await self._client.send(self._topic, value=message)
                        logging.info(f"sent to producer {item}")
                        BootstrapMetrics.collect_events_count_bucket_metrics(bucket_name)
        await self._control_bootstrap.clear_all()
        logging.info("bootstrap finished")

    async def start(self) -> None:
        await self._redis.connect()
        await self._client.connect()
        await self._s3_client.connect()
        await self.run_bootstrap()

    async def stop(self) -> None:
        await self._client.disconnect()
        await self._s3_client.disconnect()
        await self._redis.disconnect()

    @staticmethod
    def prepare_data_for_send(model_records: Any, model_minio: Any, bucket_name: str, item: Any) -> bytes | None:
        message = None
        try:
            message = model_records(
                Key=f'{bucket_name}/{item.get("Key")}',
                Records=[
                    model_minio(
                        eventTime=datetime.now(),
                        userIdentity=UserIdentity(),
                        requestParameters=RequestParameters(),
                        responseElements=ResponseElements(),
                        s3=S3(
                            bucket=BucketMinioModel(
                                name=bucket_name,
                                ownerIdentity=OwnerIdentity(),
                            ),
                            object=Object(
                                key=item.get("Key"),
                                size=item.get("Size"),
                                eTag=item.get("ETag"),
                                userMetadata=UserMetadata())),
                        source=Source(),
                    )
                ]).json().encode()
        except Exception as e:
            logging.exception(e)
        return message
