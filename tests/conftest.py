from unittest.mock import AsyncMock, PropertyMock

import pytest
from platform_config.models import Log, SentryConfig
from pytest_mock import MockerFixture

from app.clients.s3 import S3Client
from app.config import AppConfig, PrometheusConfig, RedisConfig, StorageConfig, StorageType
from app.managers.control import ControlBootstrap, ControlBootstrapManager
from app.services.bootstrap import BootstrapService


@pytest.fixture()
def app_config():
    # Фикстура конфига приложения bootstrap
    return AppConfig(
        sentry=SentryConfig(
            dsn='',
            environment='',
            log_level='WARNING',
        ),
        prometheus=PrometheusConfig(
            port=80,
            enable=False,
        ),
        redis=RedisConfig(host='', port=80, db=0),
        log=Log(
            level='',
            type='',
        ),
        kafka='',
        minio=StorageConfig(
            type=StorageType.minio,
            host='',
            port=80,
            access_key='',
            secret_key='',
            notify_topic='',
        ),
        bucket_names=["bucket1", "bucket2"],
        force_send_notification=True,
        pagination_page_size=5)


@pytest.fixture()  # Фикстура для app.managers.control.ControlBootstrap
def mocked_control_bootstrap(mocker: MockerFixture,):

    mocker.patch.object(ControlBootstrap, "__init__", lambda self: None)
    properties_to_mock = [
        "get_start_page",  # Возвращает маркер страницы из s3 для понимания с какой страницы читать
        "write_current_page"  # Запись маркера страницы s3 в redis
    ]
    for prop in properties_to_mock:
        mocker.patch.object(ControlBootstrap, prop, new_callable=AsyncMock)

    return ControlBootstrap()


@pytest.fixture()  # Фикстура для app.managers.control.ControlBootstrapManager
def mocked_control_bootstrap_manager(
    mocker: MockerFixture,
    app_config,
):

    redis_mock = mocker.Mock()
    mocker.patch.object(ControlBootstrapManager, "__init__", lambda self, redis, force_send_notification: None)
    properties_to_mock = [
        "get_control",  # Возвращает объект из app.mangers.control.ControlBootstrap для контроля маркера в s3
    ]
    for prop in properties_to_mock:
        mocker.patch.object(ControlBootstrapManager, prop, new_callable=PropertyMock)

    return ControlBootstrapManager(redis=redis_mock, force_send_notification=app_config.force_send_notification)


@pytest.fixture()  # Фикстура app.services.bootstrap.BootstrapService
def mocked_bootstrap(mocker: MockerFixture,):
    mocker.patch.object(BootstrapService, "__init__", lambda self: None)
    return BootstrapService()


@pytest.fixture()  # Фикстура app.clients.s3.S3Client
def mocked_s3(
    mocker: MockerFixture,
    app_config,
):
    mocker.patch.object(S3Client, "__init__", lambda self, host, port, secret_key, access_key, items_per_page: None)
    properties_to_mock = [
        "_get_marker",  # Маркер страницы с которой начинаем считывать объекты в s3
        "_get_data",  # Данные которые отправляем в кафку для синхронизации
    ]
    for prop in properties_to_mock:
        mocker.patch.object(S3Client, prop, new_callable=PropertyMock)

    return S3Client(
        host=app_config.minio.host,
        port=app_config.minio.port,
        secret_key=app_config.minio.secret_key,
        access_key=app_config.minio.access_key,
        items_per_page=app_config.pagination_page_size)


@pytest.fixture()
def pagination():
    return AsyncIterator


class AsyncIterator:

    def __init__(self, seq):
        self.iter = iter(seq)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopAsyncIteration
