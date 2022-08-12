from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

pytestmark = [
    pytest.mark.unit,
]


@pytest.mark.asyncio
async def test_generate_s3(
    mocked_s3,
    app_config,
    pagination,
):
    # Мокаем методы для app.clients.s3.S3Client
    mocked_s3._get_marker.return_value = "test_marker"
    mocked_s3._get_data.return_value = "get_s3_data"
    mocked_s3._paginator_config = {}
    mocked_s3._paginator = MagicMock()
    mocked_s3._paginator.paginate = MagicMock()
    mocked_s3._paginator.paginate.side_effect = [
        pagination(range(2)),  # где колличество страниц при вычетке с бакета
        pagination(range(2)),
    ]
    # Проверка на количество вызовов получения данных из бакетов
    for bucket_name in app_config.bucket_names:
        async for result in mocked_s3.generate(bucket_name):
            # Проверка что пагинейт вызван с корректнми параметрами
            mocked_s3._paginator.paginate.assert_called_with(
                Bucket=bucket_name,
                Prefix="",
                PaginationConfig={"StartingToken": ""},
            )
    assert mocked_s3._get_data.call_count == len(app_config.bucket_names) * 2
    assert mocked_s3._get_marker.call_count == len(app_config.bucket_names) * 2


@pytest.mark.asyncio
async def test_connect_s3(mocked_s3):
    # Проверка на коннект s3
    mocked_s3._session_client = AsyncMock(__aenter__=AsyncMock(return_value=Mock(get_paginator=Mock(return_value=""))))
    await mocked_s3.connect()
    assert mocked_s3._session_client.__aenter__.call_count == 1
    mocked_s3._client.get_paginator.assert_called_with("list_objects")


@pytest.mark.asyncio
async def test_disconnect_s3(mocked_s3):
    # Поверка на дисконект от s3
    mocked_s3._session_client = AsyncMock(__aexit__=AsyncMock(return_value=None))
    await mocked_s3.disconnect()
    assert mocked_s3._session_client.__aexit__.call_count == 1
