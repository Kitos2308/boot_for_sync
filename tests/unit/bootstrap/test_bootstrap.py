from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from dateutil.tz import tzutc

from app.models import MinioModel, Records

pytestmark = [
    pytest.mark.unit,
]


@pytest.mark.asyncio  # Основная функция, которая обходит бакеты из конфига и отправляет данные на синхронизацию
@pytest.mark.parametrize(
    "force_send_notification, start_page, range_of_pagination, expected_paginated_data_s3",
    [
        (
            True,  # При этом кейсе рассматриваем полную вычетку бакета не обращая внимание на маркер
            "",
            2,
            [{
                'Key': '(kCQ(qLU/101.(4VVd50rQxaVWk9a.rnd',
                'LastModified': datetime(2022, 7, 12, 12, 15, 45, 125000, tzinfo=tzutc()),
                'ETag': '"8d8c06d8fd3f5e84baba943d826e4f66-2"',
                'Size': 10417400,
                'StorageClass': 'STANDARD',
                'Owner': {
                    'DisplayName': '',
                    'ID': '02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4'
                }
            }, {
                'Key': '(kCQ(qLU/103.savX3RJdzFj17c0i.rnd',
                'LastModified': datetime(2022, 7, 1, 15, 3, 6, 261000, tzinfo=tzutc()),
                'ETag': '"09d727e16a2af9c1ab1d5354579b791b"',
                'Size': 1715528,
                'StorageClass': 'STANDARD',
                'Owner': {
                    'DisplayName': '',
                    'ID': '02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4'
                }
            }],
        ),
        (
            False,  # При этом кейсе вычитываем оставшееся данные с второй страницы
            "second_page",
            1,
            [{
                'Key': '(kCQ(qLU/101.(4VVd50rQxaVWk9a.rnd',
                'LastModified': datetime(2022, 7, 12, 12, 15, 45, 125000, tzinfo=tzutc()),
                'ETag': '"8d8c06d8fd3f5e84baba943d826e4f66-2"',
                'Size': 10417400,
                'StorageClass': 'STANDARD',
                'Owner': {
                    'DisplayName': '',
                    'ID': '02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4'
                }
            }, {
                'Key': '(kCQ(qLU/103.savX3RJdzFj17c0i.rnd',
                'LastModified': datetime(2022, 7, 1, 15, 3, 6, 261000, tzinfo=tzutc()),
                'ETag': '"09d727e16a2af9c1ab1d5354579b791b"',
                'Size': 1715528,
                'StorageClass': 'STANDARD',
                'Owner': {
                    'DisplayName': '',
                    'ID': '02d6176db174dc93cb1b899f7c6078f08654445fe8cf1b6ce98d8855f66bdbf4'
                }
            }],
        ),
        (
            True,  # При этом кейсе количество отправленных данных на синхронизацию должно быть = 0
            "",
            2,
            [],
        ),
    ])
async def test_run_bootstrap(
    app_config,
    mocked_control_bootstrap,
    mocked_control_bootstrap_manager,
    mocked_bootstrap,
    mocked_s3,
    range_of_pagination,
    pagination,
    expected_paginated_data_s3,
    force_send_notification,
    start_page,
):
    # Мокаем методы менеджеров контроля из app.managers.control -> ControlBootstrapManager и ControlBootstrap
    mocked_control_bootstrap_manager.get_control.return_value = mocked_control_bootstrap
    mocked_control_bootstrap_manager.clear_all = AsyncMock(return_value="ok")
    mocked_control_bootstrap.get_start_page = AsyncMock(return_value=start_page)
    mocked_control_bootstrap.write_current_page = AsyncMock(return_value="ok")

    # Мокаем вызовы для клиента s3
    mocked_s3._paginator = MagicMock()
    mocked_s3._paginator.paginate = MagicMock()
    mocked_s3._paginator.paginate.side_effect = [
        pagination(range(range_of_pagination)),
        pagination(range(range_of_pagination)),
    ]
    mocked_s3._paginator_config = {}
    mocked_s3._get_marker.return_value = start_page
    mocked_s3._get_data.return_value = expected_paginated_data_s3

    # Добавляем замоканных клиентов и менеджеров контроля в мок для bootstrap
    mocked_bootstrap._s3_client = mocked_s3
    mocked_bootstrap._control_bootstrap = mocked_control_bootstrap_manager
    mocked_bootstrap._control_bootstrap.get_start_page = mocked_control_bootstrap
    mocked_bootstrap._control_bootstrap.write_current_page = mocked_control_bootstrap
    mocked_bootstrap._model_minio = MinioModel
    mocked_bootstrap._model_records = Records
    mocked_bootstrap._topic = "minio"
    mocked_bootstrap._client = AsyncMock(send=AsyncMock(return_value="send"))

    # Добавление мока конфига для bootstrap
    mocked_bootstrap._config = app_config

    # Запускаем обход бакетов в s3 и отправление на синхронизацию
    await mocked_bootstrap.run_bootstrap()

    # Проверка, что получение менеджера контроля было получено для каждого бакета из конфига
    assert mocked_bootstrap._control_bootstrap.get_control.call_count == len(app_config.bucket_names)

    # Проверка маркера страницы s3 на каждый бакет
    assert mocked_control_bootstrap.get_start_page.call_count == len(app_config.bucket_names)

    # Запись маркера страницы s3 производится на каждой итерации пагинатора
    assert mocked_control_bootstrap.write_current_page.call_count == len(app_config.bucket_names) * range_of_pagination

    # Проверим количество отправленных сообщений на синхронизацию
    assert mocked_bootstrap._client.send.call_count == len(expected_paginated_data_s3) * range_of_pagination * len(
        app_config.bucket_names)

    # Проверим, что очистка маркеров страниц s3 вызовется только один раз
    assert mocked_bootstrap._control_bootstrap.clear_all.call_count == 1
