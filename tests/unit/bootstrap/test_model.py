import json
from datetime import datetime
from typing import Any

import pytest

from app.models import (S3, BucketMinioModel, MinioModel, MinioModelOBS, Object, OwnerIdentity, Records, RecordsOBS,
                        RequestParameters, ResponseElements, Source, UserIdentity, UserMetadata)
from app.services.bootstrap import BootstrapService

pytestmark = [
    pytest.mark.unit,
]

bucket_name = "test_bucket"
item = {"Key": "test_key", "Size": 100, "ETag": "e2949bf3-2b03-4962-afe6-afdf71f94e17"}
date = "2022-07-11T08:24:48.063149Z"

minio_result = Records(
    Key=f'{bucket_name}/{item.get("Key")}',
    Records=[
        MinioModel(
            eventTime=datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ"),
            userIdentity=UserIdentity(),
            requestParameters=RequestParameters(),
            responseElements=ResponseElements(),
            s3=S3(
                bucket=BucketMinioModel(
                    name=bucket_name,
                    ownerIdentity=OwnerIdentity(),
                ),
                object=Object(
                    key=item.get("Key"), size=item.get("Size"), eTag=item.get("ETag"), userMetadata=UserMetadata())),
            source=Source(),
        )
    ])

obs_result = RecordsOBS(
    Key=f'{bucket_name}/{item.get("Key")}',
    Records=[
        MinioModelOBS(
            eventTime=datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ"),
            userIdentity=UserIdentity(),
            requestParameters=RequestParameters(),
            responseElements=ResponseElements(),
            s3=S3(
                bucket=BucketMinioModel(
                    name=bucket_name,
                    ownerIdentity=OwnerIdentity(),
                ),
                object=Object(
                    key=item.get("Key"), size=item.get("Size"), eTag=item.get("ETag"), userMetadata=UserMetadata())),
            source=Source(),
        )
    ])


@pytest.mark.parametrize("model_records, model_minio, bucket_name, item, date, result", [
    (Records, MinioModel, bucket_name, item, date, minio_result),
    (RecordsOBS, MinioModelOBS, bucket_name, item, date, obs_result),
])
def test_parse_obj(model_records: Any, model_minio: Any, bucket_name: str, item: Any, date: str, result: Any) -> None:
    # Проверка парсинга модели для начальной синхронизации s3 -> OBS
    tmp = json.loads(BootstrapService.prepare_data_for_send(
        model_records,
        model_minio,
        bucket_name,
        item,
    ))
    # Намеренно меняем дату для сравнения
    tmp["Records"][0]["eventTime"] = date
    assert tmp == result

    # Проверка парсинга модели для начальной синхронизации OBS -> s3
    tmp = json.loads(BootstrapService.prepare_data_for_send(
        model_records,
        model_minio,
        bucket_name,
        item,
    ))
    tmp["Records"][0]["eventTime"] = date
    assert tmp == result
