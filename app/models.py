from datetime import datetime
from typing import List, Optional, Union

from pydantic import BaseModel, validator


class UserIdentity(BaseModel):
    principalId: Optional[str] = "minioadmin"


class RequestParameters(BaseModel):
    principalId: Optional[str] = "minioadmin"
    region: Optional[str] = ""
    sourceIPAddress: Optional[str] = ""


class ResponseElements(BaseModel):
    content_length: Optional[str] = ""
    x_amz_request_id: Optional[str] = ""
    x_minio_deployment_id: Optional[str] = ""
    x_minio_origin_endpoint: Optional[str] = ""

    class Config:
        fields = {
            "content_length": "content-length",
            "x_amz_request_id": "x-amz-request-id",
            "x_minio_deployment_id": "x-minio-deployment-id",
            "x_minio_origin_endpoint": "x-minio-origin-endpoint"
        }


class OwnerIdentity(BaseModel):
    principalId: Optional[str] = "minioadmin"


class BucketMinioModel(BaseModel):
    name: str
    ownerIdentity: Optional[OwnerIdentity]
    arn: Optional[str] = ""


class UserMetadata(BaseModel):
    content_type: Optional[str] = ""

    class Config:
        fields = {"content_type": "content-type"}


class Object(BaseModel):
    key: str
    size: int
    eTag: str
    contentType: Optional[str] = ""
    userMetadata: UserMetadata

    @validator("eTag", pre=True)
    def validate_phone(cls, eTag: str) -> Union[str, None]:
        if eTag is not None:
            return eTag.replace("\"", "")
        else:
            return None


class S3(BaseModel):
    s3SchemaVersion: Optional[str] = ""
    configurationId: Optional[str] = ""
    bucket: BucketMinioModel
    object: Object
    sequencer: Optional[str] = ""


class Source(BaseModel):
    host: Optional[str] = ""
    port: Optional[str] = ""
    userAgent: Optional[str] = ""


class MinioModel(BaseModel):
    eventVersion: Optional[str] = "2.0"
    eventSource: Optional[str] = "minio:s3"
    awsRegion: Optional[str] = ""
    eventTime: str
    eventName: Optional[str] = "s3:ObjectCreated:Put"
    userIdentity: UserIdentity
    requestParameters: RequestParameters
    responseElements: ResponseElements
    s3: S3
    source: Source

    @validator("eventTime", pre=True)
    def validate_time(cls, eventTime: datetime) -> Union[str, None]:
        if eventTime is not None:
            return datetime.strftime(eventTime, "%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            return None


class MinioModelOBS(BaseModel):
    eventVersion: Optional[str] = "3.0"
    eventSource: Optional[str] = "OBS"
    awsRegion: Optional[str] = ""
    eventTime: str
    eventName: Optional[str] = "ObjectCreated:Put"
    userIdentity: UserIdentity
    requestParameters: RequestParameters
    responseElements: ResponseElements
    obs: S3
    source: Source

    class Config:
        fields = {"obs": "s3"}

    @validator("eventTime", pre=True)
    def validate_time(cls, eventTime: datetime) -> Union[str, None]:
        if eventTime is not None:
            return datetime.strftime(eventTime, "%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            return None


class Records(BaseModel):
    EventName: Optional[str] = "s3:ObjectCreated:Put"
    Key: str
    Records: List[MinioModel]


class RecordsOBS(BaseModel):
    EventName: Optional[str] = "ObjectCreated:Put"
    Key: str
    Records: List[MinioModelOBS]
