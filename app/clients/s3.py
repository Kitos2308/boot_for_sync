from typing import AsyncGenerator, Any
from aiobotocore.session import get_session
from aiobotocore.client import AioBaseClient
from aiobotocore.paginate import AioPaginator


class S3Client:

    def __init__(
        self,
        *,
        host: str,
        port: int,
        secret_key: str,
        access_key: str,
        items_per_page: int = 10,
    ) -> None:
        self._host = host
        self._port = port
        self._secret_key = secret_key
        self._access_key = access_key
        self._items_per_page = items_per_page

        self._paginator_config = {
            "PageSize": self._items_per_page,
            "StartingToken": "",
        }

        self._session = get_session()
        self._session_client = self._session.create_client(
            "s3",
            region_name="",
            endpoint_url=f"http://{self._host}:{self._port}",
            aws_secret_access_key=self._secret_key,
            aws_access_key_id=self._access_key)
        self._client: AioBaseClient
        self._paginator: AioPaginator

    async def connect(self) -> None:
        self._client = await self._session_client.__aenter__()
        self._paginator = self._client.get_paginator("list_objects")

    async def disconnect(self) -> None:
        await self._session_client.__aexit__(None, None, None)

    def _get_marker(self, result: dict) -> str:
        return str(result.get("Marker"))

    def _get_data(self, result: dict) -> list | None:
        return result.get("Contents", [])

    async def generate(self, bucket_name: str, start_page: str = "") -> AsyncGenerator[Any, None]:
        paginator_config = self._paginator_config | {"StartingToken": start_page}
        async for result in self._paginator.paginate(Bucket=bucket_name, Prefix="", PaginationConfig=paginator_config):
            marker = self._get_marker(result)
            data = self._get_data(result)
            if data is None:
                continue
            yield data, marker
