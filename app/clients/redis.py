import aioredis


class Redis:

    def __init__(self, host: str, port: int, db: int) -> None:
        self._host = host
        self._port = port
        self._db = db
        self._client: aioredis.Redis

    async def connect(self) -> None:
        self._client = await aioredis.from_url(f"redis://{self._host}:{self._port}/{self._db}")

    async def disconnect(self) -> None:
        await self._client.close()

    async def set(self, key: str, value: str) -> None:
        await self._client.set(key, value)

    async def get(self, key: str) -> str:
        result = await self._client.get(key)
        if result is None:
            return ""
        else:
            return result.decode("utf-8")

    async def delete(self, key: str) -> None:
        await self._client.delete(key)
