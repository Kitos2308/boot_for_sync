from aiokafka import AIOKafkaProducer


class KafkaProducer:

    def __init__(self, *, bootstrap_servers: list[str] | str) -> None:
        self._bootstrap_servers = bootstrap_servers

        self._client: AIOKafkaProducer

    async def connect(self) -> None:
        self._client = AIOKafkaProducer(bootstrap_servers=self._bootstrap_servers)
        await self._client.start()

    async def disconnect(self) -> None:
        await self._client.stop()

    async def send(self, topic: str, value: bytes | None) -> None:
        await self._client.send(topic, value)
