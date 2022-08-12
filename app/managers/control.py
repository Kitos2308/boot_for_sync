from app.clients.redis import Redis


class ControlBootstrap:

    def __init__(
        self,
        redis: Redis,
        bucket_name: str,
        key: str = "key_bootstrap",
        force_send_notification: bool = False,
    ):
        self._redis = redis
        self._bucket_name = bucket_name
        self._key = f"{key}:{bucket_name}"
        self._force_send_notification = force_send_notification

    async def write_current_page(self, token_page: str) -> None:
        await self._redis.set(self._key, token_page)

    async def get_starting_token(self) -> str:
        return await self._redis.get(self._key)

    async def delete_token(self) -> None:
        await self._redis.delete(self._key)

    async def get_start_page(self) -> str:
        if self._force_send_notification:
            return ""
        else:
            return await self.get_starting_token()


class ControlBootstrapManager:

    def __init__(
        self,
        redis: Redis,
        key: str = "key_bootstrap",
        force_send_notification: bool = False,
    ):
        self._redis = redis
        self._key = key
        self._force_send_notification = force_send_notification
        self._bootstrap_controls: dict[str, ControlBootstrap] = {}

    def _create_control(self, bucket_name: str) -> ControlBootstrap:
        control = ControlBootstrap(
            redis=self._redis,
            bucket_name=bucket_name,
            key=self._key,
            force_send_notification=self._force_send_notification)
        self._bootstrap_controls[bucket_name] = control
        return control

    def get_control(self, bucket_name: str) -> ControlBootstrap:
        res = self._bootstrap_controls.get(bucket_name)
        if not res:
            res = self._create_control(bucket_name)
        return res

    async def clear_all(self) -> None:
        for control in self._bootstrap_controls.values():
            await control.delete_token()
