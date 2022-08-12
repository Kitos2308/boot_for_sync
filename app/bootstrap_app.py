import asyncio
import signal

from platform_logger import configure_logger

from app.config import AppConfig
from app.services.bootstrap import BootstrapService


def main() -> None:
    config = AppConfig()
    configure_logger(config.log.level, config.log.type)
    service = BootstrapService(config)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()

    try:
        loop.add_signal_handler(signal.SIGINT, loop.stop)
        loop.add_signal_handler(signal.SIGTERM, loop.stop)

        loop.run_until_complete(service.start())
        loop.run_forever()
    finally:
        loop.run_until_complete(service.stop())
        loop.close()


if __name__ == "__main__":
    main()
