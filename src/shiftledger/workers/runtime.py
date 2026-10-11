import asyncio
import signal
import time
from contextlib import suppress
from pathlib import Path
from tempfile import gettempdir


def stop_event() -> asyncio.Event:
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        # Docker runs on Linux; asyncio.run still handles Ctrl+C on Windows.
        with suppress(NotImplementedError):
            loop.add_signal_handler(signum, stopped.set)
    return stopped


def heartbeat_path(role: str) -> Path:
    return Path(gettempdir()) / f"shiftledger-{role}.heartbeat"


def heartbeat(role: str) -> None:
    heartbeat_path(role).write_text(str(time.time()), encoding="ascii")


async def wait_or_stop(stopped: asyncio.Event, seconds: float) -> None:
    with suppress(TimeoutError):
        await asyncio.wait_for(stopped.wait(), timeout=seconds)
