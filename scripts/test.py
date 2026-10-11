"""Run migrations and tests against an explicitly named disposable test database."""

import asyncio
import os
import subprocess
import sys

import asyncpg
from sqlalchemy.engine import make_url


async def prepare_database() -> None:
    url = make_url(os.environ["TEST_DATABASE_URL"])
    if url.database != "shiftledger_test":
        raise SystemExit("This runner only creates the disposable database shiftledger_test")
    connection = await asyncpg.connect(
        host=url.host, port=url.port, user=url.username, password=url.password, database="postgres"
    )
    try:
        exists = await connection.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", url.database
        )
        if not exists:
            await connection.execute("CREATE DATABASE shiftledger_test")
    finally:
        await connection.close()


def main() -> None:
    asyncio.run(prepare_database())
    environment = {**os.environ, "DATABASE_URL": os.environ["TEST_DATABASE_URL"], "APP_ENV": "test"}
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"], env=environment, check=True
    )
    subprocess.run([sys.executable, "-m", "alembic", "check"], env=environment, check=True)
    raise SystemExit(
        subprocess.call([sys.executable, "-m", "pytest", *sys.argv[1:]], env=environment)
    )


if __name__ == "__main__":
    main()
