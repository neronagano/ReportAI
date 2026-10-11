from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Protocol

import anyio
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

from shiftledger.config import Settings


class ObjectStorage(Protocol):
    async def put(self, key: str, data: bytes, content_type: str) -> None: ...

    async def get(self, key: str) -> bytes: ...

    async def delete(self, key: str) -> None: ...

    async def check(self) -> None: ...


class S3Storage:
    """The synchronous SDK is isolated from the event loop with bounded SDK timeouts."""

    def __init__(self, settings: Settings) -> None:
        self.bucket = settings.s3_bucket
        self.max_bytes = settings.upload_max_bytes
        self.client: S3Client = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            region_name=settings.s3_region,
            aws_access_key_id=settings.s3_access_key_id,
            aws_secret_access_key=(
                settings.s3_secret_access_key.get_secret_value()
                if settings.s3_secret_access_key
                else None
            ),
            config=Config(
                connect_timeout=3,
                read_timeout=10,
                retries={"max_attempts": 2, "mode": "standard"},
                s3={"addressing_style": "path"},
            ),
        )

    async def put(self, key: str, data: bytes, content_type: str) -> None:
        await anyio.to_thread.run_sync(
            partial(
                self.client.put_object,
                Bucket=self.bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
            )
        )

    def _read(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        with response["Body"] as stream:
            data = stream.read(self.max_bytes + 1)
        if len(data) > self.max_bytes:
            raise ValueError("Stored object exceeds the permitted size")
        return data

    async def get(self, key: str) -> bytes:
        return await anyio.to_thread.run_sync(self._read, key)

    async def delete(self, key: str) -> None:
        await anyio.to_thread.run_sync(
            partial(self.client.delete_object, Bucket=self.bucket, Key=key)
        )

    async def check(self) -> None:
        await anyio.to_thread.run_sync(partial(self.client.head_bucket, Bucket=self.bucket))

    async def ensure_bucket(self) -> None:
        try:
            await self.check()
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in {"404", "NoSuchBucket", "NotFound"}:
                raise
            kwargs = (
                {}
                if self.client.meta.region_name == "us-east-1"
                else {
                    "CreateBucketConfiguration": {
                        "LocationConstraint": self.client.meta.region_name
                    }
                }
            )
            await anyio.to_thread.run_sync(
                partial(self.client.create_bucket, Bucket=self.bucket, **kwargs)  # type: ignore[arg-type]
            )

    async def close(self) -> None:
        await anyio.to_thread.run_sync(self.client.close)
