from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pydantic_settings import BaseSettings, SettingsConfigDict

if TYPE_CHECKING:
    import boto3


class S3Settings(BaseSettings):
    """Settings for S3 clients.

    You can adapt the following settings in your environment variables (or using and .env file):
    - S3_ENDPOINT_URL: The URL of the S3 server
    - S3_REGION: The region of the S3 server. If using garage, use "garage"
    - S3_BUCKET: The bucket to use for the S3 client
    - S3_AWS_ACCESS_KEY_ID: The access key ID for the S3 client
    - S3_AWS_SECRET_ACCESS_KEY: The secret access key for the S3 client

    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="S3_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    # The endpoint URL of the S3 server
    endpoint_url: str = "http://127.0.0.1:3900"

    # The region of the S3 server. If using garage, use "garage"
    region: str = "garage"

    # The bucket to use for the S3 client
    bucket: str = "default"

    # The access key ID for the S3 client
    aws_access_key_id: str

    # The secret access key for the S3 client
    aws_secret_access_key: str

    def create_client(self) -> boto3.client:
        """Create a S3 client from the settings."""

        import boto3

        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            region_name=self.region,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )
