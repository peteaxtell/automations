import csv
import io
from io import BytesIO, StringIO

import boto3
import botocore
from prefect import get_run_logger

from src.automations.config import S3Config
from src.automations.shared.exceptions import S3FileNotFoundError


class S3Client:
    """S3 client"""

    def __init__(self) -> None:
        """Initialize the S3 client."""
        self._config = S3Config()
        self._client = boto3.client(
            "s3",
            region_name=self._config.region,
            aws_access_key_id=self._config.access_key_id,
            aws_secret_access_key=self._config.secret_access_key.get_secret_value(),
        )

    def _download_file(self, bucket: str, object_name: str) -> bytes:
        """Get the bytes of a file from an S3 bucket.

        Args:
            bucket: The name of the S3 bucket.
            object_name: The key of the object in the S3 bucket.
        Returns:
            The bytes of the file downloaded from S3.
        """

        logger = get_run_logger()

        try:
            with BytesIO() as file:
                self._client.download_fileobj(
                    Fileobj=file, Bucket=bucket, Key=object_name
                )
                file.seek(0)
                content = file.read()

            logger.info(
                f"File downloaded from S3 bucket {bucket} with object name {object_name}"
            )

            return content
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                raise S3FileNotFoundError(
                    f"File not found in S3 bucket {bucket} with object name {object_name}"
                ) from e
            else:
                raise

    def _upload_file(self, bucket: str, file: BytesIO, object_name: str) -> None:
        """Upload a file to an S3 bucket.

        Args:
            bucket: The name of the S3 bucket.
            file: A BytesIO object containing the file data to upload.
            object_name: The key to use for the uploaded object in the S3 bucket.
        """

        logger = get_run_logger()

        self._client.upload_fileobj(Fileobj=file, Bucket=bucket, Key=object_name)

        logger.info(
            f"File uploaded to S3 bucket {bucket} with object name {object_name}"
        )

    def download_csv(self, bucket: str, object_name: str) -> list[dict]:
        """Download a CSV file from S3 and return as a list of dictionaries.

        Args:
            bucket: The name of the S3 bucket.
            object_name: The key of the object in the S3 bucket.
        Returns:
            A list of dictionaries representing the rows in the CSV file.
        """
        content = self._download_file(bucket, object_name)

        return list(csv.DictReader(io.StringIO(content.decode("utf-8"))))

    def upload_csv(self, bucket: str, data: list[dict], object_name: str) -> None:
        """Upload a list of dictionaries as a CSV file to S3.

        Args:
            bucket: The name of the S3 bucket.
            data: A list of dictionaries to upload as CSV.
            object_name: The key to use for the uploaded object in the S3 bucket.
        """
        with StringIO() as file:
            writer = csv.DictWriter(file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
            file.seek(0)
            self._upload_file(bucket, file, object_name)
