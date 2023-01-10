from rptest.archival.s3_client import ObjectMetadata

from azure.storage.blob import BlobClient, BlobServiceClient, BlobType, ContainerClient
from itertools import chain, islice

import time
import datetime
from logging import Logger
from typing import Iterator, NamedTuple
from base64 import standard_b64decode


def build_connection_string(endpoint: str, storage_account: str,
                            key: str) -> str:
    parts = [
        "DefaultEndpointsProtocol=http", f"AccountName={storage_account}",
        f"AccountKey={key}", f"BlobEndpoint={endpoint}"
    ]
    return ";".join(parts)


class ABSClient:
    def __init__(self, storage_account: str, shared_key: str, endpoint: str,
                 logger: Logger):
        self.conn_str = build_connection_string(endpoint, storage_account,
                                                shared_key)
        self.logger = logger

        self.logger.info(f"Connection string: {self.conn_str}")

    def _wait_no_key(self,
                     blob_client: BlobServiceClient,
                     timeout_sec: float = 10):
        deadline = datetime.datetime.now() + datetime.timedelta(
            seconds=timeout_sec)

        try:
            while blob_client.exists():
                now = datetime.datetime.now()
                if now > deadline:
                    raise TimeoutError(
                        f"Blob was not deleted within {timeout_sec}s")
                time.sleep(2)
        except Exception as err:
            if isinstance(err, TimeoutError):
                raise err

            self.logger.debug(f"error ocurred while polling blob {err}")

    def create_bucket(self, name: str):
        blob_service = BlobServiceClient.from_connection_string(self.conn_str)
        blob_service.create_container(name)

    def delete_bucket(self, name: str):
        blob_service = BlobServiceClient.from_connection_string(self.conn_str)
        blob_service.delete_container(name)

    def empty_bucket(self, name: str):
        container_client = ContainerClient.from_connection_string(
            self.conn_str, container_name=name)
        blob_names_generator = container_client.list_blob_names()

        for blob in blob_names_generator:
            container_client.delete_blob(blob)

    def delete_object(self, bucket: str, key: str, verify=False):
        blob_client = BlobClient.from_connection_string(self.conn_str,
                                                        container_name=bucket,
                                                        blob_name=key)
        blob_client.delete_blob()

        if verify:
            self._wait_no_key(blob_client)

    def get_object_data(self, bucket: str, key: str) -> bytes:
        blob_client = BlobClient.from_connection_string(self.conn_str,
                                                        container_name=bucket,
                                                        blob_name=key)
        return blob_client.download_blob().content_as_bytes()

    def put_object(self, bucket: str, key: str, data: str):
        container_client = ContainerClient.from_connection_string(
            self.conn_str, container_name=bucket)
        blob_client = container_client.upload_blob(
            name=key,
            data=bytes(data, encoding="utf-8"),
            blob_type=BlobType.BLOCKBLOB,
            overwrite=True)

        assert blob_client.exists(), f"Failed to upload blob {key}"

    def get_object_meta(self, bucket: str, key: str) -> ObjectMetadata:
        blob_client = BlobClient.from_connection_string(self.conn_str,
                                                        container_name=bucket,
                                                        blob_name=key)
        props = blob_client.get_blob_properties()

        assert props.deleted == False

        return ObjectMetadata(bucket=props.container,
                              key=props.name,
                              etag=props.content_settings.content_md5.hex(),
                              content_length=props.size)

    def list_objects(self, bucket: str) -> Iterator[ObjectMetadata]:
        container_client = ContainerClient.from_connection_string(
            self.conn_str, container_name=bucket)
        for blob_props in container_client.list_blobs():
            yield ObjectMetadata(
                bucket=blob_props.container,
                key=blob_props.name,
                etag=blob_props.content_settings.content_md5.hex(),
                content_length=blob_props.size)
