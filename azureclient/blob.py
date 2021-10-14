import os
import sys

# getting the name of the directory
# where the this file is present.
current = os.path.dirname(os.path.realpath(__file__))

# Getting the parent directory name
# where the current directory is present.
parent = os.path.dirname(current)
# adding the parent directory to
# the sys.path.
sys.path.append(parent)

# now we can import the module in the parent
# directory.

from typing import Optional

from azure.storage.blob import BlobServiceClient, ContainerClient

from azureclient.sa import AZStorageAccount
from config.azconfig import AZDataPipelineConfig

CONN_STR = AZStorageAccount().conn_string


class BlobStorageContainer:
    def __init__(self, container_name: str = AZDataPipelineConfig.STORAGE_CONTAINER_NAME, conn_str: str = CONN_STR):
        self.container_name = container_name
        self._conn_str = conn_str
        self._storage_client: Optional[BlobServiceClient] = BlobServiceClient.from_connection_string(conn_str=conn_str)

        self._container_client: Optional[ContainerClient] = None

    @property
    def conn_string(self):
        return self._conn_str

    def exists(self):
        self._container_client = self._storage_client.get_container_client(self.container_name)
        return self._container_client.exists()

    def create(self):
        if not self.exists():
            self._container_client = self._storage_client.create_container(self.container_name)
            if self._container_client:
                print(f"The storage container ({self.container_name}) has been created.")
                return self._container_client

    def delete(self):
        if self.exists():
            self._container_client.delete_container()
            print(f"The storage container ({self.container_name}) has been deleted.")

    def add_directory(self, folder_name: str):
        pass

    def upload_file(self, filepath: str, blob: str = None):
        # Create a blob client using the local file name as the name for the blob
        if not blob:
            blob = os.path.basename(filepath)

        blob_client = self._storage_client.get_blob_client(container=self.container_name, blob=blob)
        print("Uploading to Azure Storage as blob: {}".format(blob))
        with open(filepath, "rb") as data:
            blob_client.upload_blob(data)

    def delete_file(self, blob: str):
        blob_client = self._storage_client.get_blob_client(container=self.container_name, blob=blob)
        blob_client.delete_blob()


if __name__ == '__main__':

    is_to_test_run = True
    is_to_create = True
    is_to_delete = False

    if is_to_test_run:
        bsc = BlobStorageContainer()
        if bsc.exists():
            print(f"The storage container ({bsc.container_name}) exists.")
            if is_to_delete:
                bsc.delete()
        else:
            if is_to_create:
                bsc.create()

        bsc.upload_file("/Users/tungnguyen/Documents/Github/python-azure-data-engineering/data/cars.csv")
    else:
        pass
