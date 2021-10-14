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

from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.storage import StorageManagementClient
from config.azconfig import AZDataPipelineConfig

from azureclient.rg import AZResourceGroup


class AZStorageAccount:
    def __init__(
            self, sa_name: str = AZDataPipelineConfig.STORAGE_ACCOUNT_NAME, rg: AZResourceGroup = AZResourceGroup()
    ):

        if not rg.exists():
            raise ResourceNotFoundError(message=f"The resource group ({rg.name}) does not exist")
        self._rg = rg
        self.storage_client = StorageManagementClient(
            credential=self._rg.credential, subscription_id=self._rg.subscription_id
        )

        self.sa_name = sa_name

    def exists(self):
        try:
            if self.storage_client.storage_accounts.get_properties(self._rg.name, self.sa_name):
                return True
        except ResourceNotFoundError:
            print(f"The storage account ({self.sa_name}) has not been created")
            return False

    def create(
            self,
            location: str = AZDataPipelineConfig.RG_LOCATION,
            kind: str = "StorageV2",
            sku="Standard_LRS",
            access_tier: str = "Hot",
            allow_blob_public_access: bool = False,
            minimum_tls_version="TLS1_2",
            is_hns_enabled: bool = True,
            large_file_share_enabled: bool = False,
            enable_https_traffic_only=True,
            tags: dict = AZDataPipelineConfig.TAGS,
    ):

        if not self.exists():
            params = {
                "location": location,
                "kind": kind,
                "minimum_tls_version": minimum_tls_version,
                "allow_blob_public_access": allow_blob_public_access,
                "access_tier": access_tier,
                "sku": {"name": sku},
                "large_file_shares_state": "Enabled" if large_file_share_enabled else "Disabled",
                "enable_https_traffic_only": enable_https_traffic_only,
                "is_hns_enabled": is_hns_enabled,
            }

            if tags:
                params["tags"] = tags

            poller = self.storage_client.storage_accounts.begin_create(self._rg.name, self.sa_name, params)
            sa = poller.result()
            if sa:
                print(f"The storage account ({self.sa_name}) has been created.")
                return sa

    def update(
            self,
            kind: str = "StorageV2",
            sku="Standard_LRS",
            access_tier: str = "Hot",
            allow_blob_public_access: bool = False,
            minimum_tls_version="TLS1_2",
            is_hns_enabled: bool = True,
            large_file_share_enabled: bool = False,
            enable_https_traffic_only=True,
            tags: dict = AZDataPipelineConfig.TAGS,
    ):

        if self.exists():
            params = {
                "kind": kind,
                "minimum_tls_version": minimum_tls_version,
                "access_tier": access_tier,
                "allow_blob_public_access": allow_blob_public_access,
                "sku": {"name": sku},
                "large_file_shares_state": "Enabled" if large_file_share_enabled else "Disabled",
                "enable_https_traffic_only": enable_https_traffic_only,
                "is_hns_enabled": is_hns_enabled,
            }

            if tags:
                params["tags"] = tags

            poller = self.storage_client.storage_accounts.update(self._rg.name, self.sa_name, params)
            sa = poller.result()
            if sa:
                print(f"The storage account ({self.sa_name}) has been updated.")
                return sa

    def delete(self):
        if self.exists():
            self.storage_client.storage_accounts.delete(self._rg.name, self.sa_name)
            print(f"The storage account ({self.sa_name}) has been deleted")

    @property
    def primary_key(self):
        if self.exists():
            keys = self.storage_client.storage_accounts.list_keys(self._rg.name, self.sa_name)
            print(f"Primary key for storage account ({self.sa_name}) has been retrieved")
            return keys.keys[0].value

    @property
    def conn_string(self):
        if self.exists():
            keys = self.storage_client.storage_accounts.list_keys(self._rg.name, self.sa_name)
            print(f"Connection string for storage account ({self.sa_name}) has been retrieved")
            return (
                f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;"
                f"AccountName={self.sa_name};AccountKey={keys.keys[0].value}"
            )


if __name__ == "__main__":
    is_to_test_run = True
    if is_to_test_run:
        rg = AZResourceGroup()
        if not rg.exists():
            rg.create_or_update()

        sa = AZStorageAccount(rg=rg)
        if not sa.exists():
            sa.create()
    else:
        pass
