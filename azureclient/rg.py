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

from azure.core.exceptions import ResourceNotFoundError, ServiceResponseTimeoutError
from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from config.azconfig import AZDataPipelineConfig, AzureConfig


class AZResourceGroup:
    OPERATION_TIMEOUT = AZDataPipelineConfig.OPERATION_TIMEOUT
    WAIT_ATTEMPTS = AZDataPipelineConfig.WAIT_ATTEMPTS

    def __init__(
        self, subscription_id: str = AzureConfig.SUBSCRIPTION_ID, rg_name: str = AZDataPipelineConfig.RG_NAME
    ):
        self.credential = AzureCliCredential()
        self._rg_client = ResourceManagementClient(credential=self.credential, subscription_id=subscription_id)
        self.subscription_id = subscription_id
        self.name = rg_name

    def exists(self):
        return self._rg_client.resource_groups.check_existence(self.name)

    def create_or_update(
        self, location: str = AZDataPipelineConfig.RG_LOCATION, tags: dict = AZDataPipelineConfig.TAGS
    ):
        created_or_updated = "updated" if self.exists() else "created"

        params = {"location": location}
        if tags:
            params["tags"] = tags

        rg_result = self._rg_client.resource_groups.create_or_update(self.name, params)
        if rg_result:
            print(f"The resource group ({self.name}) has been {created_or_updated}.")
            return rg_result
        else:
            if self._rg_client.resource_groups.check_existence(self.name):
                raise ResourceNotFoundError

    def delete(self):
        if self.exists():
            result = self._rg_client.resource_groups.begin_delete(self.name)
            attempt_count = 0
            while not result.done():
                if attempt_count < self.WAIT_ATTEMPTS:
                    result.wait(self.OPERATION_TIMEOUT)
                    attempt_count += 1
                    mes = (
                        f"The resource group ({self.name}) is still being deleted. "
                        f"Waiting attempt count: {attempt_count}"
                    )
                    print(f"\r\033[94m{mes}\033[0m", end="", flush=True)

                else:
                    break
            if not result.done():
                raise ServiceResponseTimeoutError(message="DELETE operation timeout")

            print(f"\r", end="", flush=True)
            print(f"The resource group ({self.name}) has been deleted")

    @property
    def location(self):
        if self.exists():
            return self._rg_client.resource_groups.get().location

    @property
    def tags(self):
        if self.exists():
            return self._rg_client.resource_groups.get().tags


if __name__ == "__main__":
    is_to_test_run = True
    is_to_create = False
    is_to_delete = True
    if is_to_test_run:
        rg = AZResourceGroup()
        if rg.exists():
            if is_to_delete:
                rg.delete()
        else:
            if is_to_create:
                rg.create_or_update()

    else:
        pass
