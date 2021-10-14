import configparser
import os

from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
load_dotenv()

az_config = configparser.ConfigParser()
az_config.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), "azconfig.ini"))


class AzureConfig:
    """Azure configuration class."""

    SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID")


class AZDataPipelineConfig(AzureConfig):
    TAGS = {
        "environment": az_config["tags"]["environment"],
        "project": az_config["tags"]["project"],
        "owner": az_config["tags"]["owner"],
        "created_with": az_config["tags"]["created_with"],
    }
    RG_NAME = az_config["resource-group"]["name"]
    RG_LOCATION = az_config["resource-group"]["location"]
    STORAGE_ACCOUNT_NAME = az_config["storage-account"]["name"]
    STORAGE_CONTAINER_NAME = az_config["storage-container"]["name"]
    ADF_NAME = az_config["data-factory"]["name"]

    OPERATION_TIMEOUT = float(az_config["timeout"]["wait"])
    WAIT_ATTEMPTS = int(az_config["timeout"]["attempts"])


if __name__ == "__main__":
    pass
