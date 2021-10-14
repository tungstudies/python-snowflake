import os

from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
load_dotenv()


class SnowflakeConfig:
    """Snowflake configuration class."""

    USERNAME = os.getenv("USERNAME")
    PASSWORD = os.getenv("PASSWORD")
    ACCOUNT = os.getenv("ACCOUNT")
    # extra
    WAREHOUSE = os.getenv("WAREHOUSE")
    DATABASE = os.getenv("DATABASE")
    SCHEMA = os.getenv("SCHEMA")

    # snowflake artifacts
    SNOWFLAKE_RESOURCE_TYPES = ['DATABASES', 'WAREHOUSES', 'ROLES', 'SCHEMAS']
    SNOWFLAKE_TABLE_TYPES = ['TEMPORARY', 'TRANSIENT']


class FinhubConfig:
    """Finhub API configuration class."""
    API_KEY = os.getenv("API_KEY")


if __name__ == "__main__":
    print(SnowflakeConfig.ACCOUNT)
    pass
