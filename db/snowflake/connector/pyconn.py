from typing import Optional, Union

import snowflake.connector as sfconn

from config.config import SnowflakeConfig


class SnowflakeClient:
    def __init__(self):
        self._conn: Optional[sfconn.SnowflakeConnection] = None
        self._cs: Optional[sfconn.cursor.SnowflakeCursor] = None

        self._user: Optional[str] = None
        self._password: Optional[str] = None
        self._account: Optional[str] = None

        self._warehouse: Optional[str] = None
        self._database: Optional[str] = None
        self._schema: Optional[str] = None
        self._role: Optional[str] = None

    def open_connection(
            self,
            user=SnowflakeConfig.USERNAME,
            password=SnowflakeConfig.PASSWORD,
            account=SnowflakeConfig.ACCOUNT,
            warehouse: Union[str, None] = None,
            database: Union[str, None] = None,
            schema: Union[str, None] = None,
    ):
        self._user = user
        self._password = password
        self._account = account
        self._warehouse = warehouse.upper() if warehouse else None
        self._database = database.upper() if warehouse else None
        self._schema = schema.upper() if warehouse else None
        return self._create_conn() if warehouse else None

    @property
    def cursor(self):
        if not self._cs or self._cs.is_closed():
            if not self._conn:
                self._create_conn()
            self._cs = self._conn.cursor()

        return self._cs

    def test_connection(self):
        try:
            self.cursor.execute("Select current_version()")
            row = self.cursor.fetchone()
            print(f"Return result @ index zero: {row[0]}")

        except sfconn.errors.Error:
            raise

        finally:
            self.cursor.close()

    def close_connection(self):
        if self._conn and not self._conn.is_closed():
            self._conn.close()

    def create_warehouse(
            self,
            wh_name: str,
            comment: str,
            size: str = "XSMALL",
            wh_type: str = "STANDARD",
            scaling_policy: str = "STANDARD",
            auto_suspend: int = 300,
            auto_resume: bool = True,
            min_cluster_count: int = 1,
            max_cluster_count: int = 1,
    ):

        sql_query = (
            f"CREATE WAREHOUSE IF NOT EXISTS {wh_name} "
            f"WITH WAREHOUSE_SIZE = {size} "
            f"WAREHOUSE_TYPE = {wh_type} "
            f"AUTO_SUSPEND = {auto_suspend} "
            f"AUTO_RESUME = {'TRUE' if auto_resume else 'FALSE'} "
            f"MIN_CLUSTER_COUNT = {min_cluster_count} "
            f"MAX_CLUSTER_COUNT = {max_cluster_count} "
            f"SCALING_POLICY = {scaling_policy} "
            f"COMMENT = '{comment}'"
        )

        print(f"Creating warehouse ({wh_name})...")

        resp = self._query_fetchone(sql_query)

        msg = "Warehouse creation request did not go through"
        self._check_response(resp, msg)

    def create_database(self, db_name: str, comment: Union[str, None] = None):
        if comment:
            sql_query = f"CREATE DATABASE IF NOT EXISTS {db_name} COMMENT = '{comment}';"
        else:
            sql_query = f"CREATE DATABASE IF NOT EXISTS {db_name};"

        print(f"Creating database ({db_name})...")
        resp = self._query_fetchone(sql_query)

        msg = "Database creation request did not go through"
        self._check_response(resp, msg)

    def create_schema(
            self,
            schema_name: str,
            comment: Union[str, None] = None,
            db_name: Union[str, None] = None,
            is_managed_access: bool = False,
    ):
        if not db_name:
            if not self._database:
                raise ValueError("Please provide database name (db_name)")
            else:
                db_name = self._database

        _schema = f'"{db_name}"."{schema_name}"'

        _managed_access = " "
        if is_managed_access:
            _managed_access += "WITH MANAGED ACCESS "

        if comment:
            sql_query = f"CREATE SCHEMA IF NOT EXISTS {_schema}{_managed_access}COMMENT = '{comment}';"
        else:
            sql_query = f"CREATE SCHEMA IF NOT EXISTS {_schema}{_managed_access};"

        print(f"Creating schema ({schema_name.upper()})...")
        resp = self._query_fetchone(sql_query)

        msg = "Schema creation request did not go through"
        self._check_response(resp, msg)

    def show_roles(self):
        return self._show_resources("roles", 1)

    def show_warehouses(self):
        return self._show_resources("warehouses", 0)

    def show_databases(self):
        return self._show_resources("databases", 1)

    def show_schemas(self, db_name: Union[str, None] = None):
        if db_name:
            return self._show_resources("schemas", 1, db_name)
        else:
            return self._show_resources("schemas", 1)

    def role_exists(self, role_name: str) -> bool:
        return role_name.upper() in self.show_roles()

    def warehouse_exists(self, wh_name: str) -> bool:
        return wh_name.upper() in self.show_warehouses

    def database_exists(self, db_name: str):
        return db_name.upper() in self.show_databases()

    def schema_exists(self, schema_name: str, db_name: str):
        if self.database_exists(db_name):
            return schema_name.upper() in self.show_schemas(db_name)
        else:
            return False

    def use_role(self, role_name: str):
        if self.role_exists(role_name):
            is_successfully_executed = self._use_resource("ROLES", role_name)
            if is_successfully_executed:
                self._role = role_name.upper()

    def use_warehouse(self, wh_name: str):
        if self.warehouse_exists(wh_name):
            is_successfully_executed = self._use_resource("WAREHOUSES", wh_name)
            if is_successfully_executed:
                self._warehouse = wh_name.upper()

    def use_database(self, db_name: str):
        if self.database_exists(db_name):
            is_successfully_executed = self._use_resource("DATABASES", db_name)
            if is_successfully_executed:
                self._database = db_name.upper()

    def use_schema(self, schema_name: str, db_name: Union[str, None] = None):

        if not db_name:
            if not self._database:
                raise ValueError("Please provide database name (db_name)")
            else:
                db_name = self._database

        if self.schema_exists(schema_name, db_name):
            _schema_name = f'"{db_name.upper()}".{schema_name.upper()}'
            is_successfully_executed = self._use_resource("SCHEMAS", _schema_name)
            if is_successfully_executed:
                self._database = db_name.upper()
                self._schema = schema_name.upper()

    @property
    def current_role(self):
        if self._role:
            return self._role

    @property
    def current_warehouse(self):
        if self._warehouse:
            return self._warehouse

    @property
    def current_database(self):
        if self._database:
            return self._database

    @property
    def current_schema_name(self):
        if self._schema:
            return self._schema

    @property
    def current_schema(self):
        if self._database and self._schema:
            _schema_name = f'"{self._database.upper()}".{self._schema.upper()}'
            return _schema_name

    def execute_query(self, sql_query: str):
        pass

    def _show_resources(self, resource_type: str, target_index: int, in_acc_or_db: Union[str, None] = None) -> list:
        if resource_type.upper() not in SnowflakeConfig.SNOWFLAKE_RESOURCE_TYPES:
            raise ValueError(f"InvalidresourceType: expected one of: {SnowflakeConfig.SNOWFLAKE_RESOURCE_TYPES}")
        else:
            _in = f' IN {in_acc_or_db.upper()}' if in_acc_or_db else ''
            sql_query = f"SHOW {resource_type.upper()}{_in};"

            resources = self._query_fetchall(sql_query)
            return [resource[target_index] for resource in resources]

    def _use_resource(self, resource_type: str, resource_name: str):
        if resource_type.upper() not in SnowflakeConfig.SNOWFLAKE_RESOURCE_TYPES:
            raise ValueError(f"InvalidResourceType: expected one of: {SnowflakeConfig.SNOWFLAKE_RESOURCE_TYPES}")
        else:
            singular_form = resource_type.upper()[:-1]
            sql_query = f"USE {singular_form} {resource_name.upper()};"
            resp_mes = self._query_fetchone(sql_query)
            return str(resp_mes).find("succe") != -1

    def _query_fetchone(self, sql_query: str):
        try:
            self.cursor.execute(sql_query)
            resp = self.cursor.fetchone()
            return resp[0]

        except sfconn.errors.Error:
            raise

        finally:
            self.cursor.close()

    def _query_fetchall(self, sql_query: str):
        try:
            self.cursor.execute(sql_query)
            resp = sfc.cursor.fetchall()
            return resp

        except sfconn.errors.Error:
            raise

        finally:
            self.cursor.close()

    def _create_conn(self):
        if not self._user or not self._password or not self._account:
            raise sfconn.errors.Error(msg="Missing username/password/account. Please create new connection.")

        if not self._conn or self._conn.is_closed():
            try:
                self._conn = sfconn.connect(
                    user=self._user,
                    password=self._password,
                    account=self._account,
                    warehouse=self._warehouse,
                    database=self._database,
                    schema=self._schema,
                )
                return self._conn

            except sfconn.errors.DatabaseError as db_ex:
                if db_ex.errno == 250001:
                    raise sfconn.errors.DatabaseError(msg="Invalid username/password. Please please re-enter.")
                else:
                    raise sfconn.errors.DatabaseError

            except sfconn.errors.Error:
                raise

    def create_table(self, tbl_name: str, column_creation_str: str, db_name: Union[str, None] = None,
                     schema_name: Union[str, None] = None,
                     comment: Union[str, None] = None, to_replace: bool = False):

        if not db_name:
            if not self._database:
                raise ValueError("Please provide database name (db_name)")
            else:
                db_name = self._database

        if not schema_name:
            if not self._schema:
                raise ValueError("Please provide database name (schema_name)")
            else:
                schema_name = self._schema

        _table = f'"{db_name.upper()}"."{schema_name.upper()}"."{tbl_name.upper()}"'

        _field_creation_str = f" ({column_creation_str})"

        _create_or_replace = 'CREATE OR REPLACE TABLE ' if to_replace else 'CREATE TABLE IF NOT EXISTS '

        if comment:
            sql_query = f"{_create_or_replace}{_table}{_field_creation_str} COMMENT = '{comment}';"
        else:
            sql_query = f"CREATE TABLE IF NOT EXISTS {_table}{_field_creation_str};"

        print(f"Creating table ({tbl_name.upper()})...")
        resp = self._query_fetchone(sql_query)

        msg = "Table creation request did not go through"
        self._check_response(resp, msg)

    def create_csv_file_format(self, ff_name: str, db_name: Union[str, None] = None,
                               schema_name: Union[str, None] = None, compression: str = 'AUTO',
                               field_delimiter: str = r',', record_delimiter: str = r'\n', skip_header: int = 0,
                               trim_space: bool = False, null_if: str = r'\\N'):

        if not db_name:
            if not self._database:
                raise ValueError("Please provide database name (db_name)")
            else:
                db_name = self._database

        if not schema_name:
            if not self._schema:
                raise ValueError("Please provide database name (schema_name)")
            else:
                schema_name = self._schema

        _ff_name = f'"{db_name.upper()}"."{schema_name.upper()}".{ff_name.upper()}'

        sql_query = f"CREATE FILE FORMAT IF NOT EXISTS {_ff_name} TYPE = 'CSV' COMPRESSION = '{compression}' " \
                    f"FIELD_DELIMITER = '{field_delimiter}' RECORD_DELIMITER = '{record_delimiter}' " \
                    f"SKIP_HEADER = {skip_header} " \
                    f"FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' TRIM_SPACE = {'TRUE' if trim_space else 'FALSE'} " \
                    f"ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ESCAPE = 'NONE' " \
                    f"ESCAPE_UNENCLOSED_FIELD = '\\134' DATE_FORMAT = 'AUTO' " \
                    f"TIMESTAMP_FORMAT = 'AUTO' NULL_IF = ('{null_if}');"

        print(f"Creating CSV file format ({ff_name.upper()})...")
        resp = self._query_fetchone(sql_query)

        msg = "CSV file format creation request did not go through"
        self._check_response(resp, msg)

    def create_json_file_format(self, ff_name: str, db_name: Union[str, None] = None,
                                schema_name: Union[str, None] = None, compression: str = 'AUTO',
                                enable_octal: bool = False, allow_duplicate: bool = False,
                                strip_outer_array: bool = False, strip_null_values: bool = False,
                                ignore_utf8_errors: bool = False,
                                comment: Union[str, None] = None):

        if not db_name:
            if not self._database:
                raise ValueError("Please provide database name (db_name)")
            else:
                db_name = self._database

        if not schema_name:
            if not self._schema:
                raise ValueError("Please provide database name (schema_name)")
            else:
                schema_name = self._schema

        _ff_name = f'"{db_name.upper()}"."{schema_name.upper()}".{ff_name.upper()}'

        _sql_query = f"CREATE FILE FORMAT IF NOT EXISTS {_ff_name} TYPE = 'JSON' COMPRESSION = '{compression}' " \
                     f"ENABLE_OCTAL = {'TRUE' if enable_octal else 'FALSE'} " \
                     f"ALLOW_DUPLICATE = {'TRUE' if allow_duplicate else 'FALSE'} " \
                     f"STRIP_OUTER_ARRAY = {'TRUE' if strip_outer_array else 'FALSE'} " \
                     f"STRIP_NULL_VALUES = {'TRUE' if strip_null_values else 'FALSE'} " \
                     f"IGNORE_UTF8_ERRORS = {'TRUE' if ignore_utf8_errors else 'FALSE'}"

        if comment:
            sql_query = f"{_sql_query} COMMENT = '{comment}';"
        else:
            sql_query = f"{_sql_query};"

        print(f"Creating JSON file format ({ff_name.upper()})...")
        resp = self._query_fetchone(sql_query)
        msg = "JSON file format creation request did not go through"
        self._check_response(resp, msg)

    @staticmethod
    def _check_response(resp, exception_message: str):
        if str(resp).find("succe") != -1:
            print(f"Result status: {resp}")
            return True
        else:
            raise sfconn.errors.BadRequest(msg=exception_message)

    def create_stage_snowflake(self, stage_name: str, db_name: Union[str, None] = None,
                               schema_name: Union[str, None] = None,
                               comment: Union[str, None] = None):

        if not db_name:
            if not self._database:
                raise ValueError("Please provide database name (db_name)")
            else:
                db_name = self._database

        if not schema_name:
            if not self._schema:
                raise ValueError("Please provide database name (schema_name)")
            else:
                schema_name = self._schema

        _stage_name = f'"{db_name.upper()}"."{schema_name.upper()}".{stage_name.upper()}'
        _sql_query = f'CREATE STAGE IF NOT EXISTS "{db_name.upper()}"."{schema_name.upper()}".{stage_name.upper()}'

        if comment:
            sql_query = f"{_sql_query} COMMENT = '{comment}';"
        else:
            sql_query = f"{_sql_query};"

        print(f"Creating stage ({stage_name.upper()})...")
        resp = self._query_fetchone(sql_query)
        msg = "Stage creation request did not go through"
        if self._check_response(resp, msg):
            return _stage_name

    def upload_csv(self, csv_filepath: str, stage_name):

        pass


if __name__ == "__main__":
    sfc = SnowflakeClient()
    sfc.open_connection()
    sfc.use_role('ACCOUNTADMIN')
    sfc.create_database('VILOK')
    sfc.use_schema('HELLOK', 'SALES')
    print(sfc.current_schema)
    # sfc.create_table('TB_TEST', 'ID INT AUTOINCREMENT NOT NULL ,MESSAGE STRING NOT NULL', 'SALES', 'DIMENSIONS')
    # sfc.create_json_file_format('json_example', 'SALES', 'DIMENSIONS', comment="hello")
    # sfc.create_stage_snowflake('csv_local', 'SALES', 'DIMENSIONS', comment="hello")
    pass
