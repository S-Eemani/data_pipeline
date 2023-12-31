import os
from collections import Counter

import dotenv
import pandas as pd
import snowflake.connector
from airflow.models import Variable
from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


class SnowflakeConnection(object):
    def __init__(
        self,
        airflow: bool = False,
        account: str = None,
        username: str = None,
        password: str = None,
        role_name: str = None,
        db_name: str = None,
        staging_schema_name: str = None,
        raw_schema_name: str = None,
        history_schema_name: str = None,
        auth_type: str = "basic",
        connector_type: str = "snowflake_sqlalchemy",
    ):
        self.airflow = airflow

        self.account = account
        self.username = username
        self.password = password
        self.role_name = role_name
        self.db_name = db_name
        self.staging_schema_name = staging_schema_name
        self.raw_schema_name = raw_schema_name
        self.history_schema_name = history_schema_name
        self.init_args(
            init_args=locals(),
            required_arg_keys=["account", "username", "password", "role_name", "db_name"],
            env_var_key_prefix="snowflake_",
        )

        self.auth_type = auth_type
        self.connector_type = connector_type
        self.conn = None
        self._set_connection()

    def init_args(self, init_args: dict, required_arg_keys: list, env_var_key_prefix: list):
        if self.airflow is False:
            self.dotenv_file = dotenv.find_dotenv()
            dotenv.load_dotenv(self.dotenv_file)
        undefined_arg_keys = []
        undefined_env_var_keys = []
        for arg_key, arg_value in init_args.items():
            if arg_key == "self":
                pass
            elif arg_value is not None:
                setattr(self, arg_key, arg_value)
            elif self.airflow is True and Variable.get(key=f"{env_var_key_prefix}{arg_key}", default_var=None) is not None:
                setattr(self, arg_key, Variable.get(key=f"{env_var_key_prefix}{arg_key}", default_var=None))
            elif self.airflow is False and os.getenv(f"{env_var_key_prefix}{arg_key}", None) is not None:
                setattr(self, arg_key, os.getenv(f"{env_var_key_prefix}{arg_key}"))
            elif arg_key in required_arg_keys:
                undefined_arg_keys.append(arg_key)
                undefined_env_var_keys.append(f"{env_var_key_prefix}{arg_key}")

        if len(undefined_arg_keys) > 0:
            raise Exception(f"Required params are not defined as {undefined_arg_keys} in __init__ args, " + f"or as {undefined_env_var_keys} in environment variables.")

    def _set_connection(self):
        if self.auth_type == "basic" and self.connector_type == "snowflake_connector":
            self.conn = snowflake.connector.connect(
                account=self.account,
                user=self.username,
                password=self.password,
            )
        if self.auth_type == "basic" and self.connector_type == "snowflake_sqlalchemy":
            engine = create_engine(
                URL(
                    account=self.account,
                    user=self.username,
                    password=self.password,
                )
            )
            self.conn = engine.connect()

    def fetch_table_as_df(self, sql: str):
        df = pd.read_sql(sql=sql, con=self.conn)
        df.columns = df.columns.str.upper()
        return df

    def upload_df_to_snowflake(self, df: pd.DataFrame, staging_table_name: str, raw_table_name: str, history_table_name: str):
        self.conn.execute(f"USE ROLE {self.role_name}")

        # ensure dataframe is in type string so that are all columns in snowflake are in type varchar
        df = df.astype(str)

        # ensure all table names and column names are uppercase
        df.columns = map(lambda x: str(x).upper(), df.columns)
        staging_table_name = staging_table_name.upper()
        raw_table_name = raw_table_name.upper()
        history_table_name = history_table_name.upper()

        # create raw table if it does not exist
        self.conn.execute(f"USE SCHEMA {self.db_name}.{self.raw_schema_name}")
        table_details = self.conn.execute(f"SHOW TABLES LIKE '{raw_table_name}' IN SCHEMA {self.db_name}.{self.raw_schema_name}").fetchall()
        create_raw_table = True if len(table_details) == 0 else False
        if create_raw_table:
            df.head(0).to_sql(
                name=raw_table_name,
                schema=self.raw_schema_name,
                con=self.conn,
                if_exists="replace",
                index=False,
            )

        # create history table if it does not exist
        self.conn.execute(f"USE SCHEMA {self.db_name}.{self.history_schema_name}")
        table_details = self.conn.execute(f"SHOW TABLES LIKE '{history_table_name}' IN SCHEMA {self.db_name}.{self.history_schema_name}").fetchall()
        create_history_table = True if len(table_details) == 0 else False
        if create_history_table:
            df.head(0).to_sql(
                name=history_table_name,
                schema=self.history_schema_name,
                con=self.conn,
                if_exists="replace",
                index=False,
            )
            self.conn.execute(f"ALTER TABLE {self.db_name}.{self.history_schema_name}.{history_table_name} ADD (DW_CREATED_USER_ID VARCHAR(16777216), DW_CREATED_TIMESTAMP TIMESTAMP_LTZ(9))")

        # drop previous staging table and load current data into new staging table
        self.conn.execute(f"USE SCHEMA {self.db_name}.{self.staging_schema_name}")
        self.conn.execute(f"DROP TABLE IF EXISTS {self.db_name}.{self.staging_schema_name}.{staging_table_name}")
        df.to_sql(
            name=staging_table_name,
            schema=self.staging_schema_name,
            con=self.conn,
            method=pd_writer,
            index=False,
            if_exists="replace",
        )

        # alter raw table to have new columns from staging table
        current_staging_table_column_tuples = self.conn.execute(
            f"""
            SELECT column_name
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_catalog = '{self.db_name}'
            AND table_schema = '{self.staging_schema_name}'
            AND table_name = '{staging_table_name}'
            ORDER BY ordinal_position
            """
        ).fetchall()
        current_staging_table_columns = [column_tuple[0] for column_tuple in current_staging_table_column_tuples]
        current_table_column_tuples = self.conn.execute(
            f"""
            SELECT column_name
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_catalog = '{self.db_name}'
            AND table_schema = '{self.raw_schema_name}'
            AND table_name = '{raw_table_name}'
            ORDER BY ordinal_position
            """
        ).fetchall()
        current_table_columns = [column_tuple[0] for column_tuple in current_table_column_tuples]
        new_column_names = list((Counter(current_staging_table_columns) - Counter(current_table_columns)).elements())
        new_column_names_str = ", ".join([f"{new_column_name} VARCHAR" for new_column_name in new_column_names])
        if len(new_column_names) > 0:
            self.conn.execute(
                f"""
                ALTER TABLE {self.db_name}.{self.raw_schema_name}.{raw_table_name}
                ADD ({new_column_names_str});
                """
            )

        # truncate previous rows and insert new rows from staging table into raw table for columns specified in staging
        self.conn.execute(f"TRUNCATE TABLE {self.db_name}.{self.raw_schema_name}.{raw_table_name}")
        insert_into_table_columns_str = ", ".join([f'"{column_name}"' for column_name in current_staging_table_columns])
        select_from_staging_table_columns_str = ", ".join([f's."{column_name}"' for column_name in current_staging_table_columns])
        self.conn.execute(
            f"""
            INSERT INTO {self.db_name}.{self.raw_schema_name}.{raw_table_name}
                ({insert_into_table_columns_str})
            SELECT {select_from_staging_table_columns_str}
            FROM {self.db_name}.{self.staging_schema_name}.{staging_table_name} as s
            """
        )

        # alter history table to have new columns from staging table
        if len(new_column_names) > 0:
            self.conn.execute(
                f"""
                ALTER TABLE {self.db_name}.{self.history_schema_name}.{history_table_name}
                ADD ({new_column_names_str});
                """
            )

        # append new rows from staging table into history table for columns specified in staging
        self.conn.execute(
            f"""
            INSERT INTO {self.db_name}.{self.history_schema_name}.{history_table_name}
                ({insert_into_table_columns_str + ", DW_CREATED_USER_ID, DW_CREATED_TIMESTAMP"})
            SELECT {select_from_staging_table_columns_str}, current_user(), current_timestamp()
            FROM {self.db_name}.{self.staging_schema_name}.{staging_table_name} as s
            """
        )
