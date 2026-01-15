#%%
import logging
import pandas as pd
from google.cloud import bigquery
import google.auth
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
#import os
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="auth.json"
import re

logger = logging.getLogger('BQC')


def list_to_filter(x: list) -> str:
    return f"({', '.join([str(x) in list])})"


class BigQueryConnector:
    def __init__(self):
        credentials, project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/drive",
                    "https://www.googleapis.com/auth/cloud-platform"
                    ])

        self.client = bigquery.Client(credentials=credentials, project=project)

    map_dytpes_sql_create = {'Int64': 'INT64',
                             'int64': 'INT64',
                             'date': 'DATE',
                             'float64': 'FLOAT64',
                             'datetime': 'DATE',
                             'datetime64[ns]': 'DATE',
                             'object': 'STRING',
                             'bool': 'BOOL',
                             'boolean': 'BOOL'}

    map_dytpes_sql_table_schema = {'Int64': 'INTEGER',
                                   'int64': 'INTEGER',
                                   'date': 'DATE',
                                   'float64': 'FLOAT',
                                   'datetime': 'DATE',
                                   'datetime64[ns]': 'DATE',
                                   'object': 'STRING',
                                   'bool': 'BOOLEAN',
                                   'boolean': 'BOOLEAN'}

    @staticmethod
    def reformat_query(query: str, is_path: bool, query_parameters: dict):
        """Replace query parameters for corresponding values. A parameter is indicated with brackets ie:{param}."""
        if query_parameters is None:
            query_parameters = {}
        if is_path:
            logger.info(f'Querying {query}')
            with open(query) as file:
                query = file.read()
        query = query.format(**query_parameters)
        return query

    def run_query(self, query: str, is_path: bool = False, query_parameters: dict = None):
        """
        Runs SQL statement defined by query. A parameter is defined using with brackets ie:{param}.
        If is_path is True query should be a path.
        """
        query = self.reformat_query(query=query, is_path=is_path, query_parameters=query_parameters)
        logger.debug(f'Querying...\n{query}')
        query_job = self.client.query(query=query)
        return query_job

    def get(self, query: str, is_path: bool = False, query_parameters: dict = None) -> pd.DataFrame:
        """
        Runs select SQL statement defined in query and gathers the results in a DataFrama. A parameter may be defined
        using with brackets ie:{param}.
        If is_path is True query should be a path.
        """
        query = self.reformat_query(query=query, is_path=is_path, query_parameters=query_parameters)
        logger.debug(f'Querying...\n{query}')
        query_job = self.client.query(query=query, job_config=bigquery.QueryJobConfig(dry_run=True))
        logger.info(
            f'This query will process {round(query_job.total_bytes_processed * 9.31 * 10 ** -10, 3)} GB ~'
            f' ${round(query_job.total_bytes_processed * 10.0 ** -12 * 0.5, 3)}')
        if (query_job.total_bytes_processed * 10.0 ** -12) > 1:
            logger.warning(
                f'This query will process more than 1TB --> {round(query_job.total_bytes_processed / 10 ** -12, 3)} TB'
                f' ~ ${round(query_job.total_bytes_processed / 5 * 10 ** -12, 3)}')

        query_job = self.client.query(query=query)
        df = query_job.result().to_dataframe()
        logger.debug(f'Query done')
        return df

    def get_table_dtypes(self, table_name: str) -> dict:
        """Returns a dict with an entry with column_name, data type for each column of the input table_name."""
        table_schema = {i.name: i.field_type for i in self.client.get_table(table_name).schema}
        return table_schema

    def upload_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        """Uploads the input df Dataframe to the given table_name.
        Note that f the table does not exist it will be created.
        """
        if not self.exists(table_name):
            logger.info(f"Table {table_name} does not exist, creating it from dataframe.")
            self.create_table_from_dtypes(df.dtypes, table_name)

        if not self.check_dtypes_compatibility(df, table_name):
            logging.error('Data types are not compatible between BQ table and dataframe.')
            raise AttributeError

        table = self.client.get_table(table_name)
        table_schema = [{'name': i.name, 'type': i.field_type} for i in table.schema]
        df = df[[i.name for i in table.schema]]

        project, dataset, table = table_name.split('.')
        logger.info(f"Uploading dataframe to {table_name}...")
        df.to_gbq(
            project_id=project,
            destination_table=f'{dataset}.{table}',
            table_schema=table_schema,
            if_exists='append')
        logger.info(f"Upload to {table_name} completed successfully")

    def exists(self, table_name: str) -> bool:
        """Returns True if the input table_name exists. """
        try:
            self.client.get_table(table_name)
            return True
        except NotFound:
            return False

    def check_dtypes_compatibility(self, df: pd.DataFrame, table_name: str) -> bool:
        """Retuns True if datatypes between the Dataframe df and the existing BQ table_name match. """
        table_schema = self.get_table_dtypes(table_name)
        dypes = df.dtypes
        for column in table_schema.keys():
            if column not in dypes.index:
                logging.info(f"Column not found: {column} in not in input DataFrame.")
                return False
            if (not self.map_dytpes_sql_table_schema[dypes[column].name] == table_schema[column]) or \
                (self.map_dytpes_sql_table_schema[dypes[column].name]=='object' and table_schema[column]=='STRING') or \
                (self.map_dytpes_sql_table_schema[dypes[column].name]=='boolean' and table_schema[column]=='BOOL'):
                logging.info(
                    f"Column type not matching: {column} is {dypes[column].name} but table is {table_schema[column]}.")
                return False
        return True

    def get_create_table_sql_statement(self, dtypes: pd.DataFrame.dtypes, table_name: str,
                                       partition_columns: list[str] = None) -> str:
        """Generates the SQL statement to create with column names and datatypes matching DataFrame.dtypes"""
        
        body = ''
        for column_name in dtypes.index:
            body = body + f'{column_name} {self.map_dytpes_sql_create[dtypes[column_name].name]}, \n'
        body = body[0:-3]

        partition_by = ''
        if partition_columns is not None:
            partition_by = f'PARTITION BY {partition_columns}'

        query = f"""CREATE TABLE {table_name} (\n{body} \n) \n {partition_by};"""
        return query

    def create_table_from_dtypes(self, dtypes: pd.DataFrame.dtypes, table_name: str,
                                 partition_columns: list[str] = None) -> None:
        """Create a new BQ table with  column names and datatypes matching DataFrame.dtypes"""

        if self.exists(table_name):
            logging.warning(f'Table {table_name} already exists')
        else:
            query = self.get_create_table_sql_statement(dtypes, table_name, partition_columns)
            logger.debug(f'\n{query}')
            self.client.query(query=query)

    def rename_table(self, current_table_name: str, new_table_name: str) -> None:
        self.client.query(query=f"ALTER TABLE {current_table_name} RENAME TO {new_table_name};")

    def copy_table(self, current_table_name: str, new_table_name: str) -> None:
        self.client.query(query=f"CREATE TABLE {new_table_name} COPY {current_table_name};")

    def drop_table(self, table_name: str) -> None:
        """Drop table table_name"""
        if self.exists(table_name):
            self.client.query(query=f"DROP TABLE {table_name};")
            logger.info(f"Table {table_name} dropped successfully")
        else:
            logging.warning(f'Table {table_name} cannot be dropped because it does not exist.')

    def add_column(self, table_name, column_name, data_type: str) -> None:
        f"""
        Ad the column column_name with datatype data_type to BQ table_name.
        * data_type: str [{self.map_dytpes_sql_create.values()}]
        """
        supported_dtypes = self.map_dytpes_sql_create.values()
        if data_type not in supported_dtypes:
            raise ValueError(f"data_type {data_type} not recognised. Supported datatypes: {supported_dtypes} data_type")
        self.client.query(query=f"ALTER TABLE {table_name} ADD COLUMN {column_name} {data_type};")

    @staticmethod
    def get_query_parameters_list(query: str, is_path: bool = True) -> list[str]:
        """List parameters defined in a given query statement. Paramenters are defined in brackets. ie; {param}"""
        if is_path:
            with open(query) as file:
                query = file.read()
        m = re.findall(r"\{([A-Za-z0-9_]+)\}", query)
        return list(set(m))


# %%
