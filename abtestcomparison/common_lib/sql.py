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
            f' ${round(query_job.total_bytes_processed * 10.0 ** -12 * 6.25, 4)}')
        if (query_job.total_bytes_processed * 10.0 ** -12) > 1:
            logger.warning(
                f'This query will process more than 1TB --> {round(query_job.total_bytes_processed * 10 ** -12, 3)} TB'
                f' ~ ${round(query_job.total_bytes_processed * 6.25 * 10 ** -12, 4)}')

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
    def _format_bytes(bytes_processed: int) -> str:
        if bytes_processed < 1024 ** 2:
            return f"{bytes_processed / 1024:.1f} KB"
        elif bytes_processed < 1024 ** 3:
            return f"{bytes_processed / 1024 ** 2:.1f} MB"
        elif bytes_processed < 1024 ** 4:
            return f"{bytes_processed / 1024 ** 3:.2f} GB"
        else:
            return f"{bytes_processed / 1024 ** 4:.2f} TB"

    def print_cost_estimate(self, query: str, is_path: bool = False, query_parameters: dict = None) -> dict:
        """Run a dry-run cost check and print a BigQuery-style summary. Returns the full cost_info dict."""
        cost_info = self.check_query_cost(query=query, is_path=is_path, query_parameters=query_parameters)
        size_str = self._format_bytes(cost_info['bytes_processed'])
        print(f"This query will process {size_str} when run.")
        print(f"Estimated query cost: ${cost_info['estimated_cost']:.2f}")
        for warning in cost_info['warnings']:
            print(f"⚠️  {warning}")
        return cost_info

    @staticmethod
    def get_query_parameters_list(query: str, is_path: bool = True) -> list[str]:
        """List parameters defined in a given query statement. Paramenters are defined in brackets. ie; {param}"""
        if is_path:
            with open(query) as file:
                query = file.read()
        m = re.findall(r"\{([A-Za-z0-9_]+)\}", query)
        return list(set(m))

    def check_query_cost(self, query: str, is_path: bool = False, query_parameters: dict = None, 
                         cost_limit: float = None, bytes_limit: float = None) -> dict:
        """
        Estimates the cost and data processed for a query before execution.
        
        Args:
            query: SQL query string or path to SQL file
            is_path: If True, query is treated as a file path
            query_parameters: Dictionary of parameters to substitute in query
            cost_limit: Maximum cost in USD to allow (optional). If exceeded, returns warning.
            bytes_limit: Maximum bytes to process in TB (optional). If exceeded, returns warning.
        
        Returns:
            Dictionary containing:
                - 'query': The formatted query string
                - 'bytes_processed': Bytes that will be scanned
                - 'gb_processed': GB that will be scanned
                - 'estimated_cost': Estimated cost in USD
                - 'within_limits': Boolean indicating if within specified limits
                - 'warnings': List of warning messages
        """
        # Format the query
        query = self.reformat_query(query=query, is_path=is_path, query_parameters=query_parameters)
        
        # Run dry run to estimate bytes
        logger.debug(f'Running dry run...\n{query}')
        query_job = self.client.query(query=query, job_config=bigquery.QueryJobConfig(dry_run=True))
        
        bytes_processed = query_job.total_bytes_processed
        gb_processed = round(bytes_processed * 9.31 * 10 ** -10, 3)
        tb_processed = round(bytes_processed / 10 ** 12, 3)
        estimated_cost = round(bytes_processed * 10.0 ** -12 * 6.25, 4)  # $6.25 per TB (BigQuery on-demand)
        
        warnings = []
        within_limits = True
        
        # Check against cost limit
        if cost_limit is not None:
            if estimated_cost > cost_limit:
                warnings.append(f"Estimated cost ${estimated_cost} exceeds limit of ${cost_limit}")
                within_limits = False
        
        # Check against bytes/TB limit
        if bytes_limit is not None:
            if tb_processed > bytes_limit:
                warnings.append(f"Query will process {tb_processed} TB, exceeding limit of {bytes_limit} TB")
                within_limits = False
        
        # Standard warnings for high usage
        if tb_processed > 1:
            warnings.append(f"Query will process more than 1TB ({tb_processed} TB ~ ${round(tb_processed * 0.5, 3)})")
        
        return {
            'query': query,
            'bytes_processed': bytes_processed,
            'gb_processed': gb_processed,
            'tb_processed': tb_processed,
            'estimated_cost': estimated_cost,
            'within_limits': within_limits,
            'warnings': warnings
        }
    
# %%
