from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.sql_create_tables import CreateSqlQueries
from helpers.sql_insert_queries import InsertSqlQueries

import logging

class LoadDimensionOperator(BaseOperator):
    """
    Description:
        Class for loading dimension data into songs, artists, users & time tables
    
    Init Arguments:
        redshift_conn_id - Redshift connection string
        table - Dimension table name
        append_only - Option for appending to dimension table or truncating data before loading
    """
    truncate_table = """DELETE FROM public.{};"""
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 append_only,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        
        self.append_only = append_only
        logging.info(self.append_only)
    
    def truncate(self, redshift):
        """
        Description:
            Truncates table before loading
        
        Arguments:
            redshift - Redshift Connection String
        
        Returns:
            None
        """
        # Truncate dim table
        truncate_sql = LoadDimensionOperator.truncate_table.format(self.table)
        redshift.run(truncate_sql)
    
    
    def process(self, redshift):
        """
        Description:
            Runs the load steps for dimension tables
        
        Arguments:
            redshift - Redshift connection string object
        
        Returns:
            None
        """
        redshift.run(getattr(CreateSqlQueries, f"create_{self.table}_table"))
        if self.append_only == "false":
            self.truncate(redshift)
        redshift.run(getattr(InsertSqlQueries, f"{self.table}_table_insert"))
   

    def execute(self, context):
        """
        Description:
            Main Execution function
        
        Arguments:
            context - Adding context metadata to function
            
        Returns:
            None
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.process(redshift)
        
