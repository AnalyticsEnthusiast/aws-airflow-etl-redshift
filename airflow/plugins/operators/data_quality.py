from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.sql_create_tables import CreateSqlQueries
from helpers.sql_insert_queries import InsertSqlQueries

import logging

class DataQualityOperator(BaseOperator):
    """
    Description: Class to check if the data has been loaded correctly but ETL pipeline.
    
    Init Arguments: 
        redshift_conn_id - Connection string to Redshift
    """
    ui_color = '#89DA59'

    tables = ['songs', 'artists', 'time', 'users', 'songplays']
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 rows_greater_than_0,
                 check_duplicates,
                 top_10_users_by_distinct_session,
                 *args,
                 **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        
        self.rows_greater_than_0 = rows_greater_than_0
        self.check_duplicates = check_duplicates
        self.top_10_users_by_distinct_session = top_10_users_by_distinct_session

        
    def number_rows_greater_than_zero(self, redshift):
        """
        Description:
            Checks if the number of rows is greater than zero in each table
            
        Arguments:
            redshift - Redshift connection string
            
        Returns:
            None
        """
        for table in DataQualityOperator.tables:
            
            sql_1 = self.rows_greater_than_0.format(table)
            records = redshift.get_records(sql_1)
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
                
            num_records = records[0][0]
            logging.info(num_records)
            
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
        
    
    
    def check_duplicates_func(self, redshift):
        """
        Description:
            Checks for duplicate rows in Artist and user tables
        
        Arguments:
            redshift - Redshift Connection String
        
        Returns:
            None
        """
        for i in ['artist', 'song']:
            
            sql_2 = self.check_duplicates.format(i, i, i)
            duplicate_check = redshift.get_records(sql_2)
            
            logging.info(f"Number of rows = {duplicate_check[0][0]}")
            logging.info(f"Number of Distinct rows = {duplicate_check[0][1]}")
            
            if duplicate_check[0][0] != duplicate_check[0][1]:
                raise ValueError(f"Duplicates found in {i}")
    
    
    def check_sample_query(self, redshift):
        """
        Description:
            Check if Sample query returns expected result
        
        Arguments:
            redshift - Redshift connection string
        
        Returns:
            None
        """
        top10 = redshift.get_records(self.top_10_users_by_distinct_session)
        
        logging.info(f"Raw data -> {top10}")
        
        if top10[0][0] != "Ryan" or top10[0][1] != "Smith":
            raise ValueError("Expected First Row incorrect")
       
                
    def execute(self, context):
        """
        Description:
            Main execution function
        
        Argument:
            context - Adding context metadata to function
        
        Returns:
            None
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #1. Check that the number of rows is greater than 0
        self.number_rows_greater_than_zero(redshift)
        
        #2. Artist table and user table may have duplicates
        self.check_duplicates_func(redshift)
        
        #3. Run a predefined query and check the result
        self.check_sample_query(redshift)
    
    
    
    