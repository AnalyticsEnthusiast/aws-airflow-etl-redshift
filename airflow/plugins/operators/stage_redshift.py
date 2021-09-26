from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.sql_create_tables import CreateSqlQueries
from helpers.sql_insert_queries import InsertSqlQueries

import logging

class StageToRedshiftOperator(BaseOperator):
    """
    Description: Class that handles loading data from S3 into Redshift staging tables.
    
    Manditory Arguments:
        aws_credentials - String name of AWS credentials object
        redshift_conn_id - String name of Redshift cluster name
        table - String name of staging table
        s3_bucket - String of bucket name in S3
        s3_key - String of key path in S3
    """
    ui_color = '#358140'
    
    path=""
    
    copy_to_redshift = """
           COPY {}
           FROM 's3://{}/{}'
           CREDENTIALS 'aws_access_key_id={};aws_secret_access_key={}'
           REGION 'us-west-2'
           JSON '{}';
        """

    @apply_defaults
    def __init__(self,
                 aws_credentials,
                 redshift_conn_id,
                 table,
                 s3_bucket,
                 s3_key,
                 year,
                 month,
                 *args, 
                 **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        
        self.year = year
        self.month = month
        
        if self.table == "staging_events":
            self.s3_key= s3_key + str(self.year) + "/" + str(self.month) + "/"
        elif self.table == "staging_songs":
            self.s3_key = s3_key
        
        logging.info(self.s3_bucket)
        logging.info(self.s3_key)
            
    def execute(self, context):
        """
        Description: Main Execution function for class
        
        Arguments:
            context - Adding context metadata to function
        
        Returns:
            None
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws = AwsHook(self.aws_credentials)
        credentials = aws.get_credentials()
        
        if self.table == "staging_events":
            redshift.run(CreateSqlQueries.create_staging_events_table)
            redshift.run("""DELETE FROM staging_events;""")
            path = f"s3://{self.s3_bucket}/log_json_path.json" 
        if self.table == "staging_songs":
            redshift.run(CreateSqlQueries.create_staging_songs_table)
            redshift.run("""DELETE FROM staging_songs;""")
            path = "auto"
        
        
        sql = StageToRedshiftOperator.copy_to_redshift.format(
            self.table,
            self.s3_bucket,
            self.s3_key,
            credentials.access_key,
            credentials.secret_key,
            path
        )
        
        redshift.run(sql)





