from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.sql_create_tables import CreateSqlQueries
from helpers.sql_insert_queries import InsertSqlQueries

import logging

class LoadFactOperator(BaseOperator):
    """
    Description: 
        Class that loads staging data into fact songplays table
    
    Init Arguments:
        redshift_conn_id - Connection string for Redshift instances
    """
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 test,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        
        self.test = test
        

    def execute(self, context):
        """
        Description:
            Main execution function
        
        Arguments: 
            context - Adding context metadata to function
           
        returns:
            None
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #Create songplays table
        redshift.run(CreateSqlQueries.create_songplays_table)
        if self.test == "true":
            redshift.run("""DELETE FROM songplays;""") #remove in prod
        
        #Run insert into select statement, loading data from
        redshift.run(InsertSqlQueries.songplay_table_insert)
