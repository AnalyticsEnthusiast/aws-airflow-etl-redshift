from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.subdag import SubDagOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from helpers.sql_create_tables import CreateSqlQueries
from helpers.sql_insert_queries import InsertSqlQueries


default_args = {
    'owner': 'Sparkify',
    'start_date': datetime.now(),
}

dag = DAG('Sparkify_check_test',
          default_args=default_args,
          description='Check Sparkify Data Quality',
          #schedule_interval='0 * * * *'
        )


start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    dag=dag
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> run_quality_checks
run_quality_checks >> end_operator