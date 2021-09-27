import os
import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from helpers.sql_create_tables import CreateSqlQueries
from helpers.sql_insert_queries import InsertSqlQueries
from helpers.sql_quality_check import DataQualitySqlQueries


START_DATE = datetime.now()-timedelta(days=7)

default_args = {
    "owner": "Sparkify",
    "start_date": START_DATE,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup_by_default": False
}


with DAG("Sparkify_Data_Warehouse_ETL",
          default_args=default_args,
          description="Load and transform data in Redshift with Airflow",
          schedule_interval="0 * * * *",
          #schedule_interval=None
        ) as dag:
    
    
    start_operator = DummyOperator(
        task_id="Begin_execution"
    )

    
    # Hard coding year and month as proof of concept
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        table="staging_events",
        aws_credentials="aws_credentials",
        redshift_conn_id="redshift",
        s3_bucket="udacity-dend",
        s3_key="log_data/",
        year="2018", # Hard Coded as 2018 for POC
        month="11", # Hard Coded as 11 for POC
        params={
            "year": "",
            "month": "" 
        }
    )

    
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        table="staging_songs",
        aws_credentials="aws_credentials",
        redshift_conn_id="redshift",
        s3_bucket="udacity-dend",
        s3_key="song_data/",
        year="2018", # Hard Coded as 2018 for POC
        month="11", # Hard Coded as 11 for POC
        params={
            "year": "",
            "month": "" 
        }
    )

    
    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        test="true", # Hard coded as true
        params={
            "test": "false"
        }
    )

    
    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        table="users",
        append_only="",
        params={
            "append_only": "false"
        }
    )

    
    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        table="songs",
        append_only="",
        params={
            "append_only": "false"
        }
    )

    
    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        table="artists",
        append_only="",
        params={
            "append_only": "false"
        }
    )

    
    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        table="time",
        append_only="",
        params={
            "append_only": "false"
        }
    )

    
    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id="redshift",
        rows_greater_than_0 = DataQualitySqlQueries.rows_greater_than_0,
        check_duplicates = DataQualitySqlQueries.check_duplicates,
        top_10_users_by_distinct_session = DataQualitySqlQueries.top_10_users_by_distinct_session
    )

    
    end_operator = DummyOperator(
        task_id="Stop_execution"
    )

    start_operator >> [stage_songs_to_redshift, stage_events_to_redshift]
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
    [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator
    
