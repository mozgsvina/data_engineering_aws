from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
import final_project_sql_statements


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.datetime(2018, 11, 1, 0, 0, 0, 0),
    'depends_on_past': False,
    'retries': 3,
    'catchup': False,
    'email_on_retry': False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        table="staging_events",
        create_table_sql=final_project_sql_statements.SqlQueries.staging_events_table_create,
        s3_key="log-data/",
        # for loading timestamped files from S3 based on the execution time:
        # s3_key = "log-data/{{ execution_date.year }}/{{ execution_date.month }}/{{ execution_date.strftime('%Y-%m-%d') }}-events.json",
        json_format="s3://airflow-bucket-anya/log_json_path.json",
        
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        table="staging_songs",
        create_table_sql=final_project_sql_statements.SqlQueries.staging_songs_table_create,
        s3_key="song-data/"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table="users",
        sql_create=final_project_sql_statements.SqlQueries.user_table_create,
        sql_insert=final_project_sql_statements.SqlQueries.user_table_insert,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table="songs",
        sql_create=final_project_sql_statements.SqlQueries.song_table_create,
        sql_insert=final_project_sql_statements.SqlQueries.song_table_insert,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table="artists",
        sql_create=final_project_sql_statements.SqlQueries.artist_table_create,
        sql_insert=final_project_sql_statements.SqlQueries.artist_table_insert,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table="time",
        sql_create=final_project_sql_statements.SqlQueries.time_table_create,
        sql_insert=final_project_sql_statements.SqlQueries.time_table_insert,
    )

    run_quality_checks = DataQualityOperator(
        # accepts COUNT type SQL queries and results
        task_id='Run_data_quality_checks',
        test_cases = {"SELECT COUNT(*) FROM artists WHERE name IS NULL;": 0,
                      "SELECT COUNT(*)FROM users WHERE first_name IS NULL;": 0,
        }
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table
    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()