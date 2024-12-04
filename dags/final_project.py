from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (
    DataQualityOperator,
    LoadDimensionOperator,
    LoadFactOperator,
    StageToRedshiftOperator,
)

default_args = {
    "owner": "udacity",
    "depends_on_past": False,
    "start_date": pendulum.now(),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "catchup": False,
}


@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
)
def final_project():

    start_operator = DummyOperator(task_id="Begin_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id="redshift",
        s3_path="s3://karim-tayie/log-data",
        table_name="staging_events",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id="redshift",
        s3_path="s3://karim-tayie/song-data",
        table_name="staging_songs",
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table", conn_id="redshift"
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table", dimension="user", conn_id="redshift"
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table", dimension="song", conn_id="redshift"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table", dimension="artist", conn_id="redshift"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table", dimension="time", conn_id="redshift"
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        conn_id="redshift",
        test_queries=["SELECT COUNT(*) FROM songplays WHERE userid IS NULL", "SELECT COUNT(*) FROM users WHERE userid IS NULL"],
        expected_results=[0, 0],
    )
    
    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_artist_dimension_table, load_song_dimension_table, load_time_dimension_table, load_user_dimension_table]
    [load_artist_dimension_table, load_song_dimension_table, load_time_dimension_table, load_user_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator


final_project_dag = final_project()
