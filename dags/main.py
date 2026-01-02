from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_videos_ids, extract_video_data, save_to_json
from DWH.dwh import staging_table, core_table
from dataquality.soda import yt_elt_data_quality
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


local_tz = pendulum.timezone("Europe/Athens")


default_args = {
    'owner': 'dataengineers',
    'depends_on_past': False,
    'email_on_failure': False, 
    'email_on_retry': False,
    'email': 'dataengineers@example.com',
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(minutes=60),
    'start_date': datetime(2025, 12, 31, tzinfo=local_tz),
    #'end_date': datetime(2026, 1, 5, tzinfo=local_tz)
}

staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id='produdce_json',
    default_args=default_args,
    description='A DAG to extract YouTube video stats and save to JSON',
    schedule='0 14 * * *',  # Daily at 14:00 Athens time
    catchup=False,
) as dag_produce:
    
    #defining the tasks

    playlist_id = get_playlist_id()
    
    video_ids = get_videos_ids(playlist_id)

    extracted_data = extract_video_data(video_ids)

    save_to_json_task=save_to_json(extracted_data)


    trigger_update_db = TriggerDagRunOperator(
        task_id="trigger_update_db",
        trigger_dag_id= "update_db"
    )


    #defining the task dependencies
    playlist_id >> video_ids >> extracted_data >> save_to_json_task >> trigger_update_db


with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='A DAG to process JSon data and update DWH both schemas',
    schedule=None,
    catchup=False,
) as dag_update:
    
    #defining the tasks

    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id="trigger_data_quality",
        trigger_dag_id= "data_quality"
    )
    
    

    #defining the task dependencies
    update_staging >> update_core>>trigger_data_quality


with DAG(
    dag_id='data_quality',
    default_args=default_args,
    description='A DAG to check the data quality of DWH both schemas',
    schedule=None,
    catchup=False,
) as dag_quality:
    
    #defining the tasks

    soda_validate_staging = yt_elt_data_quality(staging_schema)
    soda_validate_core = yt_elt_data_quality(core_schema)
    

    #defining the task dependencies
    soda_validate_staging >> soda_validate_core