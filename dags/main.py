from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_videos_ids, extract_video_data, save_to_json

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

with DAG(
    dag_id='produdce_json',
    default_args=default_args,
    description='A DAG to extract YouTube video stats and save to JSON',
    schedule='0 14 * * *',  # Daily at 14:00 Athens time
    catchup=False,
) as dag:
    
    #defining the tasks

    playlist_id = get_playlist_id()
    
    video_ids = get_videos_ids(playlist_id)

    extracted_data = extract_video_data(video_ids)

    save_to_json_task=save_to_json(extracted_data)

    #defining the task dependencies
    playlist_id >> video_ids >> extracted_data >> save_to_json_task