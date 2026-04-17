from airflow import DAG
from datawarehouse.dwh import core_table, staging_table
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from dataquality.soda import yt_elt_data_quality

# Define the local timezone
local_tz = pendulum.timezone("Europe/Berlin")

# Default Args
default_args = {
    "owner": "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "dataengineers@example.com",
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2024, 6, 1, tzinfo=local_tz),
}

# Variables
staging_schema = "staging"
core_schema = "core"
with DAG(
    dag_id = 'produce_json',
    default_args=default_args,
    description='A DAG to extract video stats from YouTube and save as JSON',
    schedule = '0 14 * * *',
    catchup = False
) as dag:
    
    #define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_data)

    # define dependencies
    playlist_id >> video_ids >> extracted_data >> save_to_json_task

with DAG(
    dag_id = 'update_db',
    default_args=default_args,
    description='DAG to process JSON file and insert data into bioth staging and core schema',
    schedule = '0 15 * * *',
    catchup = False
) as dag:
    
    #define tasks
    update_staging = staging_table()
    update_core = core_table()

    # define dependencies
    update_staging >> update_core

with DAG(
    dag_id = 'data_quality',
    default_args=default_args,
    description='DAG to check the data quality on both layers in the db',
    schedule = '0 16 * * *',
    catchup = False
) as dag:
    
    #define tasks
    soda_validation_staging = yt_elt_data_quality(schema=staging_schema)
    soda_validation_core = yt_elt_data_quality(schema=core_schema)
    # define dependencies
    soda_validation_staging >> soda_validation_core