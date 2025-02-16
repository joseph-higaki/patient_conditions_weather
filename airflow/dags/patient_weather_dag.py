from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import sys

# Default arguments for the DAG
default_args = {    
    "depends_on_past": False    
}

# Define the DAG
with DAG(
    dag_id="patient_weather_DAG",
    default_args=default_args,
    description="Patient ingestion and transformation pipeline",
    schedule_interval= timedelta(minutes=31),
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=30),
    catchup=False,
) as dag:
     
    def extract_year_coordinates(emr_path: str, event_years_coordinates_filepath:str):
        print(emr_path)
        print(event_years_coordinates_filepath)
        print(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))                 
        extractor = EventYearCoordinatesExtractor(emr_path, event_years_coordinates_filepath)        
        extractor.extract()
    
    def extract_weather_data(event_years_coordinates_filepath: str, destination_bucket_name: str, destination_path: str):
        extractor = EventWeatherDataExtractor(event_years_coordinates_filepath, destination_bucket_name, destination_path)        
        extractor.extract()

    upload_emr_files_task = LocalFilesystemToGCSOperator(
            task_id="upload_emr_files",
            src=os.getenv("SOURCE_PATH")+"/*",
            dst=os.getenv("EMR_RAW_PATH") + "/",
            bucket=os.getenv("EMR_RAW_BUCKET")
        )
     
    extract_year_coordinates_task = PythonOperator( 
        task_id="extract_year_coordinates",
        python_callable=extract_year_coordinates,
        op_kwargs={
            "emr_path": f"gs://{os.getenv('EMR_RAW_BUCKET')}/{os.getenv('EMR_RAW_PATH')}",
            "event_years_coordinates_filepath": f"gs://{os.getenv('WEATHER_BUCKET')}/{os.getenv('EVENT_YEARS_COORDINATES_FILEPATH')}"
        }
    )    

    extract_weather_data_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_weather_data,
        op_kwargs={
            "event_years_coordinates_filepath": f"gs://{os.getenv('WEATHER_BUCKET')}/{os.getenv('EVENT_YEARS_COORDINATES_FILEPATH')}",
            "destination_bucket_name": os.getenv("WEATHER_BUCKET"),
            "destination_path": os.getenv("WEATHER_DATA_DAILY_PATH")
        }
    )

    #upload_emr_files_task >> extract_year_coordinates_task >> 
    extract_weather_data_task

# ********************************************************************************************
# TO-DO: proper package management 
# ********************************************************************************************
SCRIPT_PATHS_ENV_VAR_NAMES = ["EXTRACT_SCRIPTS_PATH"]

for script_path_var_name in SCRIPT_PATHS_ENV_VAR_NAMES:
    scripts_path = os.getenv(script_path_var_name) # used from container
    if not scripts_path:
        from dotenv import load_dotenv, find_dotenv
        a = find_dotenv()
        load_dotenv(a) #find adjacent .env
        scripts_path = os.getenv(script_path_var_name) # used from local
    print(f"{script_path_var_name} = {scripts_path}") 
    sys.path.insert(0, scripts_path)

# Import the functions to be executed
from event_year_coordinates_extractor import EventYearCoordinatesExtractor
from event_weather_data_extractor import EventWeatherDataExtractor
