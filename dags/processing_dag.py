import os
import shutil

import pandas as pd
from airflow.sdk import Asset, dag, task, task_group
from airflow.sensors.base import PokeReturnValue
from pendulum import datetime

processed_data_asset = Asset("file:///opt/airflow/data/processed/processed_data.csv")


@dag(
    schedule=None,
    description="Process TikTok reviews: detect files, validate, transform, and save",
    tags=["etl", "tiktok", "processing"],
)
def processing_dag():

    @task.sensor(poke_interval=30, timeout=600, mode="poke")
    def wait_for_file():
        input_path = "/opt/airflow/data/input/tiktok_google_play_reviews.csv"

        if os.path.exists(input_path):
            return PokeReturnValue(is_done=True, xcom_value=input_path)

        return PokeReturnValue(is_done=False)

    @task.branch
    def check_file_empty(file_path: str):
        if os.path.getsize(file_path) == 0:
            return "log_empty_file"
        return "process_data_group.replace_nulls"

    @task
    def log_empty_file():
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"File is empty at {timestamp}\n"

        with open("/opt/airflow/logs/empty_files.log", "a") as f:
            f.write(log_message)

        print(log_message)

    @task_group(tooltip="Data transformation tasks")
    def process_data_group(file_path: str):

        @task()
        def replace_nulls(file_path: str):
            from include import replace_nulls_logic

            df = pd.read_csv(file_path)
            df = replace_nulls_logic(df)

            temp_path = "/opt/airflow/data/temp_after_nulls.csv"
            df.to_csv(temp_path, index=False)

            print(f"Replaced nulls: {df.shape[0]} rows")
            return temp_path

        @task()
        def sort_by_date(temp_path: str):
            from include import sort_by_date_logic

            df = pd.read_csv(temp_path)
            df = sort_by_date_logic(df)

            temp_path = "/opt/airflow/data/temp_after_sort.csv"
            df.to_csv(temp_path, index=False)

            print(f"Sorted by date: {df.shape[0]} rows")
            return temp_path

        @task()
        def clean_content(temp_path: str):
            from include import clean_content_logic

            df = pd.read_csv(temp_path)
            df = clean_content_logic(df)

            temp_path = "/opt/airflow/data/temp_after_clean.csv"
            df.to_csv(temp_path, index=False)

            print(f"Cleaned content: {df.shape[0]} rows")
            return temp_path

        @task(outlets=[processed_data_asset])
        def save_to_dataset(temp_path: str):

            output_dir = "/opt/airflow/data/processed"
            os.makedirs(output_dir, exist_ok=True)

            output_path = f"{output_dir}/processed_data.csv"
            shutil.copy(temp_path, output_path)

            for temp_file in [
                "/opt/airflow/data/temp_after_nulls.csv",
                "/opt/airflow/data/temp_after_sort.csv",
                "/opt/airflow/data/temp_after_clean.csv",
            ]:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

            print(f"Data saved to {output_path}")
            print(f"Asset updated: {processed_data_asset.uri}")
            return output_path

        nulls_replaced = replace_nulls(file_path)
        sorted_data = sort_by_date(nulls_replaced)
        cleaned_data = clean_content(sorted_data)
        save_to_dataset(cleaned_data)

    file_path = wait_for_file()

    branch_result = check_file_empty(file_path)
    log_empty_file_task = log_empty_file()

    process_data_group_result = process_data_group(file_path)

    branch_result >> [log_empty_file_task, process_data_group_result]


processing_dag_instance = processing_dag()
