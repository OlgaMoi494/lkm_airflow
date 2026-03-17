import os
import shutil

import pandas as pd
import structlog
from airflow.sdk import Asset, Variable, dag, task, task_group
from airflow.sdk.bases.sensor import PokeReturnValue

from include import clean_content_logic, replace_nulls_logic, sort_by_date_logic

logger = structlog.get_logger()

data_base_path = Variable.get("data_base_path", default="/opt/airflow/data")
processed_file_name = Variable.get("processed_file_name", default="processed_data.csv")
processed_data_asset = Asset(
    f"file://{os.path.join(data_base_path, 'processed', processed_file_name)}"
)


@dag(
    schedule=None,
    description="Process TikTok reviews: detect files, validate, transform, and save",
    tags=["etl", "tiktok", "processing"],
)
def processing_dag():

    input_file_name = Variable.get(
        "input_file_name", default="tiktok_google_play_reviews.csv"
    )

    temp_path_after_nulls = os.path.join(data_base_path, "temp_after_nulls.csv")
    temp_path_after_sort = os.path.join(data_base_path, "temp_after_sort.csv")
    temp_path_after_clean = os.path.join(data_base_path, "temp_after_clean.csv")

    @task.sensor(poke_interval=30, timeout=600, mode="poke")
    def wait_for_file():
        input_path = os.path.join(data_base_path, "input", input_file_name)

        if os.path.exists(input_path):
            return PokeReturnValue(is_done=True, xcom_value=input_path)

        return PokeReturnValue(is_done=False)

    @task.branch
    def check_file_empty(file_path: str):
        if os.path.getsize(file_path) == 0:
            return "log_empty_file"
        return "process_data_group.replace_nulls"

    @task
    def log_empty_file(ti=None, dag=None, logical_date=None):
        logger.warning(
            "Empty file detected",
            dag_id=dag.dag_id,
            task_id=ti.task_id,
            logical_date=logical_date,
        )

    @task_group(tooltip="Data transformation tasks")
    def process_data_group(file_path: str):

        @task()
        def replace_nulls(file_path: str):

            df = pd.read_csv(file_path)
            df = replace_nulls_logic(df)

            df.to_csv(temp_path_after_nulls, index=False)

            logger.info(
                "Null values replaced",
                rows=df.shape[0],
                file_path=temp_path_after_nulls,
            )
            return temp_path_after_nulls

        @task()
        def sort_by_date(temp_path: str):

            df = pd.read_csv(temp_path)
            df = sort_by_date_logic(df)

            df.to_csv(temp_path_after_sort, index=False)

            logger.info(
                "Sorted by date", rows=df.shape[0], file_path=temp_path_after_sort
            )
            return temp_path_after_sort

        @task()
        def clean_content(temp_path: str):

            df = pd.read_csv(temp_path)
            df = clean_content_logic(df)

            df.to_csv(temp_path_after_clean, index=False)

            logger.info(
                "Content cleaned", rows=df.shape[0], file_path=temp_path_after_clean
            )
            return temp_path_after_clean

        @task(outlets=[processed_data_asset])
        def save_to_dataset(temp_path: str):

            output_dir = os.path.join(data_base_path, "processed")
            os.makedirs(output_dir, exist_ok=True)

            output_path = os.path.join(output_dir, processed_file_name)
            shutil.copy(temp_path, output_path)

            for temp_file in [
                temp_path_after_nulls,
                temp_path_after_sort,
                temp_path_after_clean,
            ]:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

            logger.info("Data saved to dataset", output_path=output_path)

            logger.info("Asset updated", uri=processed_data_asset.uri)
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
