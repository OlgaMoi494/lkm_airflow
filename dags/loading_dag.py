import os

import structlog
from airflow.sdk import Variable, dag, task
from processing_dag import processed_data_asset

logger = structlog.get_logger()


@dag(
    schedule=[processed_data_asset],
    description="Load processed TikTok data to MongoDB",
    tags=["etl", "mongodb", "loading"],
    catchup=False,
)
def loading_dag():

    @task
    def load_to_mongodb():
        import pandas as pd
        from airflow.providers.mongo.hooks.mongo import MongoHook

        data_base_path = Variable.get("data_base_path", default="/opt/airflow/data")
        processed_file_name = Variable.get(
            "processed_file_name", default="processed_data.csv"
        )
        csv_path = os.path.join(data_base_path, "processed", processed_file_name)

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"File not found: {csv_path}")

        df = pd.read_csv(csv_path)

        records = df.to_dict("records")

        mongo_conn_id = Variable.get("mongo_conn_id", default="mongo_default")
        hook = MongoHook(conn_id=mongo_conn_id)
        collection_name = Variable.get("mongo_collection", default="processed_data")
        mongo_db = Variable.get("mongo_db", default="airflow_db")
        conn = hook.get_conn()
        conn[mongo_db][collection_name].delete_many({})
        hook.insert_many(
            mongo_collection=collection_name,
            docs=records,
            mongo_db=mongo_db,
        )

        logger.info(
            "Records loaded to MongoDB",
            rows=len(records),
            collection=collection_name,
            database=mongo_db,
        )

        return len(records)

    load_to_mongodb()


loading_dag_instance = loading_dag()
