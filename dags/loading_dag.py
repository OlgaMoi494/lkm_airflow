import os

from airflow.sdk import dag, task
from processing_dag import processed_data_asset


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

        csv_path = "/opt/airflow/data/processed/processed_data.csv"

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"File not found: {csv_path}")

        df = pd.read_csv(csv_path)

        records = df.to_dict("records")

        hook = MongoHook(conn_id="mongo_default")
        collection_name = os.getenv("MONGO_COLLECTION", "processed_data")
        mongo_db = os.getenv("MONGO_DB", "airflow_db")
        conn = hook.get_conn()
        conn[mongo_db][collection_name].delete_many({})
        hook.insert_many(
            mongo_collection=collection_name,
            docs=records,
            mongo_db=mongo_db,
        )

        print(f"Loaded {len(records)} records to MongoDB")

        return len(records)

    load_to_mongodb()


loading_dag_instance = loading_dag()
