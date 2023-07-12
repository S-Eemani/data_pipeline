from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from pipeline_utils.callables import (
    _delete_locally,
    _download_from_api,
    _upload_to_s3,
)
from pipeline_utils.constants import TIMEZONE_CST

with DAG(
    "data_pipeline",
    start_date=datetime(2022, 1, 1, 0, 0, 0, tzinfo=TIMEZONE_CST),
    schedule="0 0 * * *",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
):
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    with TaskGroup(group_id="unviewed_eram_files") as unviewed_eram_files:
        download_from_api = PythonOperator(
            task_id="download_from_api",
            python_callable=_download_from_api,
            op_kwargs=dict(api_endpoint="unviewed_eram_files"),
        )
        upload_to_s3 = PythonOperator(
            task_id="upload_to_s3",
            python_callable=_upload_to_s3,
            op_kwargs=dict(api_endpoint="unviewed_eram_files"),
        )
        delete_locally = PythonOperator(
            task_id="delete_locally",
            python_callable=_delete_locally,
            op_kwargs=dict(api_endpoint="unviewed_eram_files"),
        )

        (download_from_api >> upload_to_s3 >> delete_locally)

    with TaskGroup(group_id="unviewed_files") as unviewed_files:
        download_from_api = PythonOperator(
            task_id="download_from_api",
            python_callable=_download_from_api,
            op_kwargs=dict(api_endpoint="unviewed_files"),
        )
        upload_to_s3 = PythonOperator(
            task_id="upload_to_s3",
            python_callable=_upload_to_s3,
            op_kwargs=dict(api_endpoint="unviewed_files"),
        )
        delete_locally = PythonOperator(
            task_id="delete_locally",
            python_callable=_delete_locally,
            op_kwargs=dict(api_endpoint="unviewed_files"),
        )

        (download_from_api >> upload_to_s3 >> delete_locally)

    start >> [unviewed_eram_files, unviewed_files] >> end