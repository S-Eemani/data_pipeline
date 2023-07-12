import os

import pytz

CREDENTIALS = {
    "s3": {
        "service_name": "s3",
        "region_name": "",
        "aws_access_key_id": "",
        "aws_secret_access_key": "",
    },
}

S3_BUCKET_NAME = " "
S3_PATH_UNVIEWED_FILES = os.path.join("unviewed_files")
S3_PATH_UNVIEWED_ERAM_FILES = os.path.join("unviewed_eram_files")

LOCAL_PATH_ROOT = os.path.join(os.getenv("AIRFLOW_HOME"), "dags")
LOCAL_PATH_UNVIEWED_FILES = os.path.join(LOCAL_PATH_ROOT, "unviewed_files")
LOCAL_PATH_UNVIEWED_ERAM_FILES = os.path.join(LOCAL_PATH_ROOT, "unviewed_Eram_files")

LOCAL_PATH_DOWNLOAD_FROM_S3 = os.path.join(LOCAL_PATH_ROOT, "unviewed_files_from_s3")


TIMEZONE_CST = pytz.timezone(" ")
