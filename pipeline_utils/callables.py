from airflow.models import Variable


def _download_from_api(api_endpoint):
    import traceback

    from pipeline_utils.api_callables import API
    from pipeline_utils.constants import (
        LOCAL_PATH_UNVIEWED_ERAM_FILES,
        LOCAL_PATH_UNVIEWED_FILES,
    )
    """
    This method is used to download the files from the apiendpoint.
    Args:
        api_enpoint(str): The name of the specific endpoint, from where we are expecting the files from.
    """
    

    api = API(
        username=Variable.get(key="api_username"),
        password=Variable.get(key="api_password"),
        host_url=Variable.get(key="api_host_url"),
    )

    if api_endpoint == "unviewed_eram_files":
        files = api.get_unviewed_eram_files()
        files_list = files["filename"].to_list()
        save_dir_local = LOCAL_PATH_UNVIEWED_ERAM_FILES
    else:
        files = api.get_unviewed_files()
        files_list = files["filename"].to_list()
        save_dir_local = LOCAL_PATH_UNVIEWED_FILES

    file_count = 0
    for file in files_list:
        try:
            print("Downloading ", file)
            file_count += 1
            api.get_file(filename=file, save_dir=save_dir_local)
        except Exception as error:
            error_traceback = traceback.format_exc()
            print(file)
            print(error_traceback)
            print()

    print("--------------------------------------------------------------------------------")
    print(f"Total number of {api_endpoint} downloaded from api: {file_count}")
    print("--------------------------------------------------------------------------------")


def compare_contents(
    filename,
    S3_BUCKET_NAME,
    aws_access_key_id,
    aws_secret_access_key,
    S3_PATH,
    LOCAL_PATH,
):
"""
    This method is used to compare the data of files coming from the endpoint and the files that are already present. The logic considers 2 cases:
    - For files with same file name, only the files, for which the data has been modified/updated will be uploaded, with a modification in the name. We will be adding the datastamp to the filename of the modified/updated file 
    - For files with different file name, it will be uploaded as is.
    Args:
        filename (str): Name of the file that has been downloaded.
        S3_BUCKET_NAME (str): Name of the BUCKET the files are stored in S3.
        aws_access_key_id (str): Access key ID of the AWS account.
        aws_secret_access_key (str): Secret key of the AWS account.
        S3_PATH: The path of the folder in which the file, downloaded from a specific api_endpoint should be saved.
        LOCAL_PATH: The path where the files should be downloaded locally
    """
    import filecmp
    import os

    import boto3
    from pipeline_utils.constants import (
        CREDENTIALS,
        LOCAL_PATH_DOWNLOAD_FROM_S3,
    )

    bucket = S3_BUCKET_NAME
    access_key_id = aws_access_key_id
    secret_access_key = aws_secret_access_key
    s3_object = boto3.resource(
        "s3",
        region_name=CREDENTIALS.get("s3").get("region_name"),
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
    )
    obj = s3_object.Object(bucket, os.path.join(S3_PATH, filename))
    s3_file_data_encode = obj.get()["Body"].read()

    if ".pdf" in filename:
        # downloading the files from s3 and then comparing the contents
        get_pdf_file_from_s3 = boto3.client(
            "s3",
            region_name=CREDENTIALS.get("s3").get("region_name"),
            aws_access_key_id=CREDENTIALS.get("s3").get("aws_access_key_id"),
            aws_secret_access_key=CREDENTIALS.get("s3").get("aws_secret_access_key"),
        )
        get_pdf_file_from_s3.download_file(
            Bucket=S3_BUCKET_NAME,
            Key=os.path.join(S3_PATH, filename),
            Filename=os.path.join(LOCAL_PATH_DOWNLOAD_FROM_S3, filename),
        )

        # return TRUE if the contents are same
        value = filecmp.cmp(
            os.path.join(LOCAL_PATH, filename),
            os.path.join(LOCAL_PATH_DOWNLOAD_FROM_S3, filename),
        )
        if os.path.exists(os.path.join(LOCAL_PATH_DOWNLOAD_FROM_S3, filename)):
            os.remove(os.path.join(LOCAL_PATH_DOWNLOAD_FROM_S3, filename))
        return value

    else:
        # only reading and decoding the contents from S3
        #return TRUE if contents are same
        read_content_from_s3 = s3_file_data_encode.decode("utf-8")
        with open(os.path.join(LOCAL_PATH, filename), "r") as file:
            read_data_from_local = file.read().rstrip()
        return [text for text in read_content_from_s3 if text.isalpha()] == [text for text in read_data_from_local if text.isalpha()]


def _upload_to_s3(api_endpoint):
    """
    This method uploads the downloaded file to S3. While uploading, it notes down if the file contents are modified or not and the same logic will be uploaded to snowflake for reference purposes.
    Args:
        api_enpoint(str): Depending on the api_endpoint, the path in S3 will change.
    """
    import os
    import time

    import boto3
    import pandas as pd
    from pipeline_utils.constants import (
        CREDENTIALS,
        LOCAL_PATH_UNVIEWED_ERAM_FILES,
        LOCAL_PATH_UNVIEWED_FILES,
        S3_BUCKET_NAME,
        S3_PATH_UNVIEWED_ERAM_FILES,
        S3_PATH_UNVIEWED_FILES,
    )

    from pipeline_utils.SnowflakeConnection import SnowflakeConnection

    snowflake_connection = SnowflakeConnection(
        airflow=True,
        account=Variable.get(key="snowflake_account"),
        username=Variable.get(key="snowflake_username"),
        password=Variable.get(key="snowflake_password"),
        role_name=Variable.get(key="snowflake_role_name"),
        db_name=Variable.get(key="snowflake_db_name"),
        staging_schema_name=Variable.get(key="snowflake_api_staging_schema_name"),
        raw_schema_name=Variable.get(key="snowflake_apexedi_raw_schema_name"),
        history_schema_name=Variable.get(key="snowflake_api_history_schema_name"),
    )

    s3_client = boto3.client(**CREDENTIALS["s3"])
    aws_access_key_id = CREDENTIALS.get("s3").get("aws_access_key_id")
    aws_secret_access_key = CREDENTIALS.get("s3").get("aws_secret_access_key")

    bucket_name = S3_BUCKET_NAME

    if api_endpoint == "unviewed_eram_files":
        bucket_folder = S3_PATH_UNVIEWED_ERAM_FILES
        input_dir = LOCAL_PATH_UNVIEWED_ERAM_FILES
        snowflake_staging_table_name = "UNVIEWED_ERAM_FILES"
        snowflake_raw_table_name = "UNVIEWED_ERAM_FILES"
        snowflake_history_table_name = "UNVIEWED_ERAM_FILES"
        local_path = LOCAL_PATH_UNVIEWED_ERAM_FILES
    else:
        bucket_folder = S3_PATH_UNVIEWED_FILES
        input_dir = LOCAL_PATH_UNVIEWED_FILES
        snowflake_staging_table_name = "UNVIEWED_FILES"
        snowflake_raw_table_name = "UNVIEWED_FILES"
        snowflake_history_table_name = "UNVIEWED_FILES"
        local_path = LOCAL_PATH_UNVIEWED_FILES

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    # creating a list of all the files present in the S3
    files_in_s3 = []
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=bucket_folder)
    for page in page_iterator:
        if page["KeyCount"] > 0:
            for item in page["Contents"]:
                file_name = item["Key"]
                final_file_name = file_name.split("/")[-1]
                files_in_s3.append(final_file_name)
    # Initializing the Dataframe
    df_rows = []

    # checking and uploading for each file in the input_dir
    file_count = 0
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            data = {"Date": time.strftime("%Y%m%d-%H%M%S"), "File_Name": file}

            if file in files_in_s3:
                # File exists, we have to check if contents match or not
                value = compare_contents(
                    filename=file,
                    S3_BUCKET_NAME=bucket_name,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                    S3_PATH=bucket_folder,
                    LOCAL_PATH=local_path,
                )
                if value == True:
                    print(file, "contents are same")
                    data["File_Exists_in_S3"] = "True"
                    data["Contents_Modified"] = "False"
                    data["Modified_File_Name"] = " "
                else:
                    print(file, "contents are not same")
                    # timestr = time.strftime("%Y%m%d-%H%M%S")
                    timestr = time.strftime("%Y%m%d")
                    modified_file_name = timestr + "-" + file.split("-")[-1]
                    os.rename(
                        os.path.join(input_dir, file),
                        os.path.join(input_dir, modified_file_name),
                    )
                    data["File_Exists_in_S3"] = "True"
                    data["Contents_Modified"] = "True"
                    data["Modified_File_Name"] = modified_file_name
                    s3_client.upload_file(
                        # local path with the filename
                        Filename=os.path.join(root, modified_file_name),
                        # bucket
                        Bucket=bucket_name,
                        # s3 folder/filename
                        Key=f"{bucket_folder}/{modified_file_name}",
                    )
                    file_count += 1

            else:
                # file doesn't exist in S3
                print(file, "file doesn't exist in S3")
                data["File_Exists_in_S3"] = "False"
                data["Contents_Modified"] = " "
                data["Modified_File_Name"] = " "
                s3_client.upload_file(
                    # local path with the filename
                    Filename=os.path.join(root, file),
                    # bucket
                    Bucket=bucket_name,
                    # s3 folder/filename
                    Key=f"{bucket_folder}/{file}",
                )
                file_count += 1

            df_rows.append(data)

    # print(rows)
    file_names_df = pd.DataFrame(df_rows)
    # print(out_df)
    snowflake_connection.upload_df_to_snowflake(
        df=file_names_df,
        staging_table_name=snowflake_staging_table_name,
        raw_table_name=snowflake_raw_table_name,
        history_table_name=snowflake_history_table_name,
    )
    print("--------------------------------------------------------------------------------")
    print(f"Total number of {api_endpoint} uploaded to S3: {file_count}")
    print("--------------------------------------------------------------------------------")


def _delete_locally(api_endpoint):
    """
    This method is used to delete files that were downloaded locally.
    Args:
        api_enpoint(str): Depending on the api_endpoint, the local path will change.
    """
    import os

    from pipeline_utils.constants import (
        LOCAL_PATH_UNVIEWED_ERAM_FILES,
        LOCAL_PATH_UNVIEWED_FILES,
    )

    if api_endpoint == "unviewed_eram_files":
        input_dir = LOCAL_PATH_UNVIEWED_ERAM_FILES
    else:
        input_dir = LOCAL_PATH_UNVIEWED_FILES
    file_count = 0
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith(".csv"):
                continue
            else:
                if os.path.exists(file_path):
                    file_count += 1
                    os.remove(file_path)

    print("--------------------------------------------------------------------------------")
    print(f"Total number of {api_endpoint} deleted locally: {file_count}")
    print("---------------------------------------------------------------------------------")