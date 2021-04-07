"""Lambda to copy log files from RDS to S3.

The copy is incremental, based on recording the timestamp of the latest log
copied at the end of each run (stored as an integer in
s3://$S3_BUCKET_NAME/$LOG_NAME_PREFIX/$LAST_RECEIVED_FILE).

Loosely based on https://github.com/vcardillo/rdslogs_to_s3
"""

import boto3
import botocore
import io
import os
import posixpath


def get_last_received_timestamp(s3_client, bucket_name, key):
    """Read an integer from a file in S3 or return 0 if not found."""
    print(f"Reading last received timestamp from s3://{bucket_name}/{key}")
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        timestamp = int(obj["Body"].read())
        print(f"Retrieving log files with LastWritten time after {timestamp}")
        return timestamp
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response["ResponseMetadata"]["HTTPStatusCode"])
        if error_code == 404:
            print("No last received timestamp file found. All files will be retrieved from RDS.")
            return 0
        else:
            raise Exception(f"Unable to access s3://{bucket_name}/{key}: {e}")


def lambda_handler(event, context):
    rds_instance_name = os.environ["RDS_INSTANCE_NAME"]
    s3_bucket_name = os.environ["S3_BUCKET_NAME"]
    s3_prefix = os.environ["S3_BUCKET_PREFIX"]
    log_name_prefix = os.environ["LOG_NAME_PREFIX"]
    last_received_file = posixpath.join(s3_prefix, os.environ["LAST_RECEIVED_FILE"])
    region = event["region"]

    s3_client = boto3.client("s3", region_name=region)
    rds_client = boto3.client("rds", region_name=region)
    db_logs = rds_client.describe_db_log_files(
        DBInstanceIdentifier=rds_instance_name, FilenameContains=log_name_prefix
    )
    time_copied_up_to = 0
    time_copied_up_to_this_run = 0
    writes = 0

    time_copied_up_to = get_last_received_timestamp(s3_client, s3_bucket_name, last_received_file)

    for db_log in db_logs["DescribeDBLogFiles"]:
        if int(db_log["LastWritten"]) > time_copied_up_to:
            print(
                f"Downloading DB log file {db_log['LogFileName']} "
                f"(LastWritten={db_log['LastWritten']})"
            )

            if int(db_log["LastWritten"]) > time_copied_up_to_this_run:
                time_copied_up_to_this_run = int(db_log["LastWritten"])

            log_file = rds_client.download_db_log_file_portion(
                DBInstanceIdentifier=rds_instance_name,
                LogFileName=db_log["LogFileName"],
                Marker="0",
            )
            log_file_data = io.BytesIO(log_file["LogFileData"].encode())
            while log_file["AdditionalDataPending"]:
                log_file = rds_client.download_db_log_file_portion(
                    DBInstanceIdentifier=rds_instance_name,
                    LogFileName=db_log["LogFileName"],
                    Marker=log_file["Marker"],
                )
                log_file_data.write(log_file["LogFileData"].encode())

            try:
                obj_name = posixpath.join(s3_prefix, db_log["LogFileName"])
                print(f"Writing s3://{s3_bucket_name}/{obj_name}")
                s3_client.put_object(Bucket=s3_bucket_name, Key=obj_name, Body=log_file_data)
                writes += 1
            except botocore.exceptions.ClientError as e:
                raise Exception(f"Error writing to s3://{s3_bucket_name}/{obj_name}: {e}")

    print("------------ Writing of files to S3 complete:")
    if writes:
        print(f"Successfully wrote {writes} log files.")
        try:
            s3_client.put_object(
                Bucket=s3_bucket_name,
                Key=last_received_file,
                Body=str(time_copied_up_to_this_run).encode(),
            )
            print(
                "Successfully wrote new last-written marker to "
                f"s3://{s3_bucket_name}/{last_received_file}"
            )
        except botocore.exceptions.ClientError as e:
            raise Exception(f"Error writing marker to S3 bucket, S3 ClientError: {e}")
    else:
        print("No new log files were written.")

    return "Log file export complete."
